#!/usr/bin/env python3

import hashlib
import json
import logging
import os
import threading
import time
import traceback
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

from pycrdt import Map

from models import (
    OperationType,
    SyncOperation,
    SyncRequest,
    SyncResult,
    SyncType,
    create_document_resource_from_metadata,
)
from persistence import PersistenceManager
from relay_client import RelayClient
from s3rn import S3RN, S3RemoteCanvas, S3RemoteDocument, S3RemoteFile, S3RemoteFolder, S3RNType

logger = logging.getLogger(__name__)

# Burst thresholds mirror the Relay plugin's DeleteCollector
# (relay/src/folder-hsm/delete-collector.ts): destructive bursts above
# max(10% of membership, 25) are gated instead of applied.
MASS_CHANGE_THRESHOLD_FRACTION = 0.1
MASS_CHANGE_THRESHOLD_FLOOR = 25
TRUNCATION_EVENT_WINDOW_SECONDS = 60.0

# What fetch_canvas_content returns for a canvas whose CRDT was emptied.
EMPTY_CANVAS_CONTENT = json.dumps({"edges": [], "nodes": []}, indent=2, sort_keys=True)


def mass_change_threshold(membership_size: int) -> int:
    return max(int(MASS_CHANGE_THRESHOLD_FRACTION * membership_size), MASS_CHANGE_THRESHOLD_FLOOR)


class SyncEngine:
    """Core synchronization logic for Relay documents to Git repositories"""

    def __init__(
        self,
        data_dir: str,
        relay_client: RelayClient,
        persistence_manager: Optional[PersistenceManager] = None,
    ):
        self.data_dir = data_dir
        self.relay_client = relay_client
        self.persistence_manager = persistence_manager or PersistenceManager(data_dir)
        self.folder_sync_locks: Dict[str, threading.Lock] = {}
        # Per-folder truncation budgets for the current sync pass.
        self._truncation_gates: Dict[Tuple[str, str], Dict[str, int]] = {}
        # Websocket document events arrive outside folder passes. Track their
        # destructive writes in a rolling window so a burst cannot bypass the
        # per-pass gate one document at a time.
        self._recent_truncations: Dict[Tuple[str, str], Deque[float]] = {}
        self._truncation_gate_lock = threading.Lock()

    def process_document_change(
        self,
        relay_id: str,
        resource_id: str,
        timestamp: datetime,
        subdoc_snapshot: Optional[bytes] = None,
        baseline_only: bool = False,
        user: Optional[str] = None,
        deleted_from: Optional[List[Tuple[str, int]]] = None,
    ) -> SyncResult:
        """Process a document change notification with individual UUIDs.

        `user` is the Relay user whose write produced this notification, and
        `deleted_from` ranks the users whose content that write removed. They are
        the only attribution available for a change that removes content: the
        removed text is gone from the doc, so no later read of the doc can
        recover who took it out or whose it was.
        """
        try:
            print(f"Processing document change for relay {relay_id}, resource {resource_id}")

            self.persistence_manager.note_doc_writer(resource_id, user, deleted_from)

            # Ensure relay data is loaded
            self.persistence_manager.load_persistent_data(relay_id)

            # Check if this is a known folder
            if resource_id in self.persistence_manager.filemeta_folders.get(relay_id, {}):
                if not self.persistence_manager.should_sync_folder(relay_id, resource_id):
                    print(
                        f"Skipping document change for unconfigured folder {relay_id}/{resource_id}"
                    )
                    return SyncResult(resource=None, operations=[], success=True)

                # This is a folder - fetch filemeta and process
                folder_resource = S3RemoteFolder(relay_id, resource_id)
                doc = self.relay_client.get_doc_object(folder_resource)

                if "filemeta_v0" not in doc.keys():
                    return SyncResult(
                        resource=None,
                        operations=[],
                        success=False,
                        error=f"Resource {resource_id} expected to be folder but missing filemeta_v0",
                    )

                # Extract filemeta from Y.Doc
                filemeta_content = doc.get("filemeta_v0", type=Map)
                filemeta_dict = self.relay_client._map_to_dict(filemeta_content)

                print(f"Resource {resource_id} is a folder with filemeta_v0")

                # Initialize git repo for this folder
                self.persistence_manager.init_git_repo(relay_id, resource_id)

                old_filemeta = self.persistence_manager.filemeta_folders[relay_id].get(
                    resource_id, {}
                )

                # Apply sync algorithm for folder changes
                folder_operations, filemeta_accepted = self._apply_remote_folder_changes(
                    relay_id,
                    folder_resource,
                    old_filemeta,
                    filemeta_dict,
                    incremental=True,
                )
                operations = folder_operations

                if filemeta_accepted:
                    self.persistence_manager.filemeta_folders[relay_id][resource_id] = filemeta_dict
                    print(f"Updated filemeta_v0 for folder {resource_id}")
                else:
                    logger.warning(
                        "Preserving prior filemeta for folder %s because the remote "
                        "snapshot was refused",
                        resource_id,
                    )

                # Rebuild after operations so local state and accepted filemeta agree.
                self.persistence_manager._build_resource_index(relay_id)

            else:
                # This should be a document/canvas/file - lookup type from resource index
                document_resource = self.persistence_manager.lookup_resource(relay_id, resource_id)

                if document_resource is None:
                    # Debug info for unknown resource
                    print(f"DEBUG: Document {resource_id} not found in resource index")
                    print(
                        f"DEBUG: Resource index for relay {relay_id}: {list(self.persistence_manager.resource_index.get(relay_id, {}).keys())}"
                    )
                    print(f"DEBUG: Looking for resource in filemeta...")
                    for folder_id, filemeta in self.persistence_manager.filemeta_folders.get(
                        relay_id, {}
                    ).items():
                        for path, metadata in filemeta.items():
                            if isinstance(metadata, dict) and metadata.get("id") == resource_id:
                                print(
                                    f"DEBUG: Found {resource_id} in folder {folder_id} at path {path} with metadata: {metadata}"
                                )

                    print(
                        f"Warning: Document {resource_id} not found in resource index - skipping update"
                    )
                    operations = []
                else:
                    folder_id = getattr(document_resource, "folder_id", None)
                    if folder_id and not self.persistence_manager.should_sync_folder(
                        relay_id, folder_id
                    ):
                        print(
                            f"Skipping document change for unconfigured folder {relay_id}/{folder_id}"
                        )
                        return SyncResult(resource=None, operations=[], success=True)

                    subdoc_head = (
                        hashlib.sha256(subdoc_snapshot).hexdigest()
                        if subdoc_snapshot is not None
                        else None
                    )
                    if subdoc_head is not None:
                        old_head = self.persistence_manager.subdoc_heads[relay_id].get(resource_id)
                        if old_head == subdoc_head:
                            return SyncResult(resource=None, operations=[], success=True)
                        if baseline_only:
                            self.persistence_manager.subdoc_heads[relay_id][
                                resource_id
                            ] = subdoc_head
                            self.persistence_manager.save_persistent_data(relay_id)
                            return SyncResult(resource=None, operations=[], success=True)

                    # Fetch content based on resource type
                    if isinstance(document_resource, S3RemoteDocument):
                        content_str = self.relay_client.fetch_document_content(document_resource)
                        doc_type = "document"
                    elif isinstance(document_resource, S3RemoteCanvas):
                        content_str = self.relay_client.fetch_canvas_content(document_resource)
                        doc_type = "canvas"
                    elif isinstance(document_resource, S3RemoteFile):
                        # Files don't have text content to hash, they're handled differently
                        print(f"File resource {resource_id} updated - file sync handled separately")
                        operations = []
                        content_str = None
                    else:
                        print(f"Unknown resource type for {resource_id}: {type(document_resource)}")
                        operations = []
                        content_str = None

                    if content_str is not None:
                        print(f"Resource {resource_id} {doc_type} content updated")

                        # Calculate hash
                        doc_hash = hashlib.sha256(content_str.encode("utf-8")).hexdigest()
                        print(f"Resource {resource_id} hash: {doc_hash}")

                        old_hash = self.persistence_manager.document_hashes[relay_id].get(
                            resource_id
                        )

                        # If this is a document update, trigger sync
                        if old_hash != doc_hash:
                            operation = self.handle_document_update(
                                document_resource, content_str, doc_hash
                            )
                            operations = [operation] if operation else []
                            if operation and not operation.error:
                                self.persistence_manager.document_hashes[relay_id][
                                    resource_id
                                ] = doc_hash
                                if subdoc_head is not None:
                                    self.persistence_manager.subdoc_heads[relay_id][
                                        resource_id
                                    ] = subdoc_head
                        else:
                            operations = []
                            if subdoc_head is not None:
                                self.persistence_manager.subdoc_heads[relay_id][
                                    resource_id
                                ] = subdoc_head
                    else:
                        operations = []

            # Save persistent data
            self.persistence_manager.save_persistent_data(relay_id)

            return SyncResult(
                resource=None,  # No specific resource since we're working with raw IDs
                operations=operations,
                success=True,
            )

        except Exception as e:
            logger.error(
                f"Error processing document change for relay {relay_id}, resource {resource_id}: {e}"
            )
            logger.error(f"Document change processing traceback: {traceback.format_exc()}")
            return SyncResult(resource=None, operations=[], success=False, error=str(e))

    def process_sync_request(self, request: SyncRequest) -> SyncResult:
        """Process a sync request using S3RN resource"""
        try:
            resource = request.resource
            relay_id = S3RN.get_relay_id(resource)

            print(f"Processing resource {type(resource).__name__} for relay {relay_id}")

            # Ensure relay data is loaded
            self.persistence_manager.load_persistent_data(relay_id)

            folder_id = getattr(resource, "folder_id", None)
            if folder_id and not self.persistence_manager.should_sync_folder(relay_id, folder_id):
                print(f"Skipping sync request for unconfigured folder {relay_id}/{folder_id}")
                return SyncResult(resource=resource, operations=[], success=True)

            # Get document structure
            doc, parsed_content = self.relay_client.get_document_structure(resource)
            print(f"Document {resource} keys: {doc.keys()}")

            operations = []

            # Handle folder document (has "filemeta_v0" key)
            if parsed_content.get("type") == "folder":
                filemeta_dict = parsed_content["filemeta"]
                folder_uuid = S3RN.get_folder_id(resource)
                print(f"Document {resource} is a folder with filemeta_v0")

                # Initialize git repo for this folder
                self.persistence_manager.init_git_repo(relay_id, folder_uuid)

                old_filemeta = self.persistence_manager.filemeta_folders[relay_id].get(
                    folder_uuid, {}
                )

                # Apply sync algorithm for folder changes
                folder_operations, filemeta_accepted = self._apply_remote_folder_changes(
                    relay_id, resource, old_filemeta, filemeta_dict
                )
                operations.extend(folder_operations)

                if filemeta_accepted:
                    self.persistence_manager.filemeta_folders[relay_id][folder_uuid] = filemeta_dict
                    print(f"Updated filemeta_v0 for folder {resource}")
                else:
                    logger.warning(
                        "Preserving prior filemeta for folder %s because the remote "
                        "snapshot was refused",
                        folder_uuid,
                    )

                # Rebuild after operations so local state and accepted filemeta agree.
                self.persistence_manager._build_resource_index(relay_id)

            # Handle text document
            elif parsed_content.get("type") == "document":
                content_str = parsed_content["content"]
                doc_uuid = getattr(resource, "document_id", None)
                print(f"Document {resource} text content updated")

                # Calculate hash for text documents
                doc_hash = hashlib.sha256(content_str.encode("utf-8")).hexdigest()
                print(f"Document {resource} hash: {doc_hash}")

                old_hash = self.persistence_manager.document_hashes[relay_id].get(doc_uuid)

                # If this is a document update, trigger sync
                if old_hash != doc_hash:
                    operation = self.handle_document_update(resource, content_str, doc_hash)
                    if operation:
                        operations.append(operation)
                        if not operation.error:
                            self.persistence_manager.document_hashes[relay_id][doc_uuid] = doc_hash

            # Handle canvas document
            elif parsed_content.get("type") == "canvas":
                content_str = parsed_content["content"]
                canvas_uuid = getattr(resource, "canvas_id", None)
                print(f"Canvas {resource} content updated")

                # Calculate hash for canvas documents
                doc_hash = hashlib.sha256(content_str.encode("utf-8")).hexdigest()
                print(f"Canvas {resource} hash: {doc_hash}")

                old_hash = self.persistence_manager.document_hashes[relay_id].get(canvas_uuid)

                # If this is a canvas update, trigger sync
                if old_hash != doc_hash:
                    operation = self.handle_document_update(resource, content_str, doc_hash)
                    if operation:
                        operations.append(operation)
                        if not operation.error:
                            self.persistence_manager.document_hashes[relay_id][
                                canvas_uuid
                            ] = doc_hash
            else:
                print(f"Document {resource} has no recognized content type")

            # Save persistent data
            self.persistence_manager.save_persistent_data(relay_id)

            return SyncResult(resource=resource, operations=operations, success=True)

        except Exception as e:
            logger.error(f"Error processing sync request for {request.resource}: {e}")
            logger.error(f"Sync request processing traceback: {traceback.format_exc()}")
            return SyncResult(resource=request.resource, operations=[], success=False, error=str(e))

    def sync_relay_all_folders(self, relay_id: str) -> List[SyncResult]:
        """CLI/API-triggered sync for all folders in a relay"""
        try:
            # Load relay data
            self.persistence_manager.load_persistent_data(relay_id)
            # Note: Git repo initialization is now done per-folder when needed

            results = []
            configured_connectors = self.persistence_manager.git_config.get_connectors_for_relay(
                relay_id
            )

            if not configured_connectors:
                print(f"No git connectors configured for relay {relay_id}")
                return []

            for connector in configured_connectors:
                folder_uuid = connector.shared_folder_id
                folder_resource = S3RemoteFolder(relay_id, folder_uuid)

                # Create sync request that will fetch fresh data from server
                request = SyncRequest(
                    resource=folder_resource, timestamp=datetime.now(timezone.utc)
                )
                result = self.process_sync_request(request)
                results.append(result)

            return results

        except Exception as e:
            logger.error(f"Error syncing all folders for relay {relay_id}: {e}")
            return [SyncResult(resource=None, operations=[], success=False, error=str(e))]

    def sync_specific_folder(self, folder_resource: S3RemoteFolder) -> SyncResult:
        """CLI/API-triggered sync for specific folder"""
        request = SyncRequest(resource=folder_resource, timestamp=datetime.now(timezone.utc))
        return self.process_sync_request(request)

    def handle_document_update(
        self, document_resource: S3RNType, content: str, doc_hash: str
    ) -> Optional[SyncOperation]:
        """Handle updates to document content"""
        relay_id = S3RN.get_relay_id(document_resource)
        # Extract resource ID using instance method
        doc_uuid = document_resource.get_resource_id()

        # Find the document using resource index
        found_document_resource = self.persistence_manager.lookup_resource(relay_id, doc_uuid)
        file_path = self.persistence_manager.get_resource_path(relay_id, doc_uuid)

        if (
            isinstance(found_document_resource, (S3RemoteDocument, S3RemoteCanvas, S3RemoteFile))
            and file_path
        ):
            # Create operation for update
            folder_resource = S3RemoteFolder(relay_id, found_document_resource.folder_id)
            operation = SyncOperation(
                type=OperationType.UPDATE,
                path=file_path,
                folder_resource=folder_resource,
                document_resource=found_document_resource,
                content=content,
                metadata={"hash": doc_hash},
            )
            self.execute_sync_operation(relay_id, operation)
            return operation
        else:
            logger.warning(
                f"Cannot find path for document {doc_uuid} in relay {relay_id} filemeta - document may not be in a synced folder yet"
            )
            return None

    def apply_remote_folder_changes(
        self, relay_id: str, folder_resource: S3RemoteFolder, old_filemeta: Dict, new_filemeta: Dict
    ) -> List[SyncOperation]:
        operations, _filemeta_accepted = self._apply_remote_folder_changes(
            relay_id, folder_resource, old_filemeta, new_filemeta
        )
        return operations

    def _apply_remote_folder_changes(
        self,
        relay_id: str,
        folder_resource: S3RemoteFolder,
        old_filemeta: Dict,
        new_filemeta: Dict,
        *,
        incremental: bool = False,
    ) -> Tuple[List[SyncOperation], bool]:
        """
        Algorithm for applying remote folder changes to the local vault.
        Returns the operations and whether new_filemeta is safe to persist.
        """
        folder_uuid = S3RN.get_folder_id(folder_resource)
        folder_key = (relay_id, folder_uuid)
        print(f"Applying remote folder changes for folder {folder_uuid} in relay {relay_id}")

        # Get or create per-folder sync lock
        if folder_uuid not in self.folder_sync_locks:
            self.folder_sync_locks[folder_uuid] = threading.Lock()

        folder_lock = self.folder_sync_locks[folder_uuid]

        # Prevent concurrent syncs for this folder
        with folder_lock:
            try:
                membership_size = self._file_entry_count(new_filemeta)
                local_file_count = self._local_file_count(relay_id, folder_uuid)
                sync_filemeta = (
                    self._changed_filemeta_entries(old_filemeta, new_filemeta)
                    if incremental
                    else new_filemeta
                )
                if incremental:
                    logger.info(
                        "Folder %s metadata changed for %d of %d entries",
                        folder_uuid,
                        len(sync_filemeta),
                        len(new_filemeta),
                    )

                # Publication rule (relay/src/folder-hsm/bridge.ts): a wholly
                # empty remote map against non-empty local membership is a
                # publication, re-share, or server reset — never a mass
                # deletion. The plugin uploads local content; a read-only
                # mirror can only refuse to act.
                if membership_size == 0 and local_file_count > 0:
                    logger.error(
                        f"Refusing to sync folder {folder_uuid}: remote filemeta has no file "
                        f"entries while {local_file_count} local files exist. This is a "
                        f"publication/reset state, not a mass deletion."
                    )
                    return [], False

                self._truncation_gates[folder_key] = {
                    "applied": 0,
                    "limit": mass_change_threshold(membership_size),
                }

                operations = []
                diff_log = []

                # Phase 1: Process folder operations first (renames/moves affect files)
                print(f"Phase 1: Processing folder operations for {folder_uuid}")
                self.sync_by_type(
                    relay_id,
                    folder_resource,
                    sync_filemeta,
                    diff_log,
                    operations,
                    [SyncType.FOLDER],
                )

                # Wait for folder operations to complete
                self.await_all_operations(relay_id, operations)

                # Phase 2: Process file operations (docs, canvas, sync files)
                print(f"Phase 2: Processing file operations for {folder_uuid}")
                file_sync_types = [
                    SyncType.DOCUMENT,
                    SyncType.CANVAS,
                    SyncType.IMAGE,
                    SyncType.PDF,
                    SyncType.AUDIO,
                    SyncType.VIDEO,
                    SyncType.FILE,
                ]
                self.sync_by_type(
                    relay_id, folder_resource, sync_filemeta, diff_log, operations, file_sync_types
                )

                # Phase 3: Handle deletions after creates/renames complete
                print(f"Phase 3: Processing deletions for {folder_uuid}")
                creates = [op for op in operations if op.type == OperationType.CREATE]
                renames = [op for op in operations if op.type == OperationType.RENAME]
                remote_paths = [op.path for op in operations]

                # Ensure creates and renames complete before checking for deletions
                self.await_operations(relay_id, [*creates, *renames])

                # Phase 4: Clean up local files that no longer exist remotely
                deletes = self.cleanup_extra_local_files(
                    relay_id, folder_resource, new_filemeta, remote_paths, diff_log
                )

                # The mirror cannot stop a deletion from reaching Relay, and
                # git history keeps every deleted file, so a burst is applied
                # and reported rather than held.
                delete_threshold = mass_change_threshold(membership_size)
                if len(deletes) > delete_threshold:
                    snapshot = self.persistence_manager.snapshot_before_deletion(
                        relay_id,
                        folder_uuid,
                        f"Snapshot before {len(deletes)} deletions in folder {folder_uuid}",
                    )
                    restore_point = (
                        f"commit {snapshot[0][:10]} (ref {snapshot[1]}"
                        + (
                            ""
                            if snapshot[2]
                            else ", NOT yet pushed: retried every minute, lost if the pod "
                            "restarts first"
                        )
                        + f"); restore with `git fetch origin {snapshot[1]} && "
                        f"git checkout FETCH_HEAD -- <paths>`"
                        if snapshot
                        else "no snapshot (folder has no repository)"
                    )
                    logger.error(
                        f"Mass deletion applied for folder {folder_uuid}: {len(deletes)} "
                        f"deletions exceed threshold {delete_threshold} for membership of "
                        f"{membership_size}. Every deleted file is in {restore_point}."
                    )
                operations.extend(deletes)

                # Execute all operations
                for operation in operations:
                    if not operation.completed:
                        self.execute_sync_operation(relay_id, operation)

                # Print diff log
                if diff_log:
                    print(f"Sync changes for folder {folder_uuid}:")
                    for log_entry in diff_log:
                        print(f"  {log_entry}")

                return operations, True

            except Exception as e:
                logger.error(f"Error syncing folder {folder_uuid}: {e}")
                logger.error(f"Folder sync traceback: {traceback.format_exc()}")
                return [], False
            finally:
                self._truncation_gates.pop(folder_key, None)

    def _changed_filemeta_entries(self, old_filemeta: Dict, new_filemeta: Dict) -> Dict:
        """Return new or modified entries for an event-driven folder sync."""
        return {
            path: metadata
            for path, metadata in new_filemeta.items()
            if old_filemeta.get(path) != metadata
        }

    def _file_entry_count(self, filemeta: Dict) -> int:
        return sum(
            1
            for metadata in filemeta.values()
            if isinstance(metadata, dict) and "id" in metadata and metadata.get("type") != "folder"
        )

    def _local_file_count(self, relay_id: str, folder_uuid: str) -> int:
        folder_path = self.persistence_manager.get_folder_path_with_prefix(relay_id, folder_uuid)
        if not os.path.exists(folder_path):
            return 0
        count = 0
        for root, dirs, files in os.walk(folder_path):
            if ".git" in dirs:
                dirs.remove(".git")
            count += sum(1 for f in files if not f.startswith(".git"))
        return count

    def _gates_truncation(self, relay_id: str, operation: SyncOperation) -> bool:
        """Budget non-empty -> empty overwrites during a folder sync pass.

        A single truncation is authoritative (the doc's CRDT has history and
        was genuinely emptied), but a burst of them is upstream poison — e.g.
        every doc emptied by a botched re-share — that a mirror must not
        engrave into files. Returns True when the operation must be skipped.
        """
        folder_uuid = S3RN.get_folder_id(operation.folder_resource)
        folder_key = (relay_id, folder_uuid)
        folder_path = self.persistence_manager.get_folder_path_with_prefix(relay_id, folder_uuid)
        full_path = self.persistence_manager._sanitize_path(operation.path, folder_path)
        try:
            if not os.path.exists(full_path) or os.path.getsize(full_path) == 0:
                return False
        except OSError:
            return False

        gate = self._truncation_gates.get(folder_key)
        if gate is not None:
            if gate["applied"] >= gate["limit"]:
                operation.error = "Truncation burst gated: refusing to empty another non-empty file"
                logger.error(
                    f"Gating truncation burst for folder {folder_uuid}: more than "
                    f"{gate['limit']} non-empty files would be emptied in one sync pass. "
                    f"Skipping {operation.path}."
                )
                return True

            gate["applied"] += 1
            return False

        membership = self.persistence_manager.filemeta_folders.get(relay_id, {}).get(
            folder_uuid, {}
        )
        limit = mass_change_threshold(self._file_entry_count(membership))
        now = time.monotonic()
        cutoff = now - TRUNCATION_EVENT_WINDOW_SECONDS

        with self._truncation_gate_lock:
            recent = self._recent_truncations.setdefault(folder_key, deque())
            while recent and recent[0] < cutoff:
                recent.popleft()

            if len(recent) >= limit:
                operation.error = "Truncation burst gated: refusing to empty another non-empty file"
                logger.error(
                    "Gating truncation burst for folder %s: %d non-empty files "
                    "were already emptied in the last %.0f seconds. Skipping %s.",
                    folder_uuid,
                    limit,
                    TRUNCATION_EVENT_WINDOW_SECONDS,
                    operation.path,
                )
                return True

            recent.append(now)
            return False

    def sync_by_type(
        self,
        relay_id: str,
        folder_resource: S3RemoteFolder,
        filemeta: Dict,
        diff_log: List[str],
        operations: List[SyncOperation],
        sync_types: List[SyncType],
    ):
        """Process remote changes for specific file types."""
        for path, metadata in filemeta.items():
            if isinstance(metadata, dict) and "id" in metadata:
                file_type = self.get_file_type(path, metadata)
                if file_type in sync_types:
                    operation = self.apply_remote_state(
                        relay_id, folder_resource, path, metadata, diff_log
                    )
                    if operation:
                        operations.append(operation)

    def apply_remote_state(
        self,
        relay_id: str,
        folder_resource: S3RemoteFolder,
        path: str,
        metadata: Dict,
        diff_log: List[str],
    ) -> Optional[SyncOperation]:
        """
        Core algorithm for determining what operation to perform for a remote file change.
        Returns an operation object.
        """
        doc_id = metadata.get("id")
        if not doc_id:
            return None

        # Check if this is a folder type - folders are handled differently
        resource_type = metadata.get("type", "unknown")
        if resource_type == "folder":
            # Folders are directory operations, not document operations
            diff_log.append(f"ensuring directory exists for {path.lstrip('/')}")
            return SyncOperation(
                type=OperationType.CREATE,  # Create directory
                path=path,
                folder_resource=folder_resource,
                document_resource=None,  # No document resource for folders
                metadata=metadata,
            )

        # Build path within folder subdirectory, including any configured prefix
        folder_uuid = S3RN.get_folder_id(folder_resource)
        folder_path = self.persistence_manager.get_folder_path_with_prefix(relay_id, folder_uuid)
        full_path = self.persistence_manager._sanitize_path(path, folder_path)

        # Case 1: File exists locally
        if os.path.exists(full_path):
            # Check if file needs updating (hash comparison)
            if self.should_update_file(relay_id, doc_id, metadata, full_path):
                diff_log.append(f"updating {folder_uuid}/{path.lstrip('/')}")
                # Create S3RN resources for the operation
                document_resource = create_document_resource_from_metadata(
                    relay_id, folder_uuid, metadata
                )
                return SyncOperation(
                    type=OperationType.UPDATE,
                    path=path,
                    folder_resource=folder_resource,
                    document_resource=document_resource,
                    metadata=metadata,
                )
            else:
                document_resource = create_document_resource_from_metadata(
                    relay_id, folder_uuid, metadata
                )
                return SyncOperation(
                    type=OperationType.NOOP,
                    path=path,
                    folder_resource=folder_resource,
                    document_resource=document_resource,
                    completed=True,
                )

        # Case 2: File was renamed/moved (exists locally with same doc_id)
        old_path = self.persistence_manager.find_local_file_by_doc_id(relay_id, folder_uuid, doc_id)
        if old_path and old_path != path:
            diff_log.append(
                f"{folder_uuid}/{old_path.lstrip('/')} was renamed to {folder_uuid}/{path.lstrip('/')}"
            )
            document_resource = create_document_resource_from_metadata(
                relay_id, folder_uuid, metadata
            )
            return SyncOperation(
                type=OperationType.RENAME,
                path=path,
                folder_resource=folder_resource,
                document_resource=document_resource,
                from_path=old_path,
                to_path=path,
                metadata=metadata,
            )

        # Case 3: New file created remotely
        diff_log.append(f"created local file for remotely added {folder_uuid}/{path.lstrip('/')}")
        document_resource = create_document_resource_from_metadata(relay_id, folder_uuid, metadata)
        return SyncOperation(
            type=OperationType.CREATE,
            path=path,
            folder_resource=folder_resource,
            document_resource=document_resource,
            metadata=metadata,
        )

    def should_update_file(
        self, relay_id: str, doc_id: str, metadata: Dict, full_path: str
    ) -> bool:
        """Check if a file should be updated based on hash comparison"""
        remote_hash = metadata.get("hash")
        if not remote_hash:
            return True  # No hash available, assume update needed

        # Get local file hash
        try:
            with open(full_path, "rb") as f:
                local_content = f.read()
            local_hash = hashlib.sha256(local_content).hexdigest()
            return local_hash != remote_hash
        except Exception as e:
            logger.warning(f"Error reading file {full_path} for hash comparison: {e}")
            return True  # Error reading file, assume update needed

    def execute_sync_operation(self, relay_id: str, operation: SyncOperation):
        """Execute a sync operation"""
        try:
            if operation.type == OperationType.CREATE:
                self.handle_server_create(relay_id, operation)
            elif operation.type == OperationType.UPDATE:
                self.handle_server_update(relay_id, operation)
            elif operation.type == OperationType.RENAME:
                self.handle_server_rename(relay_id, operation)
            elif operation.type == OperationType.DELETE:
                self.handle_server_delete(relay_id, operation)

            operation.completed = True
            folder_uuid = (
                S3RN.get_folder_id(operation.folder_resource)
                if operation.folder_resource
                else "unknown"
            )
            print(f"Completed operation: {operation.type.value} {folder_uuid}/{operation.path}")

        except Exception as e:
            operation.error = str(e)
            folder_uuid = (
                S3RN.get_folder_id(operation.folder_resource)
                if operation.folder_resource
                else "unknown"
            )
            logger.error(
                f"Error executing operation {operation.type.value} {folder_uuid}/{operation.path}: {e}"
            )
            logger.error(f"Operation execution traceback: {traceback.format_exc()}")

    def handle_server_create(self, relay_id: str, operation: SyncOperation):
        """Handle creating a new file from remote changes."""
        document_resource = operation.document_resource
        path = operation.path
        folder_resource = operation.folder_resource

        # Check if this is a folder - folders don't have document content
        if operation.metadata and operation.metadata.get("type") == "folder":
            # Create directory structure via persistence manager
            full_path = self.persistence_manager.create_directory(folder_resource, path)
            print(f"Created directory {full_path}")
            return

        # Fetch content based on resource type
        if isinstance(document_resource, S3RemoteFile):
            # S3RemoteFile requires special handling with file hash
            file_hash = operation.metadata.get("hash") if operation.metadata else None
            mimetype = (
                operation.metadata.get("mimetype", "application/octet-stream")
                if operation.metadata
                else "application/octet-stream"
            )

            if not file_hash:
                operation.error = "S3 file missing required hash metadata"
                logger.warning(f"Skipping create operation for {path}: S3 file missing hash")
                return

            binary_content = self.relay_client.fetch_s3_file_content(
                document_resource, file_hash, mimetype
            )
            if binary_content is None:
                operation.error = "Failed to fetch S3 file content (possibly deleted)"
                logger.warning(f"Skipping create operation for {path}: S3 file not found")
                return

            # Write binary file using persistence manager
            full_path = self.persistence_manager.write_binary_file_content(
                document_resource, path, binary_content, file_hash
            )
        elif isinstance(document_resource, S3RemoteCanvas):
            # Canvas content as JSON
            content = self.relay_client.fetch_canvas_content(document_resource)

            # If content fetch failed (e.g., 404), skip this operation
            if content is None:
                operation.error = "Failed to fetch canvas content (possibly deleted)"
                logger.warning(f"Skipping create operation for {path}: canvas not found")
                return

            # Write canvas JSON using persistence manager
            file_hash = operation.metadata.get("hash") if operation.metadata else None
            full_path = self.persistence_manager.write_file_content(
                document_resource, path, content, file_hash
            )
        else:
            # Regular document/text content
            content = self.relay_client.fetch_document_content(document_resource)

            # If content fetch failed (e.g., 404), skip this operation
            if content is None:
                operation.error = "Failed to fetch document content (possibly deleted)"
                logger.warning(f"Skipping create operation for {path}: document not found")
                return

            # Write file using persistence manager
            file_hash = operation.metadata.get("hash") if operation.metadata else None
            full_path = self.persistence_manager.write_file_content(
                document_resource, path, content, file_hash
            )

        print(f"Created {full_path}")

    def handle_server_update(self, relay_id: str, operation: SyncOperation):
        """Handle updating an existing file from remote changes."""
        document_resource = operation.document_resource
        path = operation.path
        folder_resource = operation.folder_resource

        # Check if this is a folder - folders don't have document content
        if operation.metadata and operation.metadata.get("type") == "folder":
            # Ensure directory exists via persistence manager
            full_path = self.persistence_manager.create_directory(folder_resource, path)
            print(f"Updated directory {full_path}")
            return

        # Fetch content based on resource type
        if isinstance(document_resource, S3RemoteFile):
            # S3RemoteFile requires special handling with file hash
            file_hash = operation.metadata.get("hash") if operation.metadata else None
            mimetype = (
                operation.metadata.get("mimetype", "application/octet-stream")
                if operation.metadata
                else "application/octet-stream"
            )

            if not file_hash:
                operation.error = "S3 file missing required hash metadata"
                logger.warning(f"Skipping update operation for {path}: S3 file missing hash")
                return

            binary_content = self.relay_client.fetch_s3_file_content(
                document_resource, file_hash, mimetype
            )
            if binary_content is None:
                operation.error = "Failed to fetch S3 file content (possibly deleted)"
                logger.warning(f"Skipping update operation for {path}: S3 file not found")
                return

            # Write binary file using persistence manager
            full_path = self.persistence_manager.write_binary_file_content(
                document_resource, path, binary_content, file_hash
            )

            print(f"Updated {full_path}")
        elif isinstance(document_resource, S3RemoteCanvas):
            # Canvas content as JSON
            content = operation.content
            if content is None:
                content = self.relay_client.fetch_canvas_content(document_resource)

            if content == EMPTY_CANVAS_CONTENT and self._gates_truncation(relay_id, operation):
                return

            if content is not None:
                # Write canvas JSON using persistence manager
                file_hash = operation.metadata.get("hash") if operation.metadata else None
                full_path = self.persistence_manager.write_file_content(
                    document_resource, path, content, file_hash
                )

                print(f"Updated {full_path}")
            else:
                operation.error = "Failed to fetch canvas content (possibly deleted)"
                logger.warning(f"Skipping update operation for {path}: canvas not found")
        else:
            # Use provided content or fetch from remote for documents
            content = operation.content
            if content is None:
                content = self.relay_client.fetch_document_content(document_resource)

            if content == "" and self._gates_truncation(relay_id, operation):
                return

            if content is not None:
                # Write file using persistence manager
                file_hash = operation.metadata.get("hash") if operation.metadata else None
                full_path = self.persistence_manager.write_file_content(
                    document_resource, path, content, file_hash
                )

                print(f"Updated {full_path}")
            else:
                operation.error = "Failed to fetch document content (possibly deleted)"
                logger.warning(f"Skipping update operation for {path}: document not found")

    def handle_server_rename(self, relay_id: str, operation: SyncOperation):
        """Handle renaming/moving a file from remote changes."""
        from_path = operation.from_path
        to_path = operation.to_path
        document_resource = operation.document_resource

        # Move file using persistence manager - extract folder from document resource
        old_full_path, new_full_path = self.persistence_manager.move_file(
            document_resource, from_path, to_path
        )

        print(f"Renamed {old_full_path} to {new_full_path}")

    def handle_server_delete(self, relay_id: str, operation: SyncOperation):
        """Handle deleting a file from remote changes."""
        path = operation.path
        folder_resource = operation.folder_resource

        # Delete file using persistence manager
        full_path = self.persistence_manager.delete_file(folder_resource, path)

        print(f"Deleted {full_path}")

    def cleanup_extra_local_files(
        self,
        relay_id: str,
        folder_resource: S3RemoteFolder,
        current_filemeta: Dict,
        remote_paths: List[str],
        diff_log: List[str],
    ) -> List[SyncOperation]:
        """Delete local files that no longer exist remotely."""
        deletes = []

        # Get folder directory with prefix applied
        folder_uuid = S3RN.get_folder_id(folder_resource)
        folder_path = self.persistence_manager.get_folder_path_with_prefix(relay_id, folder_uuid)
        if not os.path.exists(folder_path):
            return deletes

        # Get all local files in this folder
        for root, dirs, files in os.walk(folder_path):
            # Skip .git directory to avoid deleting Git metadata
            if ".git" in dirs:
                dirs.remove(".git")

            for file in files:
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, folder_path)
                # Normalize path for comparison with filemeta (add leading slash)
                normalized_path = "/" + relative_path.replace("\\", "/")

                # Skip Git-related files
                if relative_path.startswith(".git"):
                    continue

                # Check if this file exists in remote filemeta (try both with and without leading slash)
                if (
                    relative_path not in current_filemeta
                    and normalized_path not in current_filemeta
                ):
                    diff_log.append(
                        f"deleted local file {folder_uuid}/{relative_path} for remotely deleted doc"
                    )
                    deletes.append(
                        SyncOperation(
                            type=OperationType.DELETE,
                            path=relative_path,
                            folder_resource=folder_resource,
                            document_resource=None,
                        )
                    )

        return deletes

    def get_file_type(self, path: str, metadata: Dict) -> SyncType:
        """Determine file type based on metadata type field and file extension"""
        # Use metadata type field primarily
        metadata_type = metadata.get("type", "file")  # Default to file if no type specified

        if metadata_type == "folder":
            return SyncType.FOLDER
        elif metadata_type in ("document", "markdown"):
            # Real Relay filemeta uses "markdown"; "document" is kept for
            # older entries and internal fixtures.
            return SyncType.DOCUMENT
        elif metadata_type == "canvas":
            return SyncType.CANVAS
        elif metadata_type == "file":
            # For generic files, determine specific type by extension
            path_lower = path.lower()
            if path_lower.endswith((".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".webp")):
                return SyncType.IMAGE
            elif path_lower.endswith(".pdf"):
                return SyncType.PDF
            elif path_lower.endswith((".mp3", ".wav", ".flac", ".aac", ".ogg", ".m4a")):
                return SyncType.AUDIO
            elif path_lower.endswith((".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm", ".mkv")):
                return SyncType.VIDEO
            else:
                return SyncType.FILE
        else:
            # Handle any other metadata types
            return SyncType.FILE

    def await_all_operations(self, relay_id: str, operations: List[SyncOperation]):
        """Wait for all operations to complete"""
        for operation in operations:
            if not operation.completed and not operation.error:
                self.execute_sync_operation(relay_id, operation)

    def await_operations(self, relay_id: str, operations: List[SyncOperation]):
        """Wait for specific operations to complete"""
        self.await_all_operations(relay_id, operations)

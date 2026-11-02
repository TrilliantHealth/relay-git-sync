# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a real-time synchronization bridge between Relay Server collaborative documents and Git repositories. The system monitors collaborative documents over websocket subscriptions, with optional webhook ingestion, and maintains synchronized copies in local Git repositories with automatic version control.

## Data-Safety Invariants

Any change to sync behavior must preserve these repository-level invariants:

1. **The mirror follows Relay's deletions and reports bursts.** The mirror
   cannot stop a deletion from reaching Relay; the plugin's DeleteCollector
   does that on each client. Holding a burst back only left the mirror's
   folder map stale, misfiling edits to moved documents. Deletion bursts above
   `max(10% of membership, 25)` are applied after the working tree, uncommitted
   content included, is committed to a `refs/mass-deletions/` ref that is
   pushed, and retried each minute until a remote accepts it. One
   `Mass deletion applied` error names the ref and whether it was pushed.
2. **An empty state vector means unsynced, not empty.** A Y-Doc that decodes
   to zero clients was never written: the server has the guid registered but
   no content was uploaded yet. Fetches defer; they never wipe files.
3. **A genuinely emptied doc (history present, empty text) is authoritative
   for one file, suspicious in bulk.** Truncation bursts above the same
   threshold are gated per sync pass.
4. **A wholly-empty remote map against non-empty local files is a
   publication/reset, never a mass deletion.** The mirror refuses to act on it.
5. **Deletes must stay recoverable.** Deletions are regular commits — never
   force-push, so git history remains the recovery mechanism.
6. **Filemeta carries no content hash for markdown/canvas** — content and
   emptiness live only in the per-document Y-Doc. Never infer document
   content state from filemeta.
7. **Subdocument index heads are change signals, not content.** Persist their
   digests for reconnect catch-up, then fetch only documents whose heads
   changed. A folder event must diff old and new filemeta rather than sweep
   every document.

## Key Architecture

The codebase follows a modular architecture with clear separation of concerns:

### Core Components

- **RelayClient** (`relay_client.py`): Wrapper around the bundled Relay SDK
- **SyncEngine** (`sync_engine.py`): Core synchronization logic for converting Y-Sweet documents to Git repositories
- **OperationsQueue** (`operations_queue.py`): Thread-safe queue for processing sync requests with git commit coordination
- **WebsocketChangeListener** (`websocket_listener.py`): Live events, keepalive, and subdocument-index catch-up
- **WebhookProcessor** (`webhook_handler.py`): Processes incoming webhook notifications from Relay Server
- **WebServer** (`web_server.py`): HTTP server that handles webhook endpoints
- **PersistenceManager** (`persistence.py`): Manages persistent state files and Git repository operations

### Data Models

- **SyncOperation** (`models.py`): Data structure representing file operations (CREATE, UPDATE, RENAME, DELETE)
- **SyncRequest/SyncResult** (`models.py`): Request/response structures for sync operations
- **S3RN Resources** (`s3rn.py`): Resource naming system for Relay Server resources (folders, documents, files, canvas)

### Synchronization Flow

1. **Event Reception** (`websocket_listener.py`, `web_server.py`): Websocket events by default, optional webhooks
2. **Index Catch-up** (`websocket_listener.py`): Persisted subdocument heads select changed documents
3. **Queue Management** (`operations_queue.py`): Duplicate notifications are coalesced and processed by a worker
4. **Document Analysis** (`sync_engine.py`): Folder metadata is diffed; changed documents are fetched
5. **Sync Operations** (`sync_engine.py`): Three-phase sync process with conflict resolution
6. **Git Commits** (`persistence.py`): Automatic commits every 10 seconds when changes are detected

### Data Storage

- **State Directory**: `state/<relay_id>/` contains persistent state per relay:
  - `document_hashes.json`: Hash tracking for change detection
  - `subdoc_heads.json`: Persisted Relay subdocument index head digests
  - `shared_folders.json`: Folder metadata from Relay Server
  - `local_state.json`: Local file tracking per folder
- **Repository Directory**: `repos/<relay_id>/<folder_id>/` contains synchronized content organized by relay and folder

## Development Commands

### Running the Server
```bash
# Run webhook server (default port 8000)
python app.py

# Run with custom configuration
python app.py --port 8080 --commit-interval 30 --relay-server-url "http://localhost:8080" --data-dir "/custom/path"
```

### Dependencies
```bash
# Install dependencies
uv sync

# Install dev dependencies for linting/formatting
uv sync --group dev
```

### Code Quality
```bash
# Format code
black .
isort .

# Run pre-commit hooks
pre-commit run --all-files
```

### Deployment
The project includes Fly.io configuration (`fly.toml`) for deployment with persistent storage mounted at `/data`.

## Key Implementation Details

### Multi-Relay/Multi-Folder Support
- Each relay gets its own state directory and repository structure
- Per-relay and per-folder sync locks prevent concurrent operations
- Hierarchical organization: `repos/<relay_id>/<folder_id>/`

### Resource Type Handling
- **Folders**: Contain `filemeta_v0` Map with file metadata
- **Documents**: Contain `contents` Text with document content  
- **Files**: Binary content with metadata
- **Canvas**: Collaborative whiteboard content

### Conflict Resolution
- Hash-based change detection prevents unnecessary updates
- Rename detection by tracking document IDs across path changes
- Thread-safe operations with per-folder locking

### Authentication
- Supports API key authentication via relay server URL
- SSH key management via environment variable (`persistence.py`)

## File Structure

```
.
├── app.py                 # Main entry point and server orchestration
├── models.py              # Data structures and type definitions
├── relay_client.py        # Y-Sweet DocumentManager wrapper
├── sync_engine.py         # Core synchronization logic
├── operations_queue.py    # Thread-safe request processing
├── webhook_handler.py     # Webhook processing logic
├── web_server.py          # HTTP server implementation
├── persistence.py         # State management and Git operations
├── s3rn.py               # Resource naming and type system
├── repos/                # Synchronized Git repositories
└── state/                # Persistent state files per relay
```

## Testing

- Run the tests with `uv run pytest`

# Offline snapshot maintenance

Storage refusal preserves accepted content and freezes freshness. Do not delete pins
or resize/alter receipts while readers exist. An export lock alone does not fence
Phoenix assets or Store/Reloader readers.

1. Record current/served/previous revisions, allocated/free bytes and inodes, actual
   quota and latest deferred result. Select a maintenance window or a separately
   validated read-only replacement. Avoid route cutover from an unvalidated cache.
2. Drain requests using the app's real gateway/readiness budgets. Stop **all** app,
   exporter, maintenance and volume-owning processes/pods for this exact root,
   including old rollout pods. Verify they are gone and the volume owner is fenced.
   Do not force-delete an old pod as proof it stopped. Leave live storage alone if
   owner disappearance/CSI fencing cannot be established.
3. Take an offline recoverable volume snapshot/backup before changing history.
   Validate current.json, served.json and served.previous.json schema/path/revisions
   and generation existence; null previous is allowed. Inspect a pending served
   publication and recover it using the app's tested rollback protocol before any
   pruning. An invalid/missing receipt or unresolved journal blocks maintenance.
4. Preserve every current/served/previous generation and the known fallback. Inspect
   every permanent pin, including failed candidates: pins may be retired only after
   the stopped-reader proof and backup exist. List proposed unreferenced candidates
   and obtain a second review against the preserved receipts. Never remove the
   receipts, accepted native files, reader-visible generation or export configuration.
5. Prune only those reviewed offline candidates on the detached volume. This document
   deliberately supplies no online deletion command or automatic age-based pin expiry.
   Re-measure allocated bytes/free reserve/inodes and adjust the approved bounded
   session policy from real measurements, not the original illustrative4Gi value.
6. Start exactly one fenced owner. Verify Store content source, served/current/previous
   agreement, accepted native assets, retries, quota refusal and a container/pod restart
   on the intended volume. Restore traffic only after accepted-source checks pass;
   /readyz alone does not prove Relay was accepted. Retain backup until recovery proof
   and review are complete.

This procedure needs an authorized operator and intended-volume validation. It is
not an executed maintenance event. Cross-process online reader retirement remains
additional implementation requiring independent proof.

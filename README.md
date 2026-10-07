# Tuki notification worker

This trusted Node service listens to Firebase Realtime Database and sends FCM notifications for new chats, friend requests and SOS events. It preserves the existing service's follower mirror. It uses the existing Firebase project; it does not enable Firebase billing or deploy Cloud Functions. It must remain running and connected for background push delivery.

## Delivery behavior

- Watches recipient copies of every new message, including the first conversation and rapid messages.
- Uses the sender's profile name, message text, sound and vibration. SOS uses `tuki-sos-v1`; messages and friend requests use `tuki-messages-v1`.
- Rechecks blocks, deletion, read messages, cancelled requests and current SOS episodes before sending. SOS recipients must be mutual friends.
- Uses durable per-event leases and per-token acknowledgements. Retries transient failures without resending successful tokens and removes invalid tokens without deleting a refreshed replacement.
- On startup, considers only the last minute of events. Older history is not pushed to people. Events during a longer service outage will not be recovered as alerts.
- The app can wake this free host after saving a direct message via authenticated `POST /message-wake`. The server verifies the Firebase ID token and fetches that sender's recipient-side message; client-provided text or sender IDs are never trusted. Only messages from the last five minutes qualify. This explicitly recovers a message after a cold start without replaying unrelated history. The normal dispatcher still checks blocks/read/deletion state and deduplicates delivery. The app retries once without blocking message sending.
- `GET /` and `GET /health` expose database connection status and aggregate counters, without tokens, private messages or account IDs. Health returns 503 while disconnected; the root response includes the repair version.

Android requires notification permission and enabled notification channels. Device silent mode, channel sound settings, force-stop and battery restrictions can affect alerts. Build 8 can display local foreground system alerts plus a native app banner; Build 6's installed native dependencies permit an in-app foreground banner and background FCM, but adding foreground system notification support requires a new APK/AAB.

## Run locally

Install dependencies in `server`, then run `node scripts/run-notification-worker.cjs` from the project root. The worker reads `FIREBASE_SERVICE_ACCOUNT` (JSON supplied through a secret environment variable) or the existing local `.secrets/firebase-service-account.json`. Never commit or print that credential.

Optional environment variables:

- `FIREBASE_DATABASE_URL`: the existing project's Realtime Database URL.
- `TUKI_WORKER_PORT` / `PORT`: HTTP port, default `3000`.
- `TUKI_WORKER_HOST`: listener address, default `0.0.0.0`. Use `127.0.0.1` locally.

The repair session temporarily started a hidden local worker on `127.0.0.1:3090`, then stopped it after discovering the existing Render sender to avoid duplicate delivery. Logs are in `release/notification-repair-2026-10-06/worker.stdout.log` and `worker.stderr.log`. The corrected cloud worker still needs deployment to the existing Render service.

Run server regression tests with `npm test` from `server`. `node scripts/audit-notifications.cjs --all` validates registered tokens with FCM dry-run and never sends a test notification. `--prune` additionally removes only tokens that FCM reports as invalid.

## Hosting

For an existing Render service, use root directory `server`, build command `npm ci`, start command `node worker.js`, the existing database URL and a protected `FIREBASE_SERVICE_ACCOUNT` environment variable. Do not copy the credential into logs or release receipts.

Render's free web services spin down when idle and can restart. A health endpoint does not itself provide 24/7 operation. Free hosting therefore cannot guarantee continuous SOS or background notifications. See [Render free service limits](https://render.com/docs/free). No paid plan or cloud deployment was enabled by this repair.

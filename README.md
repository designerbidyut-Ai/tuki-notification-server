# Tuki Free Real-Time Notification & Follower Worker

A zero-cost, lightweight standalone Node.js worker service that listens to Firebase Realtime Database events and dispatches high-importance Firebase Cloud Messaging (FCM) push notifications directly to users' phones.

## Features
- **Zero Cost ($0)**: Runs on Render.com's 100% Free Tier (no credit card required).
- **Real-Time Push Notifications**:
  - Instant chat alerts on Android channel `tuki-messages-v1` with sound and vibration.
  - Friend request alerts.
  - Automatically suppresses alerts for blocked users or users who toggled notifications off.
  - Cleans up stale/unregistered device tokens automatically.
- **Followers Synchronization**: Synchronizes `/following` to `/followers` automatically.
- **Health Check**: Provides `GET /health` and `GET /` endpoints to ensure 24/7 liveness.

---

## Deploying to Render.com (100% Free, No Credit Card)

### Step 1: Sign Up / Log In
1. Go to [https://render.com](https://render.com).
2. Sign in with GitHub or your Google Account (no credit card needed).

### Step 2: Create a Web Service
1. Click **New +** > **Web Service**.
2. Connect your GitHub repository (or select **Public Git repository** if your repo is public).
3. Set the following settings:
   - **Name**: `tuki-notification-worker`
   - **Region**: Singapore (`Singapore (Southeast Asia)`) or Frankfurt/Oregon.
   - **Branch**: `master`
   - **Root Directory**: `server`
   - **Runtime**: `Node`
   - **Build Command**: `npm install`
   - **Start Command**: `node worker.js`
   - **Instance Type**: `Free` ($0/month)

### Step 3: Add Environment Variables
Under the **Environment Variables** section, add:
1. `FIREBASE_DATABASE_URL`:
   ```
   https://livelocation-afb04-default-rtdb.asia-southeast1.firebasedatabase.app
   ```
2. `FIREBASE_SERVICE_ACCOUNT`:
   Run this command locally to get the string:
   ```powershell
   node server/get-render-secret.cjs
   ```
   Paste the generated string into the value box.

### Step 4: Click "Deploy Web Service"
That's it! Render will build and run your service within 60 seconds. Your notifications will be live 24/7!

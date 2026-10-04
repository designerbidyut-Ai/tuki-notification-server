const admin = require('firebase-admin');
const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// 1. Initialize Firebase Admin SDK
function getServiceAccount() {
  if (process.env.FIREBASE_SERVICE_ACCOUNT) {
    try {
      const raw = process.env.FIREBASE_SERVICE_ACCOUNT.trim();
      if (raw.startsWith('{')) {
        return JSON.parse(raw);
      }
      return JSON.parse(Buffer.from(raw, 'base64').toString('utf8'));
    } catch (e) {
      console.error('Failed to parse FIREBASE_SERVICE_ACCOUNT environment variable:', e.message);
    }
  }

  const localCandidates = [
    path.join(__dirname, '../.secrets/firebase-service-account.json'),
    path.join(__dirname, '.secrets/firebase-service-account.json'),
    path.join(__dirname, 'firebase-service-account.json'),
  ];

  for (const candidate of localCandidates) {
    if (fs.existsSync(candidate)) {
      console.log(`Loading service account from: ${candidate}`);
      return JSON.parse(fs.readFileSync(candidate, 'utf8'));
    }
  }

  throw new Error('No Firebase service account credentials found. Set FIREBASE_SERVICE_ACCOUNT env var or place firebase-service-account.json in .secrets/.');
}

const serviceAccount = getServiceAccount();

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DATABASE_URL || 'https://livelocation-afb04-default-rtdb.asia-southeast1.firebasedatabase.app',
});

const db = admin.database();
const messaging = admin.messaging();

console.log('Firebase Admin initialized for project:', serviceAccount.project_id);

const stats = {
  startedAt: new Date().toISOString(),
  messagesProcessed: 0,
  notificationsSent: 0,
  friendRequestsProcessed: 0,
  followersSynced: 0,
  errors: 0,
};

const digest = (val) => crypto.createHash('sha256').update(String(val)).digest('hex');

// Track processed events in-memory to prevent duplicates
const processedEvents = new Set();
function markProcessed(id) {
  processedEvents.add(id);
  if (processedEvents.size > 2000) {
    const first = processedEvents.values().next().value;
    processedEvents.delete(first);
  }
}

// 2. Notification Dispatcher
async function sendPushNotification(recipientUid, senderId, type, eventId, customText) {
  try {
    if (!recipientUid || !senderId) return;

    // Check if recipient has blocked sender or vice versa
    const [block1, block2, deletion1, deletion2, disabled] = await Promise.all([
      db.ref(`blocks/${recipientUid}/${senderId}`).get(),
      db.ref(`blocks/${senderId}/${recipientUid}`).get(),
      db.ref(`deletionRequests/${recipientUid}`).get(),
      db.ref(`deletionRequests/${senderId}`).get(),
      db.ref(`notifications_disabled/${recipientUid}`).get(),
    ]);

    if (block1.exists() || block2.exists()) {
      console.log(`[Notification] Suppressed due to block between ${senderId} and ${recipientUid}`);
      return;
    }

    if (deletion1.exists() || deletion2.exists()) return;
    if (disabled.val() === 'true' || disabled.val() === true) {
      console.log(`[Notification] User ${recipientUid} has notifications disabled.`);
      return;
    }

    const key = digest(eventId || `${senderId}_${recipientUid}_${Date.now()}`);

    // Create inbox item
    await db.ref(`notifications/${recipientUid}/${key}`).transaction((existing) => {
      if (existing) return undefined;
      return {
        senderId,
        type,
        createdAt: Date.now(),
        read: false,
      };
    });

    // Fetch device tokens
    const tokensSnap = await db.ref(`pushTokens/${recipientUid}`).get();
    if (!tokensSnap.exists()) {
      console.log(`[Notification] No push tokens found for recipient ${recipientUid}`);
      return;
    }

    const tokensObj = tokensSnap.val() || {};
    const entries = Object.entries(tokensObj);
    if (entries.length === 0) return;

    const titles = {
      message: 'New message',
      friend: 'New friend request',
      nearby: 'A friend is nearby',
    };

    const bodies = {
      message: customText ? (customText.length > 50 ? customText.slice(0, 47) + '...' : customText) : 'Open Tuki to read your message.',
      friend: 'Someone wants to connect with you on Tuki.',
      nearby: 'Your friend was just seen within 100 m. Open Tuki to see who.',
    };

    const payload = {
      tokens: entries.map(([, token]) => token),
      notification: {
        title: titles[type] || 'Tuki alert',
        body: bodies[type] || 'Open Tuki to view.',
      },
      data: {
        type,
        senderId,
        eventId: key,
      },
      android: {
        priority: 'high',
        notification: {
          channelId: 'tuki-messages-v1',
          tag: key,
          sound: 'default',
        },
      },
    };

    const response = await messaging.sendEachForMulticast(payload);
    stats.notificationsSent += response.successCount;
    console.log(`[Notification] Sent ${type} to ${recipientUid}: ${response.successCount} success, ${response.failureCount} fail`);

    // Clean up invalid tokens
    for (let i = 0; i < response.responses.length; i++) {
      const res = response.responses[i];
      if (!res.success) {
        const errCode = res.error?.code;
        if (errCode === 'messaging/registration-token-not-registered' || errCode === 'messaging/invalid-registration-token') {
          const [deviceId] = entries[i];
          console.log(`[Notification] Removing invalid token for device: ${deviceId}`);
          await db.ref(`pushTokens/${recipientUid}/${deviceId}`).remove();
        }
      }
    }
  } catch (err) {
    stats.errors++;
    console.error(`[Notification Error]`, err.message);
  }
}

// 3. Realtime Listeners

// A) Watch Conversations for New Messages
let isInitialMessagesLoad = true;
const knownLastTimestamps = new Map();

function startMessagesListener() {
  console.log('[Listener] Starting conversations listener for chat messages...');
  const ref = db.ref('conversations');

  ref.on('child_changed', async (snap) => {
    const userA = snap.key;
    const peers = snap.val() || {};

    for (const [userB, timestamp] of Object.entries(peers)) {
      const pairKey = [userA, userB].sort().join('_');
      const lastTs = knownLastTimestamps.get(pairKey) || 0;

      if (timestamp > lastTs) {
        knownLastTimestamps.set(pairKey, timestamp);

        if (!isInitialMessagesLoad) {
          // Fetch the latest message
          const msgSnap = await db.ref(`messages/${userA}/${userB}`).limitToLast(1).get();
          if (msgSnap.exists()) {
            const msgs = msgSnap.val() || {};
            const msgId = Object.keys(msgs)[0];
            const msg = msgs[msgId];

            if (msg && !processedEvents.has(msgId)) {
              markProcessed(msgId);
              stats.messagesProcessed++;

              const recipient = msg.senderId === userA ? userB : userA;
              console.log(`[Chat] New message from ${msg.senderId} to ${recipient}: "${msg.text?.slice(0, 20)}..."`);
              await sendPushNotification(recipient, msg.senderId, 'message', msgId, msg.text);
            }
          }
        }
      }
    }
  });

  // Seed initial timestamps so we don't alert on past historical messages
  ref.once('value', (snap) => {
    const all = snap.val() || {};
    for (const [userA, peers] of Object.entries(all)) {
      for (const [userB, ts] of Object.entries(peers || {})) {
        const pairKey = [userA, userB].sort().join('_');
        knownLastTimestamps.set(pairKey, Math.max(knownLastTimestamps.get(pairKey) || 0, ts));
      }
    }
    isInitialMessagesLoad = false;
    console.log(`[Listener] Seeded ${knownLastTimestamps.size} conversation pairs. Ready for live messages.`);
  });
}

// B) Watch Friend Requests
let isInitialFriendRequestsLoad = true;
const knownFriendRequests = new Set();

function startFriendRequestsListener() {
  console.log('[Listener] Starting friendRequests listener...');
  const ref = db.ref('friendRequests');

  ref.on('child_added', (snap) => {
    const recipient = snap.key;
    const senders = snap.val() || {};

    for (const sender of Object.keys(senders)) {
      const reqId = `${recipient}_${sender}`;
      if (!knownFriendRequests.has(reqId)) {
        knownFriendRequests.add(reqId);

        if (!isInitialFriendRequestsLoad) {
          stats.friendRequestsProcessed++;
          console.log(`[Friend] New request: ${sender} -> ${recipient}`);
          sendPushNotification(recipient, sender, 'friend', `friend_${reqId}_${Date.now()}`);
        }
      }
    }
  });

  ref.on('child_changed', (snap) => {
    const recipient = snap.key;
    const senders = snap.val() || {};

    for (const sender of Object.keys(senders)) {
      const reqId = `${recipient}_${sender}`;
      if (!knownFriendRequests.has(reqId)) {
        knownFriendRequests.add(reqId);
        stats.friendRequestsProcessed++;
        console.log(`[Friend] New request (updated): ${sender} -> ${recipient}`);
        sendPushNotification(recipient, sender, 'friend', `friend_${reqId}_${Date.now()}`);
      }
    }
  });

  ref.once('value', (snap) => {
    const all = snap.val() || {};
    for (const [recipient, senders] of Object.entries(all)) {
      for (const sender of Object.keys(senders || {})) {
        knownFriendRequests.add(`${recipient}_${sender}`);
      }
    }
    isInitialFriendRequestsLoad = false;
    console.log(`[Listener] Seeded ${knownFriendRequests.size} existing friend requests. Ready for live requests.`);
  });
}

// C) Sync Followers Count
function startFollowersSyncListener() {
  console.log('[Listener] Starting follower synchronization listener...');
  const ref = db.ref('following');

  ref.on('child_added', (snap) => {
    const uid = snap.key;
    const peers = snap.val() || {};
    for (const peer of Object.keys(peers)) {
      db.ref(`followers/${peer}/${uid}`).set(true);
      stats.followersSynced++;
    }
  });

  ref.on('child_changed', async (snap) => {
    const uid = snap.key;
    const currentFollowing = snap.val() || {};
    const existingFollowers = (await db.ref('followers').get()).val() || {};

    for (const [peer, followersObj] of Object.entries(existingFollowers)) {
      if (followersObj && followersObj[uid] && !currentFollowing[peer]) {
        await db.ref(`followers/${peer}/${uid}`).remove();
        console.log(`[Follow] Unfollowed: ${uid} -> ${peer}`);
      }
    }

    for (const peer of Object.keys(currentFollowing)) {
      await db.ref(`followers/${peer}/${uid}`).set(true);
      stats.followersSynced++;
    }
  });
}

// Start all listeners
startMessagesListener();
startFriendRequestsListener();
startFollowersSyncListener();

// 4. Express Health Check Server (Required for Render.com free hosting)
const app = express();
const PORT = process.env.PORT || 3000;

app.get('/', (req, res) => {
  res.json({
    status: 'online',
    service: 'tuki-notification-worker',
    uptimeSeconds: Math.floor(process.uptime()),
    stats,
  });
});

app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

const server = app.listen(PORT, () => {
  console.log(`🚀 Tuki Notification Worker listening on port ${PORT}`);
});

process.on('SIGINT', () => {
  console.log('Shutting down gracefully...');
  server.close(() => process.exit(0));
});

process.on('SIGTERM', () => {
  console.log('Shutting down gracefully...');
  server.close(() => process.exit(0));
});

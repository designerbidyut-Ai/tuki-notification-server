FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY worker.js notification-worker.cjs notification-core.cjs notification-policy.cjs notification-queue.cjs followers-sync.cjs message-wake.cjs location-refresh.cjs profile-media.cjs public-geo-index.cjs thread-summaries.cjs circle-summaries.cjs account-jobs.cjs account-cleanup.cjs moderation.cjs city-join.cjs ./
EXPOSE 3000
CMD ["node", "worker.js"]

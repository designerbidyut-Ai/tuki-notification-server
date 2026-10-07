FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev
COPY worker.js notification-worker.cjs notification-core.cjs followers-sync.cjs message-wake.cjs ./
EXPOSE 3000
CMD ["node", "worker.js"]

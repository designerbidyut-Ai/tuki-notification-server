FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install --omit=dev
COPY worker.js ./
EXPOSE 3000
CMD ["node", "worker.js"]

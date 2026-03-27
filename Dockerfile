FROM node:20-bookworm

WORKDIR /app

COPY package*.json ./
RUN npm ci

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
RUN npx -y playwright@1.39.0 install --with-deps chromium

COPY . .
RUN npm run build

ENV NODE_ENV=production
ENV PORT=3000

EXPOSE 3000

RUN chmod +x /app/docker-start.sh

CMD ["/app/docker-start.sh"]

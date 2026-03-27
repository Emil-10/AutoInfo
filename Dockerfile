FROM mcr.microsoft.com/playwright:v1.39.0-jammy

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .
RUN npm run build

ENV NODE_ENV=production
ENV PORT=3000

EXPOSE 3000

CMD ["bash", "-lc", "xvfb-run -a npm start"]

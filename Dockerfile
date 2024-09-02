FROM node:18-alpine as base
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install

FROM base AS app
COPY . .
RUN npm run build
EXPOSE 8080
CMD ["node", "dist/index.js"]


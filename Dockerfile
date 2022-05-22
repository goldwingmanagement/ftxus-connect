FROM node:latest

WORKDIR /app

COPY package.json package.json

RUN npm install

COPY . .

RUN npm run build

ENTRYPOINT ["node", "./lib/index.js"]
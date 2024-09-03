FROM oven/bun:latest

WORKDIR /app

RUN mkdir -p /var/log/clickhouse-server
RUN chmod 755 /var/log/clickhouse-server

COPY package.json .
COPY bun.lockb .

RUN bun install

COPY . .

CMD ["bun", "run", "start"]
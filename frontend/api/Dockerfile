# 1. Build the project
FROM node:22-slim AS builder

WORKDIR /app

COPY frontend/api/package.json frontend/api/yarn.lock ./
RUN yarn install --frozen-lockfile --non-interactive
COPY frontend/api/ .
RUN yarn build

# 2. Run the app
FROM node:22-slim AS runner

WORKDIR /app

COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package.json /app/yarn.lock ./
# Install only prod dependencies to keep image small
RUN yarn install --production --frozen-lockfile --non-interactive

CMD ["npx", \
     "@google-cloud/functions-framework", \
     "--target=api", \
     "--signature-type=http"]

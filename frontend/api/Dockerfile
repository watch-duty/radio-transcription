# 1. Build the project
FROM node:22-slim AS builder

# Build the common folder first
WORKDIR /common
COPY frontend/common/package.json frontend/common/yarn.lock ./
RUN --mount=type=cache,target=/usr/local/share/.cache/yarn \
    yarn install --frozen-lockfile --non-interactive
COPY frontend/common/ .
RUN yarn build

# Build the api folder
WORKDIR /app
COPY frontend/api/package.json frontend/api/yarn.lock ./
RUN --mount=type=cache,target=/usr/local/share/.cache/yarn \
    yarn install --frozen-lockfile --non-interactive
COPY frontend/api/ .
RUN yarn build

# 2. Run the app
FROM node:22-slim AS runner

# Copy the built common folder so the api can resolve the dependency at runtime
WORKDIR /common
COPY --from=builder /common/package.json ./
COPY --from=builder /common/dist ./dist

WORKDIR /app
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package.json /app/yarn.lock ./
# Install only prod dependencies to keep image small
RUN --mount=type=cache,target=/usr/local/share/.cache/yarn \
    yarn install --production --frozen-lockfile --non-interactive

CMD ["npx", \
     "@google-cloud/functions-framework", \
     "--target=api", \
     "--signature-type=http"]

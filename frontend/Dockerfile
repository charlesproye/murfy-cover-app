# Stage 1: Dependencies
FROM node:20-bookworm-slim AS deps

ENV DEBIAN_FRONTEND=noninteractive

# Required for building sharp and other native modules
RUN apt-get update && apt-get install -y --no-install-recommends \
  build-essential \
  python3 \
  libvips-dev \
  pkg-config \
  autoconf \
  automake \
  libtool \
  ca-certificates \
  git \
  && rm -rf /var/lib/apt/lists/*

RUN corepack enable && corepack prepare pnpm@9.15.4 --activate

WORKDIR /app

COPY package.json pnpm-lock.yaml ./

# Install all dependencies (this compiles sharp)
RUN pnpm install --no-frozen-lockfile


# Stage 2: Build
FROM node:20-bookworm-slim AS builder
RUN corepack enable && corepack prepare pnpm@9.15.4 --activate

WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY . .

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV NEXT_PUBLIC_API_URL=https://get-evalue.com/api/v1

RUN pnpm build

# Stage 3: Production runtime
FROM node:20-bookworm-slim AS runner

ENV DEBIAN_FRONTEND=noninteractive

# Runtime dependencies for sharp's shared objects
RUN apt-get update && apt-get install -y --no-install-recommends \
    libvips42 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ENV PORT=3000
ENV HOSTNAME="0.0.0.0"

# Create a non-root user
RUN addgroup --system --gid 1001 nodejs && \
    adduser --system --uid 1001 nextjs

# Copy Next.js standalone output (self-contained server)
COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

# Copy only sharp (compiled in deps)
COPY --from=deps /app/node_modules/sharp ./node_modules/sharp

# Adjust ownership
RUN chown -R nextjs:nodejs /app
USER 1001

EXPOSE 3000
CMD ["node", "server.js"]

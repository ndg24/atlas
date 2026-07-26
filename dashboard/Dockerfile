# Builds the dashboard as a Next.js standalone server (next.config.mjs's
# output: "standalone") so the runtime image only needs the traced
# dependency subset, not the whole node_modules tree -- mirrors the
# multi-stage build+copy shape of coordinator/Dockerfile and engine/Dockerfile.
FROM node:20-alpine AS builder
WORKDIR /build/dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ .
RUN npm run build

FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY --from=builder /build/dashboard/.next/standalone ./
COPY --from=builder /build/dashboard/.next/static ./.next/static
COPY --from=builder /build/dashboard/public ./public
EXPOSE 3000
CMD ["node", "server.js"]

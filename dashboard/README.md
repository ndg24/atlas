# Atlas Dashboard

A Next.js app for browsing datasets, running SQL/NL queries against the
coordinator (with the optimizer's plan tree via `/explain`), viewing the AI
analyst's data-quality/outlier/trend insights, running multi-agent research
questions and reading the cited report, and reviewing query history.

## How it talks to the coordinator

There's no login flow anywhere in Atlas yet, and the coordinator has no CORS
support, so this app never calls the coordinator directly from the browser.
Instead, server-side Next.js Route Handlers (`app/api/atlas/[...path]`,
`app/api/query`, `app/api/query-nl`) hold a bearer token and proxy every
request; the browser only ever talks to this same-origin app.

## Running locally (native, not Docker)

1. Start the rest of the stack (Postgres, MinIO, Redis, catalog, coordinator,
   workers) -- see the repo root README's "Getting Started".
2. Mint a dev token (there's no login flow, so this is the only way to get
   one):
   ```
   JWT_SECRET=<same secret the coordinator is running with> \
     go run ./coordinator/cmd/tokengen -user-id dev-user \
     -workspace-id 00000000-0000-0000-0000-000000000001 -ttl 24h
   ```
3. `cp .env.local.example .env.local` and fill in `ATLAS_TOKEN` with that
   token (and `ATLAS_COORDINATOR_URL` if the coordinator isn't on the
   default `localhost:8080`).
4. `npm install && npm run dev` -- http://localhost:3000

## Running via Docker Compose

`deploy/docker-compose.yml` has a `dashboard` service, but `ATLAS_TOKEN`
must already be exported in your shell before `docker compose up` --
tokengen only needs the shared `JWT_SECRET` value, not a running
coordinator, so it's not actually circular:

```
JWT_SECRET=dev-insecure-secret-change-me go run ./coordinator/cmd/tokengen \
  -user-id dev-user -workspace-id 00000000-0000-0000-0000-000000000001 -ttl 24h
export ATLAS_TOKEN=<result>
docker compose -f deploy/docker-compose.yml up
```

(`dev-insecure-secret-change-me` is the `JWT_SECRET` compose sets for the
`coordinator` service -- match whatever you've actually configured there.)

## Testing

```
npm run lint
npm test
npm run build
```

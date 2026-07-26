-- Backs the login/signup flow (coordinator's POST /auth/signup, /auth/login):
-- until now the only way to get a bearer token was the dev-facing tokengen
-- CLI against a shared secret, with no user-facing credential check at all.
-- Defaulting to '' keeps this additive for any user row that might already
-- exist (none are seeded today, but the default avoids a NOT NULL failure
-- either way).
ALTER TABLE users ADD COLUMN password_hash TEXT NOT NULL DEFAULT '';

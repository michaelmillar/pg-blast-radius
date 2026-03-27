ALTER TABLE users ADD COLUMN email TEXT;
ALTER TABLE users ALTER COLUMN email SET NOT NULL;
CREATE INDEX idx_users_email ON users (email);

-- Castor Auth Service Database Schema (PostgreSQL 17)
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    username VARCHAR(64) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    role VARCHAR(16) NOT NULL DEFAULT 'USER',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);

CREATE TABLE IF NOT EXISTS s3_credentials (
    access_key_id VARCHAR(32) PRIMARY KEY,
    secret_access_key VARCHAR(64) NOT NULL,
    user_id UUID REFERENCES users(id) ON DELETE CASCADE,
    label VARCHAR(64),
    status VARCHAR(16) DEFAULT 'ACTIVE',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_s3_credentials_user_id ON s3_credentials(user_id);

#!/bin/bash
# Setup PostgreSQL database user for ETL application
# This runs from the VM which has access to the private PostgreSQL endpoint

set -e

# Check required environment variables
if [ -z "$POSTGRES_HOST" ] || [ -z "$POSTGRES_ADMIN_USER" ] || [ -z "$POSTGRES_ADMIN_PASSWORD" ] || \
   [ -z "$DATABASE_NAME" ] || [ -z "$DB_USERNAME" ] || [ -z "$DB_PASSWORD" ]; then
  echo "Error: Required environment variables not set"
  echo "Required: POSTGRES_HOST, POSTGRES_ADMIN_USER, POSTGRES_ADMIN_PASSWORD, DATABASE_NAME, DB_USERNAME, DB_PASSWORD"
  exit 1
fi

echo "Setting up database user: $DB_USERNAME"

# Check if user already exists
USER_EXISTS=$(PGPASSWORD="$POSTGRES_ADMIN_PASSWORD" psql \
  "host=$POSTGRES_HOST port=5432 dbname=postgres user=$POSTGRES_ADMIN_USER sslmode=require" \
  -tAc "SELECT 1 FROM pg_roles WHERE rolname='$DB_USERNAME'")

if [ "$USER_EXISTS" = "1" ]; then
  echo "User $DB_USERNAME already exists, updating password..."
  PGPASSWORD="$POSTGRES_ADMIN_PASSWORD" psql \
    "host=$POSTGRES_HOST port=5432 dbname=postgres user=$POSTGRES_ADMIN_USER sslmode=require" \
    -c "ALTER USER \"$DB_USERNAME\" WITH PASSWORD '$DB_PASSWORD';"
else
  echo "Creating user $DB_USERNAME..."
  PGPASSWORD="$POSTGRES_ADMIN_PASSWORD" psql \
    "host=$POSTGRES_HOST port=5432 dbname=postgres user=$POSTGRES_ADMIN_USER sslmode=require" \
    -c "CREATE USER \"$DB_USERNAME\" WITH PASSWORD '$DB_PASSWORD' LOGIN;"
fi

echo "Granting privileges on database $DATABASE_NAME..."
PGPASSWORD="$POSTGRES_ADMIN_PASSWORD" psql \
  "host=$POSTGRES_HOST port=5432 dbname=postgres user=$POSTGRES_ADMIN_USER sslmode=require" \
  <<EOF
-- Grant database connect privilege
GRANT CONNECT ON DATABASE "$DATABASE_NAME" TO "$DB_USERNAME";

-- Connect to the target database to grant schema/table privileges
\c "$DATABASE_NAME"

-- Grant schema privileges
GRANT USAGE, CREATE ON SCHEMA public TO "$DB_USERNAME";

-- Grant privileges on all existing tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "$DB_USERNAME";

-- Grant privileges on all existing sequences
GRANT SELECT, UPDATE, USAGE ON ALL SEQUENCES IN SCHEMA public TO "$DB_USERNAME";

-- Grant default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "$DB_USERNAME";

-- Grant default privileges for future sequences
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, UPDATE, USAGE ON SEQUENCES TO "$DB_USERNAME";
EOF

echo "Database user setup complete!"

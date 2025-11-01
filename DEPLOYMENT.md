# Manual Deployment Guide

This guide walks through manually deploying the Fund Lens ETL application to the Azure VM.

## Prerequisites

- Terraform infrastructure deployed (VM, PostgreSQL database created)
- SSH access to the VM configured
- FEC API key
- PostgreSQL admin credentials

## Step 1: Get Infrastructure Values

From your local machine, in the `terraform/` directory:

```bash
cd terraform

# Get these values - you'll need them later
terraform output resource_group_name
terraform output vm_name
terraform output admin_username
terraform output vm_public_ip
terraform output postgres_fqdn
terraform output database_name
terraform output database_username
terraform output ssh_command
```

Save these values - you'll need them throughout the deployment.

## Step 2: SSH to VM

```bash
# Use the SSH command from terraform output
ssh <admin_username>@<vm_public_ip>
```

## Step 3: Install System Dependencies

```bash
sudo apt-get update
sudo apt-get install -y \
  python3.12 \
  python3.12-venv \
  python3-pip \
  git \
  postgresql-client \
  build-essential \
  libpq-dev
```

## Step 4: Clone Repository

```bash
# Create application directory
sudo mkdir -p /opt/fund-lens-etl
sudo chown $USER:$USER /opt/fund-lens-etl

# Clone repository into the directory (note the '.' at the end)
cd /opt/fund-lens-etl
git clone https://github.com/<your-username>/fund-lens-etl.git .
```

## Step 5: Install Poetry

```bash
curl -sSL https://install.python-poetry.org | python3.12 -
export PATH="$HOME/.local/bin:$PATH"

# Add to ~/.bashrc for future sessions
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
```

## Step 6: Install Python Dependencies

```bash
cd /opt/fund-lens-etl

# Configure poetry to create venv in project directory
poetry config virtualenvs.in-project true

# Install only production dependencies
poetry install --only main
```

## Step 7: Create Environment File

Create `.env` file with your configuration:

```bash
cd /opt/fund-lens-etl
cat > .env << 'EOF'
DATABASE_URL=postgresql://<database_username>:<db_password>@<postgres_fqdn>:5432/<database_name>
FEC_API_KEY=<your_fec_api_key>
PREFECT_API_URL=http://localhost:4200/api
EOF
```

Replace the placeholders:
- `<database_username>` - from `terraform output database_username`
- `<db_password>` - your chosen database password
- `<postgres_fqdn>` - from `terraform output postgres_fqdn`
- `<database_name>` - from `terraform output database_name`
- `<your_fec_api_key>` - your FEC API key

## Step 8: Create Database User

Connect to PostgreSQL as admin and create the application user:

```bash
# Set variables for convenience
POSTGRES_HOST=<postgres_fqdn>
POSTGRES_ADMIN_USER=<admin_username>  # e.g., cfadmin
POSTGRES_ADMIN_PASSWORD=<admin_password>
DATABASE_NAME=<database_name>
DB_USERNAME=<database_username>
DB_PASSWORD=<your_chosen_db_password>

# Connect to PostgreSQL as admin
PGPASSWORD="$POSTGRES_ADMIN_PASSWORD" psql \
  "host=$POSTGRES_HOST port=5432 dbname=postgres user=$POSTGRES_ADMIN_USER sslmode=require"
```

In the PostgreSQL prompt, run:

```sql
-- Create user (if doesn't exist)
CREATE USER "<database_username>" WITH PASSWORD '<your_db_password>' LOGIN;

-- Grant database connect privilege
GRANT CONNECT ON DATABASE "<database_name>" TO "<database_username>";

-- Connect to the target database
\c "<database_name>"

-- Grant schema privileges
GRANT USAGE, CREATE ON SCHEMA public TO "<database_username>";

-- Grant privileges on all existing tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO "<database_username>";

-- Grant privileges on all existing sequences
GRANT SELECT, UPDATE, USAGE ON ALL SEQUENCES IN SCHEMA public TO "<database_username>";

-- Grant default privileges for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO "<database_username>";

-- Grant default privileges for future sequences
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, UPDATE, USAGE ON SEQUENCES TO "<database_username>";

-- Exit
\q
```

## Step 9: Run Database Migrations

```bash
cd /opt/fund-lens-etl
poetry run alembic upgrade head
```

## Step 10: Set Up Prefect Server Service

```bash
# Copy service file
sudo cp /opt/fund-lens-etl/scripts/systemd/prefect-server.service /etc/systemd/system/

# Update VM_USER in service file
sudo sed -i "s/\${VM_USER}/$USER/g" /etc/systemd/system/prefect-server.service

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable prefect-server.service
sudo systemctl start prefect-server.service

# Check status
sudo systemctl status prefect-server.service

# View logs if needed
sudo journalctl -u prefect-server -f
```

Wait a few seconds for the Prefect server to start up.

## Step 11: Deploy Prefect Flows

```bash
cd /opt/fund-lens-etl

# Deploy all flows
poetry run python scripts/deploy.py --all

# Or deploy individually:
# poetry run python scripts/deploy.py --flow bronze-daily
# poetry run python scripts/deploy.py --flow bronze-monthly
# poetry run python scripts/deploy.py --flow silver
# poetry run python scripts/deploy.py --flow gold
```

## Step 12: Set Up Prefect Worker Service

```bash
# Copy service file
sudo cp /opt/fund-lens-etl/scripts/systemd/prefect-worker.service /etc/systemd/system/

# Update VM_USER in service file
sudo sed -i "s/\${VM_USER}/$USER/g" /etc/systemd/system/prefect-worker.service

# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable prefect-worker.service
sudo systemctl start prefect-worker.service

# Check status
sudo systemctl status prefect-worker.service

# View logs if needed
sudo journalctl -u prefect-worker -f
```

## Step 13: Verify Deployment

### Check Services

```bash
# Check Prefect server
sudo systemctl status prefect-server.service

# Check Prefect worker
sudo systemctl status prefect-worker.service
```

### Access Prefect UI

From your local machine, open a browser to:
```
http://<vm_public_ip>:4200
```

You should see:
- All four flow deployments (bronze-daily, bronze-monthly, silver, gold)
- Upcoming scheduled runs
- The worker should show as online

### View Logs

```bash
# Server logs
sudo journalctl -u prefect-server -f

# Worker logs
sudo journalctl -u prefect-worker -f
```

## Scheduled Runs

Once deployed, flows will run automatically:

**Daily (Mon-Fri):**
- 2:00 AM ET - Bronze ingestion (incremental)
- 2:30 AM ET - Silver transformation
- 3:30 AM ET - Gold transformation

**Monthly (First Sunday of month):**
- 3:00 AM ET - Bronze ingestion (full refresh)
- 3:30 AM ET - Silver transformation
- 4:30 AM ET - Gold transformation

## Updating the Application

To deploy code changes:

```bash
# SSH to VM
ssh <admin_username>@<vm_public_ip>

# Pull latest code
cd /opt/fund-lens-etl
git pull origin main

# Install any new dependencies
poetry install --only main

# Run any new migrations
poetry run alembic upgrade head

# Re-deploy flows (if flow code changed)
poetry run python scripts/deploy.py --all

# Restart worker to pick up code changes
sudo systemctl restart prefect-worker.service
```

## Troubleshooting

### Database Connection Issues

Test connection:
```bash
cd /opt/fund-lens-etl
poetry run python -c "from sqlalchemy import create_engine; from dotenv import load_dotenv; import os; load_dotenv(); engine = create_engine(os.getenv('DATABASE_URL')); print('Connected!' if engine.connect() else 'Failed')"
```

### Prefect Server Not Starting

```bash
# Check logs
sudo journalctl -u prefect-server -n 50

# Try starting manually to see errors
cd /opt/fund-lens-etl
poetry run prefect server start --host 0.0.0.0
```

### Prefect Worker Not Picking Up Runs

```bash
# Check worker logs
sudo journalctl -u prefect-worker -n 50

# Verify worker is connected (check Prefect UI)
# Restart worker
sudo systemctl restart prefect-worker.service
```

### Flow Deployment Issues

```bash
# Check if Prefect server is accessible
curl http://localhost:4200/api/health

# Re-deploy specific flow
cd /opt/fund-lens-etl
poetry run python scripts/deploy.py --flow bronze-daily
```

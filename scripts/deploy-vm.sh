#!/bin/bash
set -e

echo "=== Starting deployment ==="

# Install system dependencies
echo "Checking system dependencies..."
sudo apt-get update -qq
sudo apt-get install -y -qq \
  python3.12 \
  python3.12-venv \
  python3-pip \
  git \
  postgresql-client \
  build-essential \
  libpq-dev

# Create directory if it doesn't exist
if [ ! -d "/opt/fund-lens-etl" ]; then
  echo "Creating /opt/fund-lens-etl directory..."
  sudo mkdir -p /opt/fund-lens-etl
  sudo chown ${VM_USER}:${VM_USER} /opt/fund-lens-etl
fi

cd /opt/fund-lens-etl

# Clone or update repository
if [ ! -d ".git" ]; then
  echo "Cloning repository..."
  git clone https://github.com/${GITHUB_REPO}.git .
else
  echo "Pulling latest code..."
  git pull origin main
fi

# Install poetry if not present
if ! command -v poetry &> /dev/null; then
  echo "Installing poetry..."
  curl -sSL https://install.python-poetry.org | python3.12 -
  export PATH="$HOME/.local/bin:$PATH"
fi

# Configure poetry to create venv in project directory
poetry config virtualenvs.in-project true

# Install dependencies (production only, no dev dependencies)
echo "Installing Python dependencies with poetry..."
poetry install --only main --no-interaction --no-ansi

# Update environment variables
echo "Updating environment variables..."
cat > .env << EOF
DATABASE_URL=${DATABASE_URL}
FEC_API_KEY=${FEC_API_KEY}
PREFECT_API_URL=http://localhost:4200/api
EOF

# Setup database user (if not already exists)
echo "Setting up database user..."
POSTGRES_HOST=${POSTGRES_HOST} \
POSTGRES_ADMIN_USER=${POSTGRES_ADMIN_USER} \
POSTGRES_ADMIN_PASSWORD=${POSTGRES_ADMIN_PASSWORD} \
DATABASE_NAME=${DATABASE_NAME} \
DB_USERNAME=${DB_USERNAME} \
DB_PASSWORD=${DB_PASSWORD} \
bash /opt/fund-lens-etl/scripts/setup-db-user.sh

# Run database migrations
echo "Running database migrations..."
poetry run alembic upgrade head

# Create Prefect server systemd service if it doesn't exist
if [ ! -f "/etc/systemd/system/prefect-server.service" ]; then
  echo "Creating Prefect server systemd service..."
  sudo cp /opt/fund-lens-etl/scripts/systemd/prefect-server.service /etc/systemd/system/
  sudo sed -i "s/\${VM_USER}/${VM_USER}/g" /etc/systemd/system/prefect-server.service

  # Enable and start server
  sudo systemctl daemon-reload
  sudo systemctl enable prefect-server.service
  sudo systemctl start prefect-server.service
  echo "Prefect server service created and started"

  # Wait for server to be ready
  echo "Waiting for Prefect server to start..."
  sleep 5
fi

# Deploy Prefect flows
echo "Deploying Prefect flows..."
poetry run python scripts/deploy.py --all

# Create worker systemd service if it doesn't exist
if [ ! -f "/etc/systemd/system/prefect-worker.service" ]; then
  echo "Creating Prefect worker systemd service..."
  sudo cp /opt/fund-lens-etl/scripts/systemd/prefect-worker.service /etc/systemd/system/
  sudo sed -i "s/\${VM_USER}/${VM_USER}/g" /etc/systemd/system/prefect-worker.service

  # Enable and start service
  sudo systemctl daemon-reload
  sudo systemctl enable prefect-worker.service
  sudo systemctl start prefect-worker.service
  echo "Prefect worker service created and started"
else
  # Restart existing service
  echo "Restarting Prefect worker service..."
  sudo systemctl restart prefect-worker.service
fi

# Check service status
echo "Checking Prefect server status..."
sudo systemctl status prefect-server.service --no-pager || true

echo ""
echo "Checking Prefect worker status..."
sudo systemctl status prefect-worker.service --no-pager || true

echo ""
echo "=== Deployment completed successfully ==="

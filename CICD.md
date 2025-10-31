# CI/CD Pipeline

Automated deployment pipeline using GitHub Actions for infrastructure provisioning and Prefect flow deployment.

## Workflow

```
Push to main
    ↓
Terraform Apply (Infrastructure)
  • Azure VM with managed identity
  • PostgreSQL database (on shared server)
  • Networking and storage
    ↓
Deploy Prefect Flows (ETL Pipeline)
  • Create database user (from VM via private endpoint)
  • Run Alembic migrations
  • Self-hosted Prefect server
  • Prefect worker with systemd
  • Flow deployments with schedules
    ↓
Scheduled Execution (Automatic)
```

## Triggers

The deployment workflow runs on:
- Push to `main` branch (when code or infrastructure changes)
- Manual trigger via GitHub Actions UI

## Required GitHub Secrets

Configure these in your GitHub repository settings:

### Azure Infrastructure
- `ARM_CLIENT_ID` - Azure service principal client ID
- `ARM_TENANT_ID` - Azure tenant ID
- `ARM_SUBSCRIPTION_ID` - Azure subscription ID
- `TF_STATE_RESOURCE_GROUP` - Terraform state resource group
- `TF_STATE_STORAGE_ACCOUNT` - Terraform state storage account
- `TF_STATE_CONTAINER` - Terraform state container name
- `TF_BACKEND_KEY` - Terraform backend key
- `TF_VAR_SHARED_STATE_KEY` - Shared infrastructure state key
- `TF_VAR_ALLOWED_SSH_IPS` - JSON array of allowed SSH IPs
- `TF_VAR_SSH_PUBLIC_KEY` - SSH public key for VM access

### Database Secrets
- `DB_PASSWORD` - Password for ETL application database user (used to construct DATABASE_URL)
- `POSTGRES_ADMIN_USERNAME` - PostgreSQL server admin username (for creating database user from VM)
- `POSTGRES_ADMIN_PASSWORD` - PostgreSQL server admin password (for creating database user from VM)

### Application Secrets
- `FEC_API_KEY` - Federal Election Commission API key

**Note:** `DATABASE_URL` is automatically constructed from Terraform outputs (host, database name, username) + `DB_PASSWORD` secret. No need to manually configure the full connection string.

## Database Configuration

### Automatic DATABASE_URL Construction

The deployment workflow automatically constructs the `DATABASE_URL` connection string from:

**From Terraform Outputs:**
- `postgres_fqdn` - PostgreSQL server hostname
- `database_name` - Database name (e.g., `fund-lens-production-etl`)
- `database_username` - Database username (e.g., `fund_lens_production_etl`)

**From GitHub Secrets:**
- `DB_PASSWORD` - Database user password

**Constructed Format:**
```
postgresql://{username}:{password}@{host}:5432/{database}
```

**Example:**
```
postgresql://fund_lens_production_etl:SecurePass123@myserver.postgres.database.azure.com:5432/fund-lens-production-etl
```

This approach ensures:
- Single source of truth for infrastructure values (Terraform)
- Only the password is stored as a secret
- Automatic synchronization when infrastructure changes
- No manual connection string maintenance

## Shared Infrastructure Dependencies

This Terraform configuration requires outputs from shared infrastructure:

**Required Outputs:**
- `resource_group_name` - Resource group for all resources
- `vnet_name` - Virtual network for VM networking
- `storage_account_name` - Storage account name
- `storage_account_id` - Storage account resource ID
- `postgres_server_fqdn` - PostgreSQL server hostname (e.g., `servername.postgres.database.azure.com`)
  - Server name is automatically extracted from FQDN
- `postgres_admin_username` - Admin username for creating databases/users

## Prefect Flow Schedules

Once deployed, flows run automatically:

**Daily (Mon-Fri):**
- 2:00 AM ET - Bronze ingestion (incremental)
- 2:30 AM ET - Silver transformation
- 3:30 AM ET - Gold transformation

**Monthly (1st):**
- 3:00 AM ET - Bronze ingestion (full refresh)
- 3:30 AM ET - Silver transformation
- 4:30 AM ET - Gold transformation

## Monitoring

- **GitHub Actions**: View deployment logs in Actions tab
- **Prefect UI**: Self-hosted at `http://<vm-ip>:4200` (access via allowed IPs in NSG)
  - Get VM IP: `cd terraform && terraform output vm_public_ip`
- **Azure Portal**: Check VM health and metrics
- **Service Logs** (SSH to VM):
  - Prefect Server: `sudo journalctl -u prefect-server -f`
  - Prefect Worker: `sudo journalctl -u prefect-worker -f`
  - SSH command: `cd terraform && terraform output -raw ssh_command`

**Last Updated**: 2025-10-31

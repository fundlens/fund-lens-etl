# CI/CD Pipeline

Automated deployment pipeline using GitHub Actions for infrastructure provisioning and Prefect flow deployment.

## Workflow

```
Push to main
    ↓
Terraform Apply (Infrastructure)
    ↓
Deploy Prefect Flows (ETL Pipeline)
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

### Application Secrets
- `FEC_API_KEY` - Federal Election Commission API key
- `DATABASE_URL` - PostgreSQL connection string

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

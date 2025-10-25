variable "shared_state_resource_group_name" {
  type = string
  description = "Resource group for the shared infrastructure Terraform state"
}

variable "shared_state_storage_account_name"  {
  type = string
  description = "Storage account for the shared infrastructure Terraform state"
}

variable "shared_state_container_name" {
  type = string
  description = "Container name for the shared infrastructure Terraform state"
}

variable "shared_state_key" {
  type = string
  description = "Blob name for the shared infrastructure Terraform state"
}

variable "project_name" {
  type = string
  description = "Name of the project"
  default = "fund-lens"
}

variable "environment" {
  type = string
  description = "Name of deployment environment"
  default = "production"
}

variable "allowed_ssh_ips" {
  type = list(string)
  description = "List of allowed IPs for SSH access (CIDR notation)"
}

variable "vm_size" {
  type = string
  description = "Size of Azure VM"
  default = "Standard_B2s"
}

variable "admin_username" {
  type = string
  description = "Name for VM admin user"
  default = "azureuser"
}

variable "ssh_public_key" {
  type = string
  description = "Public key for SSH access"
}

variable "os_disk_storage_type" {
  type = string
  description = "Azure disk storage type for VM"
  default = "Standard_LRS"
}

variable "os_disk_size_gb" {
  type = number
  description = "Disk size in GB of VM"
  default = 30
}

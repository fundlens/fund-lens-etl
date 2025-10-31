output "vm_public_ip" {
  description = "Public IP address of the ETL VM"
  value       = azurerm_public_ip.etl_vm.ip_address
}

output "vm_id" {
  description = "Resource ID of the ETL VM"
  value       = azurerm_linux_virtual_machine.etl_vm.id
}

output "vm_private_ip" {
  description = "Private IP address of the ETL VM"
  value       = azurerm_network_interface.etl_vm.private_ip_address
}

output "managed_identity_principal_id" {
  description = "Principal ID of the VM's managed identity"
  value       = azurerm_user_assigned_identity.etl_vm.principal_id
}

output "managed_identity_client_id" {
  description = "Client ID of the VM's managed identity"
  value       = azurerm_user_assigned_identity.etl_vm.client_id
}

output "ssh_command" {
  description = "SSH command to connect to the VM"
  value       = "ssh ${var.admin_username}@${azurerm_public_ip.etl_vm.ip_address}"
}

output "resource_group_name" {
  description = "Name of the resource group"
  value       = local.resource_group_name
}

output "vm_name" {
  description = "Name of the VM"
  value       = azurerm_linux_virtual_machine.etl_vm.name
}

output "admin_username" {
  description = "Admin username for the VM"
  value       = var.admin_username
}

output "postgres_fqdn" {
  description = "Fully qualified domain name of the PostgreSQL server"
  value       = local.postgres_fqdn
}

output "database_name" {
  description = "Name of the PostgreSQL database"
  value       = azurerm_postgresql_flexible_server_database.etl.name
}

output "database_username" {
  description = "Database username for ETL application"
  value       = local.db_username
}

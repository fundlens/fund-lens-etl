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

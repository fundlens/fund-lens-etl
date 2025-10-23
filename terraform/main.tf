data "terraform_remote_state" "shared" {
  backend = "azurerm"

  config = {
    resource_group_name  = var.shared_state_resource_group_name
    storage_account_name = var.shared_state_storage_account_name
    container_name       = var.shared_state_container_name
    key                  = var.shared_state_key
  }
}

locals {
  # Common tags for all resources
  common_tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
    Component   = "ETL-Pipeline"
  }

  # Naming prefix for resources
  name_prefix = "${var.project_name}-${var.environment}-etl"

  # Referenced shared infrastructure values
  resource_group_name  = data.terraform_remote_state.shared.outputs.resource_group_name
  vnet_name            = data.terraform_remote_state.shared.outputs.vnet_name
  storage_account_name = data.terraform_remote_state.shared.outputs.storage_account_name
  postgres_fqdn        = data.terraform_remote_state.shared.outputs.postgres_server_fqdn
}

data "azurerm_resource_group" "shared" {
  name = local.resource_group_name
}

data "azurerm_subnet" "vm" {
  name                 = "fund-lens-vm-subnet"
  virtual_network_name = local.vnet_name
  resource_group_name  = data.azurerm_resource_group.shared.name
}

locals {
  location  = data.azurerm_resource_group.shared.location
  subnet_id = data.azurerm_subnet.vm.id  # Changed to use data source
}

resource "azurerm_user_assigned_identity" "etl_vm" {
  name                = "${local.name_prefix}-vm-identity"
  resource_group_name = local.resource_group_name
  location            = local.location

  tags = local.common_tags
}

resource "azurerm_role_assignment" "etl_vm_storage" {
  scope                = data.terraform_remote_state.shared.outputs.storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.etl_vm.principal_id
}

resource "azurerm_role_assignment" "etl_vm_storage_account" {
  scope                = data.terraform_remote_state.shared.outputs.storage_account_id
  role_definition_name = "Storage Account Contributor"
  principal_id         = azurerm_user_assigned_identity.etl_vm.principal_id
}

resource "azurerm_role_assignment" "etl_vm_reader" {
  scope                = data.azurerm_resource_group.shared.id
  role_definition_name = "Reader"
  principal_id         = azurerm_user_assigned_identity.etl_vm.principal_id
}

resource "azurerm_public_ip" "etl_vm" {
  name                = "${local.name_prefix}-vm-pip"
  resource_group_name = local.resource_group_name
  location            = local.location
  allocation_method   = "Static"
  sku                 = "Standard"

  tags = local.common_tags
}

resource "azurerm_network_security_group" "etl_vm" {
  name                = "${local.name_prefix}-vm-nsg"
  resource_group_name = local.resource_group_name
  location            = local.location

  tags = local.common_tags
}

resource "azurerm_network_security_rule" "ssh" {
  name                        = "AllowSSH"
  priority                    = 100
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "Tcp"
  source_port_range           = "*"
  destination_port_range      = "22"
  source_address_prefixes     = var.allowed_ssh_ips
  destination_address_prefix  = "*"
  resource_group_name         = local.resource_group_name
  network_security_group_name = azurerm_network_security_group.etl_vm.name
}

resource "azurerm_network_security_rule" "prefect_ui" {
  name                        = "AllowPrefectUI"
  priority                    = 110
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "Tcp"
  source_port_range           = "*"
  destination_port_range      = "4200"
  source_address_prefixes     = var.allowed_ssh_ips
  destination_address_prefix  = "*"
  resource_group_name         = local.resource_group_name
  network_security_group_name = azurerm_network_security_group.etl_vm.name
}

resource "azurerm_network_security_rule" "outbound_internet" {
  name                        = "AllowInternetOutbound"
  priority                    = 100
  direction                   = "Outbound"
  access                      = "Allow"
  protocol                    = "*"
  source_port_range           = "*"
  destination_port_range      = "*"
  source_address_prefix       = "*"
  destination_address_prefix  = "Internet"
  resource_group_name         = local.resource_group_name
  network_security_group_name = azurerm_network_security_group.etl_vm.name
}

resource "azurerm_network_security_rule" "vnet_inbound" {
  name                        = "AllowVNetInbound"
  priority                    = 200
  direction                   = "Inbound"
  access                      = "Allow"
  protocol                    = "*"
  source_port_range           = "*"
  destination_port_range      = "*"
  source_address_prefix       = "VirtualNetwork"
  destination_address_prefix  = "VirtualNetwork"
  resource_group_name         = local.resource_group_name
  network_security_group_name = azurerm_network_security_group.etl_vm.name
}

resource "azurerm_network_interface" "etl_vm" {
  name                = "${local.name_prefix}-vm-nic"
  resource_group_name = local.resource_group_name
  location            = local.location

  ip_configuration {
    name                          = "internal"
    subnet_id                     = local.subnet_id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.etl_vm.id
  }

  tags = local.common_tags
}

resource "azurerm_network_interface_security_group_association" "etl_vm" {
  network_interface_id      = azurerm_network_interface.etl_vm.id
  network_security_group_id = azurerm_network_security_group.etl_vm.id
}

resource "azurerm_linux_virtual_machine" "etl_vm" {
  name                = "${local.name_prefix}-vm"
  resource_group_name = local.resource_group_name
  location            = local.location
  size                = var.vm_size
  admin_username      = var.admin_username

  network_interface_ids = [
    azurerm_network_interface.etl_vm.id
  ]

  admin_ssh_key {
    username   = var.admin_username
    public_key = var.ssh_public_key
  }

  os_disk {
    name                 = "${local.name_prefix}-vm-osdisk"
    caching              = "ReadWrite"
    storage_account_type = var.os_disk_storage_type
    disk_size_gb         = var.os_disk_size_gb
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  identity {
    type = "UserAssigned"
    identity_ids = [
      azurerm_user_assigned_identity.etl_vm.id
    ]
  }

  tags = local.common_tags
}
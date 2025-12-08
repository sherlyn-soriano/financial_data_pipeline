import pulumi
import pulumi_azure_native as azure_native
import pulumi_azuread as azuread

def create_key_vault(
    name: pulumi.Input[str],
    resource_group_name: pulumi.Input[str],
    location: pulumi.Input[str],
) -> azure_native.keyvault.Vault:
    
    current = azuread.get_client_config()
    key_vault = azure_native.keyvault.Vault(
        "key-vault-resource",
        vault_name=name,
        resource_group_name=resource_group_name,
        location=location,
        properties=azure_native.keyvault.VaultPropertiesArgs(
            tenant_id=current.tenant_id,
            sku=azure_native.keyvault.SkuArgs(
                family=azure_native.keyvault.SkuFamily.A,
                name=azure_native.keyvault.SkuName.STANDARD,
            ),
            access_policies=[
                azure_native.keyvault.AccessPolicyEntryArgs(
                    tenant_id=current.tenant_id,
                    object_id=current.object_id,
                    permissions=azure_native.keyvault.PermissionsArgs(
                        keys=["get", "list", "create", "delete", "update"],
                        secrets=["get", "list", "set", "delete"],
                        certificates=["get", "list", "create", "delete"],
                    ),
                )
            ],
            enabled_for_deployment=True,
            enabled_for_disk_encryption=True,
            enabled_for_template_deployment=True,
            enable_soft_delete=True,
            soft_delete_retention_in_days=90,
            enable_purge_protection=False,
        ),
        tags={
            "Purpose":"SecretManagement",
            "Layer":"Security"
        }
    )

    return key_vault

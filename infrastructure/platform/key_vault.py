import pulumi
import pulumi_azure_native as azure_native
import pulumi_azuread as azuread

def create_key_vault(name: str,resource_group_name: str, location: str):
    current = azuread.get_client_config()
    key_vault = azure_native.keyvault.Vault(
        name,
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
                        keys=["all"],
                        secrets=["all"],
                        certificates=["all"],
                    ),
                )
            ],
            enabled_for_disk_encryption=True,
            enabled_for_template_deployment=True,
            enable_soft_delete=True,
            soft_delete_retention_in_days=7,
        ),
        tags={
            "Purpose":"SecretManagement",
            "Layer":"Security"
        }
    )

    pulumi.log.info(f"Key Vault created: {name}")
    return key_vault
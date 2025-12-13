import pulumi
from platform.resource_group import create_resource_group
from platform.storage import create_data_lake
from platform.key_vault import create_key_vault
from platform.databricks_ws import create_databricks_workspace
from platform.data_factory import create_data_factory

config = pulumi.Config()
project_name = config.get("project_name") or "finde"
environment = config.get("environment") or "dev"
location = config.get("location") or "eastus2"

def get_resource_name(resource_type: str) -> str:
    return f"{resource_type}-{project_name}-{environment}"

resource_group = create_resource_group(
    name=get_resource_name("rg"),
    location=location,
    tags={
        "Environment":environment,
        "Project":project_name,
        "ManagedBy":"Pulumi",
        "Owner":"DataEngineering"
    }
)

storage_name = f"st{project_name.replace('-','')}{environment}"
storage_name = storage_name[:24].lower()

storage_account = create_data_lake(
    name=storage_name,
    resource_group_name=resource_group.name,
    location=location,
    tags= {
        "Environment":environment,
        "Project": project_name,
        "ManagedBy":"Pulumi",
        "Owner":"DataEngineering"
    }
)


key_vault = create_key_vault(
    name=get_resource_name("kv"),
    resource_group_name=resource_group.name,
    location=location,
     tags={
        "Environment": environment,
        "Project": project_name,
        "ManagedBy": "Pulumi",
        "Owner": "DataEngineering"
    }
)

databricks_workspace = create_databricks_workspace(
    name=get_resource_name("dbw"),
    resource_group_name=resource_group.name,
    location=location,
    sku="Standard",
    tags={
        "Environment": environment,
        "Project": project_name,
        "ManagedBy": "Pulumi",
        "Owner": "DataEngineering"}
)

data_factory = create_data_factory(
    name=get_resource_name("adf"),
    resource_group_name=resource_group.name,
    location=location,
    tags={
        "Environment": environment,
        "Project": project_name,
        "ManagedBy": "Pulumi",
        "Owner": "DataEngineering"}
)

pulumi.export("resource_group_name", resource_group.name)
pulumi.export("storage_account_name", storage_account.name)
pulumi.export("key_vault_name", key_vault.name)
pulumi.export("key_vault_uri", key_vault.properties.vault_uri)
pulumi.export("databricks_workspace_url", databricks_workspace.workspace_url)
pulumi.export("data_factory_name", data_factory.name)
pulumi.export("data_factory_id",data_factory.id)

pulumi.export("deployment_summary", pulumi.Output.all(
    resource_group.name,
    storage_account.name,
    key_vault.name,
    databricks_workspace.workspace_url,
    data_factory.name
).apply(lambda args: f"""

Resource Group:    {args[0]}
Storage Account:   {args[1]}
Key Vault:         {args[2]}
Databricks:        {args[3]}
Data Factory:      {args[4]}

"""))

#Next Steps:
#1. Run: pulumi stack output storage_account_key
#2. Upload data: python scripts/upload_to_datalake.py
#3. Deploy notebooks: cd databricks && ./deploy.sh

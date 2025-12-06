import pulumi
import pulumi_azure_native as azure_native

# check if standard starts capital C 
def create_databricks_workspace(name: str, resource_group_name: str, location: str, sku: str = "Standard"):
    workspace = azure_native.databricks.Workspace(
        name,
        workspace_name=name,
        resource_group_name=resource_group_name,
        location=location,
        sku=azure_native.databricks.SkuArgs(name=sku),
        parameters=azure_native.databricks.WorkspaceCustomParametersArgs(
            enable_no_public_ip=azure_native.databricks.WorkspaceNoPublicIPBooleanParameterArgs(value=False),
        ),
        tags={
            "Purpose": "DataProcessing",
            "Layer": "Compute",
        },
    )

    pulumi.log.info(f"Databricks Workspace created: {name}")
    return workspace

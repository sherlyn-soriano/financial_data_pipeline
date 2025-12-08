import pulumi
import pulumi_azure_native as azure_native

# check if standard starts capital C 
def create_databricks_workspace(
    name: pulumi.Input[str],
    resource_group_name: pulumi.Input[str],
    location: pulumi.Input[str],
    sku: str = "Standard",
) -> azure_native.databricks.Workspace:
    
    workspace = azure_native.databricks.Workspace(
        "databricks",
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

    return workspace

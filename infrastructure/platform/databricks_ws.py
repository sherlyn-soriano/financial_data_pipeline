import pulumi
import pulumi_azure_native as azure_native
from typing import Optional

def create_databricks_workspace(
    name: pulumi.Input[str],
    resource_group_name: pulumi.Input[str],
    location: pulumi.Input[str],
    sku: str = "Standard",
    tags: Optional[dict] = None
) -> azure_native.databricks.Workspace:
    
    db_tags = {
        "Purpose": "DataProcessing",
        "Layer": "Compute",
    }
    if tags:
        db_tags.update(tags)
    
    workspace = azure_native.databricks.Workspace(
        "databricks",
        workspace_name=name,
        resource_group_name=resource_group_name,
        location=location,
        sku=azure_native.databricks.SkuArgs(name=sku),
        parameters=azure_native.databricks.WorkspaceCustomParametersArgs(
            enable_no_public_ip=azure_native.databricks.WorkspaceNoPublicIPBooleanParameterArgs(value=False),
        ),
        tags=db_tags,
    )

    return workspace

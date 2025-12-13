import pulumi 
import pulumi_azure_native as azure_native
from typing import Optional

def create_data_factory(
    name: pulumi.Input[str],
    resource_group_name: pulumi.Input[str],
    location: pulumi.Input[str],
    tags: Optional[dict] = None
) -> azure_native.datafactory.Factory:
    
    df_tag = {
        "Purpose":"Orchestration",
        "Layer":"Integration"
    }
    
    if tags:
        df_tag.update(tags)

    factory = azure_native.datafactory.Factory(
        "data-factory",
        factory_name=name,
        resource_group_name=resource_group_name,
        location=location,
        identity=azure_native.datafactory.FactoryIdentityArgs(
            type=azure_native.datafactory.FactoryIdentityType.SYSTEM_ASSIGNED
        ),
        tags=df_tag
    )

    return factory

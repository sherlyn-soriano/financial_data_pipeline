import pulumi 
import pulumi_azure_native as azure_native

def create_data_factory(
    name: pulumi.Input[str],
    resource_group_name: pulumi.Input[str],
    location: pulumi.Input[str],
) -> azure_native.datafactory.Factory:
    
    factory = azure_native.datafactory.Factory(
        "data-factory",
        factory_name=name,
        resource_group_name=resource_group_name,
        location=location,
        identity=azure_native.datafactory.FactoryIdentityArgs(
            type=azure_native.datafactory.FactoryIdentityType.SYSTEM_ASSIGNED
        ),
        tags={
            "Purpose":"Orchestration",
            "Layer":"Integration"
        }

    )

    return factory

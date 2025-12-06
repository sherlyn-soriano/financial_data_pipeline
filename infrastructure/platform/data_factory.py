import pulumi 
import pulumi_azure_native as azure_native

def create_data_factory(name: str, resource_group_name: str, location:str):
    factory = azure_native.datafactory.Factory(
        name,
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
    pulumi.log.info(f"Data Factory created: {name}")
    return factory
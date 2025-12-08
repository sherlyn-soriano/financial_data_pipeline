import pulumi
import pulumi_azure_native as azure_native

def create_resource_group(name: str, location: str, tags: dict) -> azure_native.resources.ResourceGroup:
    
    rg = azure_native.resources.ResourceGroup(
        name,
        resource_group_name = name,
        location=location,
        tags=tags
    )
    
    return rg


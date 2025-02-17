import requests

# Base URL for the Open Targets API
base_url = "https://api.platform.opentargets.org/api/v4"

# Endpoint for the GraphQL API
graphql_endpoint = f"{base_url}/graphql"

# Query to inspect the schema
query = {
    "query": """
    {
      __schema {
        types {
          name
          fields {
            name
          }
        }
      }
    }
    """
}

# Send a POST request to the API
response = requests.post(graphql_endpoint, json=query)

# Check if the request was successful
if response.status_code == 200:
    data = response.json()
    schema = data.get("data", {}).get("__schema", {}).get("types", [])
    for type_ in schema:
        print(f"Type: {type_['name']}")
        if type_["fields"]:
            print("  Fields:")
            for field in type_["fields"]:
                print(f"    {field['name']}")
else:
    print(f"Failed to query schema. Status code: {response.status_code}, Error: {response.text}")

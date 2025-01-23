import requests
import pandas as pd  # Import pandas for DataFrame handling

# Replace with your API endpoint and key
url = "https://orchestrator.pgatour.com/graphql"
X_API_KEY = "da2-gsrx5bibzbb4njvhl7t37wqyl4"


def introspect_official_players():
    query = """
    {
      __type(name: "TourCupCombinedPlayer") {
        name
        fields {
          name
          description
          type {
            name
            kind
            ofType {
              name
              kind
            }
          }
        }
      }
    }
    """
    response = requests.post(
        url,
        json={"query": query},
        headers={"x-api-key": X_API_KEY}
    )

    if response.status_code == 200:
        schema_data = response.json()
        fields = schema_data.get("data", {}).get("__type", {}).get("fields", [])
        
        rows = []
        for field in fields:
            rows.append({
                "Field Name": field["name"],
                "Description": field.get("description", ""),
                "Type Name": field["type"]["name"],
                "Type Kind": field["type"]["kind"],
            })
        
        df = pd.DataFrame(rows)
        print(df)
        return df
    else:
        print(f"Failed to fetch schema: {response.status_code}")
        print(response.text)
        return None

# Run the introspection
if __name__ == "__main__":
    introspect_official_players()

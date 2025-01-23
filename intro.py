import requests
import pandas as pd  # Import pandas for DataFrame handling

# Replace with your API endpoint and key
url = "https://orchestrator.pgatour.com/graphql"
X_API_KEY = 

# Introspection query for TourCupSplit
introspection_query = """
{
  __type(name: "TourCupSplit") {
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

def introspect_tour_cup_split():
    response = requests.post(
        url,
        json={"query": introspection_query},
        headers={"x-api-key": X_API_KEY}
    )

    if response.status_code == 200:
        schema_data = response.json()
        fields = schema_data.get("data", {}).get("__type", {}).get("fields", [])
        
        # Format and print the fields
        rows = []
        for field in fields:
            rows.append({
                "Field Name": field["name"],
                "Description": field.get("description", ""),
                "Type Name": field["type"]["name"],
                "Type Kind": field["type"]["kind"],
            })
        
        # Convert to a DataFrame for easier viewing
        df = pd.DataFrame(rows)
        print(df)
        return df
    else:
        print(f"Failed to fetch schema: {response.status_code}")
        print(response.text)
        return None

# Run the introspection
if __name__ == "__main__":
    introspect_tour_cup_split()

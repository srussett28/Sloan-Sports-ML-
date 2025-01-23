import requests
import pandas as pd

url = "https://orchestrator.pgatour.com/graphql"
X_API_KEY = "da2-gsrx5bibzbb4njvhl7t37wqyl4"


def introspect_official_players_type():
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
        print("Raw Response:", schema_data)  # Debug raw API response
        fields = schema_data.get("data", {}).get("__type", {}).get("fields", [])
        if not fields:
            print("No fields found in 'TourCupCombinedPlayer'.")
            return None

        rows = []
        for field in fields:
            field_type = field.get("type", {})
            of_type = field_type.get("ofType")  # This could be None

            rows.append({
                "Field Name": field.get("name"),
                "Type Name": field_type.get("name"),
                "Type Kind": field_type.get("kind"),
                "OfType Name": of_type["name"] if of_type else None,
                "OfType Kind": of_type["kind"] if of_type else None,
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
    introspect_official_players_type()


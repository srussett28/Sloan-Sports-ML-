import pandas as pd
import psycopg2
import requests
from datetime import datetime

# Define the function to fetch data
X_API_KEY = 

def get_tournament_past_results(tournament_id, year=None, api_key="your_api_key"):
    """
    Fetch past results for a given tournament from the PGA Tour API.
    """
    url = "https://orchestrator.pgatour.com/graphql"
    query = {
        "operationName": "TournamentPastResults",
        "variables": {
            "tournamentPastResultsId": tournament_id,
            "year": year
        },
        "query": """query TournamentPastResults($tournamentPastResultsId: ID!, $year: Int) {
            tournamentPastResults(id: $tournamentPastResultsId, year: $year) {
                players {
                    id
                    position
                    total
                    parRelativeScore
                    rounds { score }
                    player { id firstName lastName country }
                }
            }
        }"""
    }

    # Send the POST request
    response = requests.post(
        url,
        json=query,
        headers={"x-api-key": api_key}
    )

    if response.status_code != 200:
        raise Exception(f"API request failed with status code {response.status_code}")

    # Parse and return the data
    data = response.json()["data"]["tournamentPastResults"]["players"]
    results = []
    for player in data:
        rounds = [round.get("score", None) for round in player.get("rounds", [])]
        results.append({
            "Player ID": player["player"]["id"],
            "First Name": player["player"]["firstName"],
            "Last Name": player["player"]["lastName"],
            "Country": player["player"]["country"],
            "Position": player["position"],
            "Total Score": player["total"],
            "Par Relative Score": player["parRelativeScore"],
            "Round Scores": rounds
        })
    return pd.DataFrame(results)

# Define the database connection
def connect_to_db():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",  # Replace with your actual password
        host="localhost",
        port="5432"
    )

# Insert data into PostgreSQL
def insert_into_db(df, conn):
    cursor = conn.cursor()
    for _, row in df.iterrows():
        print(row)  # Debugging step to identify problematic data
        try:
            cursor.execute("""
                INSERT INTO tournament_results (
                    player_id, first_name, last_name, country, position,
                    total_score, par_relative_score, round_scores, year, tournament_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id, tournament_id, year) DO NOTHING
            """, (
                row["Player ID"], row["First Name"], row["Last Name"],
                row["Country"], row["Position"], row["Total Score"],
                row["Par Relative Score"], str(row["Round Scores"]),
                row["Year"], row["Tournament ID"]
            ))
        except Exception as e:
            print(f"Error inserting row: {row}")
            print(f"Exception: {e}")
    conn.commit()
    cursor.close()

# Profile the data
def profile_data(df):
    print("Data Profiling Report:")
    print("Shape of the DataFrame:", df.shape)
    print("Missing Values:\n", df.isnull().sum())
    print("Data Types:\n", df.dtypes)
    print("Sample Data:\n", df.head())

def main():
    tournament_ids = {
        "R2024016": "The Sentry Tournament Of Champions",
        "R2024006": "Sony Open in Hawaii",
     
    }
    years = range(2007, 2025)  # Historical years
    all_data = []

  
    for tournament_id in tournament_ids.keys():
        for year in years:
            try:
               
                adjusted_year = int(f"{year}0")
                print(f"Fetching data for {tournament_id} in adjusted year {adjusted_year}...")
                
                df = get_tournament_past_results(tournament_id, adjusted_year)
                df["Year"] = year  
                df["Tournament ID"] = tournament_id
                all_data.append(df)
            except Exception as e:
                print(f"Error fetching data for {tournament_id} in year {year}: {e}")

   
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
    else:
        print("No data fetched. Exiting.")
        return

   
    profile_data(combined_data)

    
    combined_data["Round Scores"] = combined_data["Round Scores"].apply(lambda x: str(x) if isinstance(x, list) else x)

 
    combined_data["Position"] = combined_data["Position"].str.replace(r"[^\d]", "", regex=True)
    combined_data["Position"] = pd.to_numeric(combined_data["Position"], errors="coerce")  
    combined_data["Position"] = combined_data["Position"].fillna(-1).astype(int) 


    combined_data["Par Relative Score"] = combined_data["Par Relative Score"].replace({"E": 0})
    combined_data["Par Relative Score"] = pd.to_numeric(combined_data["Par Relative Score"], errors="coerce")
    combined_data["Par Relative Score"] = combined_data["Par Relative Score"].fillna(0).clip(lower=-2147483648, upper=2147483647)

    # Clean Total Score column
    combined_data["Total Score"] = combined_data["Total Score"].replace("-", 0)  # Replace '-' with 0
    combined_data["Total Score"] = pd.to_numeric(combined_data["Total Score"], errors="coerce").fillna(0).astype(int)
    combined_data["Total Score"] = combined_data["Total Score"].clip(lower=0, upper=2147483647)

    # Ensure Position is within valid range
    combined_data["Position"] = combined_data["Position"].clip(lower=0, upper=2147483647)
   
    conn = connect_to_db()
    insert_into_db(combined_data, conn)
    conn.close()

    print("Data pipeline completed successfully!")

if __name__ == "__main__":
    main()


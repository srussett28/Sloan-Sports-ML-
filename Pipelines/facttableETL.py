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
        dbname="",
        user="",
        password="",
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
    "R2024002": "The American Express",
    "R2024004": "Farmers Insurance Open",
    "R2024005": "ATT Pebble Beach ProAm",
    "R2024003": "WM Phoenix Open",
    "R2024007": "The Genesis Invitational",
    "R2024540": "Mexico Open at Vidante",
    "R2024010": "Cognizant Classic In the Palm Beaches",
    "R2024009": "Arnold Palmer Invitational",
    "R2024483": "Puerto Rico Open",
    "R2024011": "The Players Championship",
    "R2024475": "Valspar Championship",
    "R2024020": "Texas Childrens Houston Open",
    "R2024041": "Valero Texas Open",
    "R2024014": "The Masters",
    "R2024012": "RBC Heritage",
    "R2024522": "Corales Puntacana Championship",
    "R2024018": "Zurich Classic of New Orleans",
    "R2024019": "The CJ Cup Byron Nelson",
    "R2024480": "Wells Fargo Championship",
    "R2024553": "Myrtle Beach Classic",
    "R2024033": "PGA Championship",
    "R2024021": "Charles Schwab Challenge",
    "R2024032": "RBC Canadian Open",
    "R2024023": "The Memorial Tournament",
    "R2024026": "The US Open",
    "R2024034": "Travelers Championship",
    "R2024523": "Rocket Mortgage Classic",
    "R2024030": "The John Deere Classic",
    "R2024541": "Genesis Scottish Open",
    "R2024518": "ISCO Championship",
    "R2024100": "The Open Championship",
    "R2024472": "Barracuda Championship",
    "R2024525": "3M Open",
    "R2024519": "Mens Olympic Golf Competition",
    "R2024013": "Wyndham Championship",
    "R2024027": "FedEx St Jude Championship",
    "R2024028": "BMW Championship",
    "R2024060": "Tour Championship",
    "R2024464": "Procore Championship",
    "R2024527": "The Presidents Cup",
    "R2023468": "The Ryder Cup",
    "R2024054": "Sanderson Farms Championship",
    "R2024554": "Black Desert Championship",
    "R2024047": "Shriners Childrens Open",
    "R2024527": "Zozo Championship",
    "R2024457": "World Wide Technology Championship",
    "R2024528": "Butterfield Bermuda Championship",
    "R2024493": "The RSM Classic",
    "R2024478": "Hero World Challenge",
    "R2024551": "Grant Thornton Invitiational"    
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


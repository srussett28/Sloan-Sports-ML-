import pandas as pd
import requests
import psycopg2


def get_df(YEAR, STAT_ID, DESCR):
    X_API_KEY = 
    url = "https://orchestrator.pgatour.com/graphql"
    req = {
        "operationName": "StatDetails",
        "variables": {"tourCode": "R", "statId": STAT_ID, "year": YEAR, "eventQuery": None},
        "query": """query StatDetails($tourCode: TourCode!, $statId: String!, $year: Int) {
          statDetails(tourCode: $tourCode, statId: $statId, year: $year) {
            rows {
              ... on StatDetailsPlayer {
                playerId
                playerName
                stats { statValue }
              }
            }
          }
        }""",
    }

    response = requests.post(url, json=req, headers={"x-api-key": X_API_KEY})
    try:
        data = response.json()["data"]["statDetails"]["rows"]
    except KeyError:
        return pd.DataFrame()  # Return an empty DataFrame in case of error

    table = [{
        "PID": item.get("playerId", None),
        "PLAYER": item.get("playerName", "Unknown"),
        DESCR: item["stats"][0]["statValue"] if item.get("stats") else None,
    } for item in data]

    return pd.DataFrame(table)


def get_players():
    X_API_KEY = 
    url = "https://orchestrator.pgatour.com/graphql"
    req = {
        "operationName": "PlayerDirectory",
        "variables": {"tourCode": "R"},
        "query": """query PlayerDirectory($tourCode: TourCode!) {
          playerDirectory(tourCode: $tourCode) {
            players { id displayName country }
          }
        }""",
    }

    response = requests.post(url, json=req, headers={"x-api-key": X_API_KEY})
    try:
        data = response.json()["data"]["playerDirectory"]["players"]
    except KeyError:
        return pd.DataFrame()

    table = [{"PID": item["id"], "PLAYER": item["displayName"], "Country": item["country"]} for item in data]
    return pd.DataFrame(table)


Overview_Stats = {
    "120": "scoring_avg_adjusted",
    "156": "birdie_avg",
    "02675": "sg_total",
    "101": "driving_distance",
    "02568": "sg_approach_green",
    "103": "gir_percent",
    "130": "scrambling",
    "02564": "sg_putting",
}


def pipeline():
    years = list(range(2007, 2025))
    overview_dfs = []

    print("📥 Fetching player data...")
    players = get_players()

    for year in years:
        print(f"📊 Processing data for year: {year}...")
        overview_df = pd.DataFrame()

        for stat_id, stat_name in Overview_Stats.items():
            print(f"🔄 Fetching {stat_name} for {year}...")
            if overview_df.empty:
                overview_df = get_df(year, stat_id, stat_name)
            else:
                curr_stat_df = get_df(year, stat_id, stat_name)
                overview_df = pd.merge(overview_df, curr_stat_df, on=["PID", "PLAYER"], how="outer")

        overview_df["Year"] = year

       
        overview_df = pd.merge(overview_df, players, on=["PID", "PLAYER"], how="left")

        
        stat_columns = list(Overview_Stats.values())
        overview_df = overview_df.dropna(subset=stat_columns, how="all")

        overview_dfs.append(overview_df)

    
    overview_df = pd.concat(overview_dfs, ignore_index=True)

   
    column_mapping = {
        "PID": "pid",
        "PLAYER": "player_name",
        "ScoringAvgAdjusted": "scoring_avg_adjusted",
        "BirdieAvg": "birdie_avg",
        "SG_Total": "sg_total",
        "DrivingDistance": "driving_distance",
        "SG_ApproachGreen": "sg_approach_green",
        "GIR%": "gir_percent",
        "Scrambling": "scrambling",
        "SG_Putting": "sg_putting",
        "Year": "year",
    }
    
    overview_df = overview_df.rename(columns=column_mapping)
    
  
    overview_df = overview_df.drop(columns=["Country"], errors="ignore")

  
    float_columns = ["scoring_avg_adjusted", "birdie_avg", "sg_total", "driving_distance", "sg_approach_green", "gir_percent", "scrambling", "sg_putting"]
    for col in float_columns:
        overview_df[col] = pd.to_numeric(overview_df[col], errors="coerce").fillna(0)

    overview_df["pid"] = pd.to_numeric(overview_df["pid"], errors="coerce").fillna(0).astype(int)
    overview_df["year"] = pd.to_numeric(overview_df["year"], errors="coerce").fillna(0).astype(int)

    print("✅ Data cleaned and processed successfully!")
    
    
    overview_df.to_excel("cleaned_overview_data.xlsx", index=False)
    print("📤 Data exported to cleaned_overview_data.xlsx!")

    return overview_df


def insert_into_database(data, connection_params):
    """
    Inserts cleaned data into the PostgreSQL overview_table.
    """

    expected_columns = [
        "pid", "player_name", "year", "scoring_avg_adjusted", "birdie_avg", "sg_total",
        "driving_distance", "sg_approach_green", "gir_percent", "scrambling", "sg_putting"
    ]

  
    data = data[expected_columns]

   
    data_to_insert = [tuple(row) for row in data.itertuples(index=False, name=None)]

    query = """
        INSERT INTO overview_table (
            pid, player_name, year, scoring_avg_adjusted, birdie_avg, sg_total,
            driving_distance, sg_approach_green, gir_percent, scrambling, sg_putting
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pid, year) DO NOTHING;
    """

    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        cursor.executemany(query, data_to_insert)
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Data successfully inserted into PostgreSQL!")
    except Exception as e:
        print(f"❌ Error inserting data: {e}")


connection_params = {
    "dbname": "postgres",
    "user": "",
    "password": "",  
    "host": "localhost",
    "port": 5432,
}


if __name__ == "__main__":
    cleaned_df = pipeline()
    insert_into_database(cleaned_df, connection_params)
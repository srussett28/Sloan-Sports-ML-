import pandas as pd
import requests
import psycopg2

# Function to query data from PGA Tour API
def get_df(YEAR, STAT_ID, DESCR):
    X_API_KEY = "da2-gsrx5bibzbb4njvhl7t37wqyl4"
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

# Function to get player data
def get_players():
    X_API_KEY = "da2-gsrx5bibzbb4njvhl7t37wqyl4"
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

# Function to clean data
def clean_data(df):
    # Drop rows with more than 4 missing values
    df = df.dropna(thresh=len(df.columns) - 4)

    # Convert numeric columns to proper types
    float_columns = [col for col in df.columns if col not in ["PID", "PLAYER", "Year"]]
    for col in float_columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows where PID is missing
    df = df[df["PID"].notna()]

    return df

def insert_into_database(data, connection_params):
    query = """
        INSERT INTO strokes_gained_table (
            pid, player_name, year, 
            sg_off_tee, sg_around_green, sg_tee_to_green, sg_approach_the_green
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (pid, year) DO NOTHING;
    """
    try:
        # Convert DataFrame to list of tuples
        data_to_insert = [tuple(row) for row in data.itertuples(index=False, name=None)]
        
        print("\n🔍 Sample Data to Insert:")
        print(data_to_insert[:5])  # Print first 5 rows for verification
        
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        cursor.executemany(query, data_to_insert)
        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Data inserted successfully.")
    except Exception as e:
        print(f"❌ Error inserting data: {e}")

# Pipeline function
def pipeline():
    strokes_gained_stats = {
        "02567": "SG_OffTee",
        "02569": "SG_AroundGreen",
        "02674": "SG_TeeToGreen",
        "02568": "SG_ApproachGreen",
    }

    years = list(range(2007, 2025))
    strokes_gained_dfs = []

    print("📥 Fetching player data...")
    players = get_players()

    for year in years:
        print(f"📊 Processing data for year: {year}...")
        strokes_gained_df = pd.DataFrame()

        for stat_id, stat_name in strokes_gained_stats.items():
            print(f"🔄 Fetching {stat_name} for {year}...")
            curr_stat_df = get_df(year, stat_id, stat_name)
            if strokes_gained_df.empty:
                strokes_gained_df = curr_stat_df
            else:
                strokes_gained_df = pd.merge(strokes_gained_df, curr_stat_df, on=["PID", "PLAYER"], how="outer")

        strokes_gained_df["Year"] = year

        # Merge player data
        strokes_gained_df = pd.merge(strokes_gained_df, players, on=["PID", "PLAYER"], how="left")

        # Drop rows with all stats missing
        strokes_gained_df = strokes_gained_df.dropna(subset=list(strokes_gained_stats.values()), how="all")

        strokes_gained_dfs.append(strokes_gained_df)

    # Combine all years into a single DataFrame
    final_df = pd.concat(strokes_gained_dfs, ignore_index=True)

    # Clean the data
    final_df = clean_data(final_df)

    # Rename columns for the database
    column_mapping = {
        "PID": "pid",
        "PLAYER": "player_name",
        "Year": "year",
        "SG_OffTee": "sg_off_tee",
        "SG_AroundGreen": "sg_around_green",
        "SG_TeeToGreen": "sg_tee_to_green",
        "SG_ApproachGreen": "sg_approach_the_green",  # ✅ FIXED COLUMN NAME
    }
    final_df = final_df.rename(columns=column_mapping)

    final_df = final_df[[
        "pid", "player_name", "year", 
        "sg_off_tee", "sg_around_green", "sg_tee_to_green", "sg_approach_the_green"
    ]]

    print("\n🔍 Final DataFrame Preview Before Inserting:")
    print(final_df.head())

    print("\n✅ Data cleaned and ready for insertion!")

    # **PostgreSQL connection details**
    connection_params = {
        "dbname": "postgres",
        "user": "postgres",
        "password": "postgres",
        "host": "localhost",
        "port": 5432,
    }

    # **Insert into database**
    insert_into_database(final_df, connection_params)

    print("\n🚀 Pipeline completed successfully!")

if __name__ == "__main__":
    pipeline()
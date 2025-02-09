import requests
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

off_the_tee_stats = {
    "129": "total_driving",
    "158": "ball_striking",
    "159": "longest_drives",
    "496": "driving_320_plus",
    "495": "driving_300_320",
    "454": "driving_300_plus",
    "455": "driving_280_300",
    "456": "driving_260_280",
    "457": "driving_240_260",
    "218": "driving_240_or_less",
    "102": "driving_accuracy",
    "02435": "rough_tendency",
    "460": "right_rough_tendency",
    "459": "left_rough_tendency",
    "080": "rrt_score",
    "081": "lrt_score",
    "01008": "fairway_bunker_tendency",
    "02401": "clubhead_speed",
    "02402": "ball_speed",
    "02403": "smash_factor",
    "02404": "launch_angle",
    "02405": "spin_rate",
    "02406": "distance_apex",
    "02407": "apex_height",
    "02408": "hang_time",
    "02409": "carry_distance",
    "02410": "carry_efficiency",
    "02411": "total_distance_efficiency",
    "02412": "total_driving_efficiency",
    "02341": "percent_yards_tee_shot",         
    "02342": "percent_yards_tee_shot_par4",
    "02343": "percent_yards_tee_shot_par5",
    "461": "miss_fairway_other",
    "213": "hit_fairway",
    "02420": "dist_fairway_edge",
    "02421": "dist_fairway_center",
    "02422": "left_tendency",
    "02423": "right_tendency",
    "02438": "good_drive"
}
def connect_to_db():
    return psycopg2.connect(
        dbname="",
        user="",
        password="",
        host="localhost",
        port="5432"
    )

def convert_feet_inches(value):
    if isinstance(value, str):
        value = value.strip()
        if "'" in value:  # Example: "5'10"
            parts = value.split("'")
            feet = int(parts[0].strip()) if parts[0].strip().isdigit() else 0
            inches = int(parts[1].replace('"', '').strip()) if len(parts) > 1 and parts[1].replace('"', '').strip().isdigit() else 0
            return round(feet + (inches / 12), 2)  # Convert to decimal feet
        elif " " in value:  # Example: "95 6" (incorrect format)
            parts = value.split()
            feet = int(parts[0].strip()) if parts[0].strip().isdigit() else 0
            inches = int(parts[1].strip()) if len(parts) > 1 and parts[1].strip().isdigit() else 0
            return round(feet + (inches / 12), 2)
        else:
            try:
                return float(value.replace('"', '').strip())
            except ValueError:
                return np.nan  # Handle non-numeric values
    return value  # Return as is if not a string


def get_df(year, stat_id, descr):
    url = "https://orchestrator.pgatour.com/graphql"
    req = {
        "operationName": "StatDetails",
        "variables": {"tourCode": "R", "statId": stat_id, "year": year, "eventQuery": None},
        "query": """query StatDetails($tourCode: TourCode!, $statId: String!, $year: Int) {
            statDetails(tourCode: $tourCode, statId: $statId, year: $year) {
                rows {
                    ... on StatDetailsPlayer {
                        playerId
                        playerName
                        stats {
                            statName
                            statValue
                        }
                    }
                }
            }
        }"""
    }
    response = requests.post(url, json=req, headers={"x-api-key": "da2-gsrx5bibzbb4njvhl7t37wqyl4"})
    
    if response.status_code != 200:
        print(f"❌ API ERROR {response.status_code} for {descr} ({stat_id}) in {year}")
        return pd.DataFrame(columns=["PID", "PLAYER", descr])  # Ensure columns exist

    data = response.json().get("data", {}).get("statDetails", {}).get("rows", [])

    if not data:
        print(f"⚠️ No data for {descr} ({stat_id}) in {year}")
        return pd.DataFrame(columns=["PID", "PLAYER", descr])  # Ensure structure

    table = []
    for item in data:
        raw_value = str(item.get("stats", [{}])[0].get("statValue", np.nan)).replace(",", "").replace("'", "").strip()
        clean_value = convert_feet_inches(raw_value)

        table.append({
            "PID": item.get("playerId", "Unknown"),
            "PLAYER": item.get("playerName", "Unknown"),
            descr: clean_value
        })

    df = pd.DataFrame(table)

    # 🔹 Ensure all required columns exist
    if "PID" not in df.columns:
        df["PID"] = np.nan
    if "PLAYER" not in df.columns:
        df["PLAYER"] = np.nan

    return df


def insert_into_db(df, conn):
    cursor = conn.cursor()
    column_names = df.columns.tolist()
    
    # ✅ Ensure ON CONFLICT references the correct unique constraint name
    insert_query = """
        INSERT INTO off_the_tee_table ({})
        VALUES %s
        ON CONFLICT ON CONSTRAINT off_the_tee_pid_year_unique DO NOTHING;
    """.format(", ".join(column_names))
    
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]
    
    print("--- Debugging: Checking Data Before Insert ---")
    print("Columns in DataFrame:", df.columns.tolist())
    print(f"Total records to insert: {len(values)}")

    if values:
        execute_values(cursor, insert_query, values)
        conn.commit()
        print("Data committed to PostgreSQL successfully!")
    else:
        print("No records to insert.")
    
    cursor.close()

def clean_data(df):
    df.replace({"-": np.nan}, inplace=True)
    df.dropna(thresh=len(df.columns) - 25, inplace=True)
    return df

def main():
    years = list(range(2007, 2025))
    dfs = []
    
    for year in years:
        print(f"Processing data for Year: {year}")
        df = pd.DataFrame()
        for key, stat in off_the_tee_stats.items():
            stat_df = get_df(year, key, stat)
            df = stat_df if df.empty else df.merge(stat_df, on=["PID", "PLAYER"], how="outer")
        
        rename_mapping = {"PLAYER": "player_name", "PID": "pid", "Year": "year"}
        rename_mapping.update(off_the_tee_stats)
        df.rename(columns=rename_mapping, inplace=True)
        df["year"] = year  
        print(f"Year {year}: {len(df)} records loaded.")
        dfs.append(df)
    
    combined_df = pd.concat(dfs, ignore_index=True)
    combined_df = clean_data(combined_df)
    conn = connect_to_db()
    insert_into_db(combined_df, conn)
    conn.close()
    print("Pipeline completed successfully!")

if __name__ == "__main__":
    main()


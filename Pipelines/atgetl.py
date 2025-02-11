import requests
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

atg_stats = {  
    "362": "scrambling_from_sand",
    "363": "scrambling_from_rough",
    "365": "scrambling_from_other",
    "366": "scrambling_from_more_30",
    "367": "scrambling_from_20_30",
    "368": "scrambling_from_10_20",
    "369": "scrambling_from_less_10",
    "111": "sand_save_percent",
    "370": "sand_save_from_more_30",
    "371": "sand_save_from_20_30",
    "372": "sand_save_from_10_20",
    "373": "sand_save_from_less_10",
    "374": "proximity_to_hole",
    "375": "proximity_to_hole_sand",
    "376": "proximity_to_hole_rough",
    "378": "proximity_to_hole_other",
    "379": "proximity_to_hole_more_30",
    "380": "proximity_to_hole_20_30",
    "381": "proximity_to_hole_10_20",
    "382": "proximity_to_hole_less_10",
    "481": "scrambling_avg_distance_to_hole",
    "466": "scrambling_rtp_more_30",
    "467": "scrambling_rtp_20_30",
    "468": "scrambling_rtp_10_20",
    "464": "scrambling_rtp_rough"
}

# 🔹 Connect to PostgreSQL
def connect_to_db():
    return psycopg2.connect(
        dbname="",
        user="",
        password="",
        host="localhost",
        port="5432"
    )
    
    # 🔹 Convert Feet & Inches to Decimal Format
def convert_feet_inches(value):
    if isinstance(value, str):
        value = value.strip()
        if "'" in value:  # Example: "5'10"
            parts = value.split("'")
            feet = int(parts[0].strip()) if parts[0].strip().isdigit() else 0
            inches = int(parts[1].replace('"', '').strip()) if len(parts) > 1 and parts[1].replace('"', '').strip().isdigit() else 0
            return round(feet + (inches / 12), 2)  # Convert to decimal feet
        elif " " in value:  # Example: "95 6"
            parts = value.split()
            feet = int(parts[0].strip()) if parts[0].strip().isdigit() else 0
            inches = int(parts[1].strip()) if len(parts) > 1 and parts[1].strip().isdigit() else 0
            return round(feet + (inches / 12), 2)
        else:
            try:
                return float(value.replace('"', '').strip())
            except ValueError:
                return np.nan
    return value  # Return as is if not a string


# 🔹 Fetch Data from API
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
    response = requests.post(url, json=req, headers={"x-api-key": "API KEY"})
    
    if response.status_code != 200:
        print(f"❌ API ERROR {response.status_code} for {descr} ({stat_id}) in {year}")
        return pd.DataFrame(columns=["PID", "PLAYER", descr])  

    data = response.json().get("data", {}).get("statDetails", {}).get("rows", [])

    if not data:
        print(f"⚠️ No data for {descr} ({stat_id}) in {year}")
        return pd.DataFrame(columns=["PID", "PLAYER", descr])  

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

# 🔹 Insert into PostgreSQL
def insert_into_db(df, conn):
    cursor = conn.cursor()
    column_names = df.columns.tolist()
    
    # ✅ Ensure ON CONFLICT references the correct unique constraint name
    insert_query = """
    INSERT INTO around_the_green_table ({})
    VALUES %s
    ON CONFLICT ON CONSTRAINT around_the_green_pid_year_unique DO NOTHING;
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

# 🔹 Clean Data
def clean_data(df):
    df.replace({"-": np.nan}, inplace=True)
    df.dropna(thresh=len(df.columns) - 15, inplace=True)
    return df

# 🔹 Main Pipeline
def main():
    years = list(range(2007, 2025))
    dfs = []
    
    for year in years:
        print(f"Processing data for Year: {year}")
        df = pd.DataFrame()
        for key, stat in atg_stats.items():
            stat_df = get_df(year, key, stat)
            df = stat_df if df.empty else df.merge(stat_df, on=["PID", "PLAYER"], how="outer")
        
        rename_mapping = {"PLAYER": "player_name", "PID": "pid", "Year": "year"}
        rename_mapping.update(atg_stats)
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

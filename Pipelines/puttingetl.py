import requests
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values


putting_stats = {   
    "02428": "total_putting",
    "02439": "bonus_putting",
    "104": "putting_avg",
    "402": "overall_putting_avg",
    "115": "birdie_or_better_conv_percent",
    "119": "putts_per_rd",
    "393": "putts_per_rd1",
    "394": "putts_per_rd2",
    "395": "putts_per_rd3",
    "396": "putts_per_rd4",
    "398": "one_putts_per_rd",
    "399": "two_putts_per_rd",
    "400": "three_putts_per_rd",
    "401": "three_plus_putts_per_rd",
    "413": "one_putt_percent",
    "414": "one_putt_percent_rd1",
    "415": "one_putt_percent_rd2",
    "416": "one_putt_percent_rd3",
    "417": "one_putt_percent_rd4",
    "420": "total_one_putts_inside_5ft",
    "421": "total_one_putts_5_10",
    "422": "total_one_putts_10_15",
    "423": "total_one_putts_15_20",
    "424": "total_one_putts_20_25",
    "425": "total_one_putts_more_25",
    "498": "longest_putts",
    "426": "three_putt_avoidance",
    "427": "three_putt_avoidance_rd1",
    "428": "three_putt_avoidance_rd2",
    "429": "three_putt_avoidance_rd3",
    "430": "three_putt_avoidance_rd4",
    "068": "three_putt_avoidance_inside_5ft",
    "069": "three_putt_avoidance_inside_5_10",
    "070": "three_putt_avoidance_inside_10_15",
    "145": "three_putt_avoidance_inside_15_20",
    "146": "three_putt_avoidance_inside_20_25",
    "147": "three_putt_avoidance_inside_more_25",
    "441": "total_three_putts_inside_5ft",
    "442": "total_three_putts_inside_5_10",
    "443": "total_three_putts_inside_10_15",
    "444": "total_three_putts_inside_15_20",
    "445": "total_three_putts_inside_20_25",
    "446": "total_three_putts_inside_more_25",
    "408": "putts_made_percent_more_25",
    "02429": "putts_made_percent_more_20",
    "02328": "putts_made_percent_15_25",
    "407": "putts_made_percent_20_25",
    "406": "putts_made_percent_15_20",
    "02327": "putts_made_percent_5_15",
    "405": "putts_made_percent_10_15",
    "484": "putts_made_percent_inside_10ft",
    "404": "putts_made_percent_5_10",
    "02427": "putts_made_percent_3_5",
    "403": "putts_made_percent_inside_5ft",
    "348": "putts_made_percent_10ft",
    "346": "putts_made_percent_8ft",
    "345": "putts_made_percent_7ft",
    "344": "putts_made_percent_6ft",
    "343": "putts_made_percent_5ft",
    "356": "putting_made_percent_more_10",
    "485": "putting_made_percent_4_8",
    "342": "putting_made_percent_4ft",
    "341": "putting_from_more_3ft",
    "434": "putts_made_per_event_over_10ft",
    "435": "putts_made_per_event_over_20ft",
    "438": "avg_distance_of_putts_made",
    "02440": "avg_distance_of_birdie_putts_made",
    "02442": "avg_distance_of_eagle_putts_made",
    "135": "putts_made_distance",
    "349": "approach_putt_performance",
    "409": "avg_putting_distance_all_1_putts",
    "410": "avg_putting_distance_all_2_putts",
    "389": "avg_putting_distance_gir_1_putts",
    "390": "avg_putting_distance_gir_2_putts",
    "073": "gir_putting_avg_more_35",
    "072": "gir_putting_avg_30_35",
    "071": "gir_putting_avg_25_30",
    "388": "gir_putting_more_25",
    "387": "gir_putting_20_25",
    "386": "gir_putting_15_20",
    "385": "gir_putting_10_15"
}


# 🔹 Connect to PostgreSQL
def connect_to_db():
    return psycopg2.connect(
        dbname=",
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
    response = requests.post(url, json=req, headers={"x-api-key": "API Key"})
    
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
    INSERT INTO putting_stats_table ({})
    VALUES %s
    ON CONFLICT ON CONSTRAINT putting_stats_pid_year_unique DO NOTHING;
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
        for key, stat in putting_stats.items():
            stat_df = get_df(year, key, stat)
            df = stat_df if df.empty else df.merge(stat_df, on=["PID", "PLAYER"], how="outer")
        
        rename_mapping = {"PLAYER": "player_name", "PID": "pid", "Year": "year"}
        rename_mapping.update(putting_stats)
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
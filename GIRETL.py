
import requests
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

gir_stats = {    
    "02437": "greens_fringe_in_regs",
    "326": "gir_200_plus",
    "327": "gir_175_200",
    "328": "gir_150_175",
    "329": "gir_125_150",
    "330": "gir_less_125",
    "077": "gir_100_125",
    "02332": "gir_100_plus",
    "02330": "gir_less_100",
    "078": "gir_75_100",
    "079": "gir_less_75",
    "190": "gir_from_fairway",
    "02434": "gir_from_fw_bunker",
    "199": "gir_otf",
    "331": "proximity_to_hole",
    "02361": "approach_more_275",
    "02360": "approach_250_275",
    "02359": "approach_225_250",
    "02358": "approach_200_225",
    "336": "approach_more_200",
    "337": "approach_175_200",
    "338": "approach_150_175",
    "339": "approach_125_150",
    "340": "approach_50_125",
    "074": "approach_100_125",
    "075": "approach_75_100",
    "076": "approach_50_75",
    "02329": "approach_inside_100",
    "02331": "approach_more_100",
    "431": "fairway_proximity",
    "437": "rough_proximity",
    "432": "left_rough_proximity",
    "433": "right_rough_proximity",
    "02375": "approach_rgh_more_275",
    "02374": "approach_rgh_250_275",
    "02373": "approach_rgh_225_250",
    "02372": "approach_rgh_200_225",
    "02371": "approach_rgh_more_100",
    "02370": "approach_rgh_inside_100",
    "02369": "approach_rgh_more_200",
    "02368": "approach_rgh_175_200",
    "02367": "approach_rgh_150_175",
    "02366": "approach_rgh_125_150",
    "02365": "approach_rgh_50_125",
    "02364": "approach_rgh_100_125",
    "02363": "approach_rgh_75_100",
    "02362": "approach_rgh_50_75",
    "02333": "birdie_or_better_fwy",
    "02334": "birdie_or_better_lr",
    "02335": "birdie_or_better_rr",
    "02336": "birdie_or_better_rgh",
    "357": "birdie_or_better_200_plus",
    "358": "birdie_or_better_175_200",
    "359": "birdie_or_better_150_175",
    "360": "birdie_or_better_125_150",
    "361": "birdie_or_better_less_125",
    "02379": "approach_rtp_more_275",
    "02378": "approach_rtp_250_275",
    "02377": "approach_rtp_225_250",
    "02376": "approach_rtp_200_225",
    "480": "approach_rtp_more_200",
    "479": "approach_rtp_175_200",
    "478": "approach_rtp_150_175",
    "473": "approach_rtp_125_150",
    "472": "approach_rtp_less_125",
    "028": "approach_rtp_100_125",
    "029": "approach_rtp_75_100",
    "030": "approach_rtp_50_75",
    "02380": "approach_rtp_50_75_rgh",
    "02381": "approach_rtp_75_100_rgh",
    "02382": "approach_rtp_100_125_rgh",
    "02383": "approach_rtp_50_125_rgh",
    "02384": "approach_rtp_125_150_rgh",
    "02385": "approach_rtp_150_175_rgh",
    "02386": "approach_rtp_175_200_rgh",
    "02387": "approach_rtp_more_200_rgh",
    "02388": "approach_rtp_less_100_rgh",
    "02389": "approach_rtp_more_100_rgh",
    "02390": "approach_rtp_200_225_rgh",
    "02391": "approach_rtp_225_250_rgh",
    "02392": "approach_rtp_250_275_rgh",
    "02393": "approach_rtp_more_275_rgh",
    "469": "approach_left_rgh",
    "470": "approach_right_rgh",
    "471": "fairway_approach",
    "419": "going_for_green",
    "486": "going_for_green_hit_green_percent",
    "02357": "going_for_green_birdie_or_better",
    "436": "par5_going_for_green",
    "02426": "avg_going_for_it_distance",
    "02431": "avg_distance_after_going_for_it",
    "350": "total_hole_outs",
    "351": "longest_hole_out",
    "02325": "avg_approach_shot_distance",
    "02338": "avg_approach_distance_birdie_or_better",
    "02339": "avg_approach_distance_par",
    "02340": "avg_approach_distance_bogey_or_worse",
    "02430": "avg_distance_to_hole_after_tee_shot"
}

# 🔹 Connect to PostgreSQL
def connect_to_db():
    return psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
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
    response = requests.post(url, json=req, headers={"x-api-key": "api key"})
    
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
    INSERT INTO greens_in_regulation_table ({})
    VALUES %s
    ON CONFLICT ON CONSTRAINT greens_in_regulation_pid_year_unique DO NOTHING;
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
    df.dropna(thresh=len(df.columns) - 25, inplace=True)
    return df

# 🔹 Main Pipeline
def main():
    years = list(range(2007, 2025))
    dfs = []
    
    for year in years:
        print(f"Processing data for Year: {year}")
        df = pd.DataFrame()
        for key, stat in gir_stats.items():
            stat_df = get_df(year, key, stat)
            df = stat_df if df.empty else df.merge(stat_df, on=["PID", "PLAYER"], how="outer")
        
        rename_mapping = {"PLAYER": "player_name", "PID": "pid", "Year": "year"}
        rename_mapping.update(gir_stats)
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
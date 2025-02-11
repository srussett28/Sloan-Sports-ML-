import requests
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

scoring_stats = {     
    "108": "scoring_avg_actual",
    "116": "scoring_avg_before_cut",
    "02417": "stroke_difference_field_avg",
    "299": "lowest_round",
    "152": "rounds_in_the_60s",
    "153": "sub_par_rounds",
    "156": "birdie_avg",
    "107": "total_birdies",
    "155": "avg_holes_between_eagles",
    "106": "total_eagles",
    "105": "par_breakers",
    "160": "bounce_back",
    "02415": "birdie_to_bogey_ratio",
    "112": "par3_birdies_or_better",
    "113": "par4_birdies_or_better",
    "114": "par5_birdies_or_better",
    "447": "par4_eagles",
    "448": "par5_eagles",
    "352": "birdie_or_better_percent",
    "357": "birdie_or_better_percent_200_plus",
    "358": "birdie_or_better_percent_175_200",
    "359": "birdie_or_better_percent_150_175",
    "360": "birdie_or_better_percent_125_150",
    "361": "birdie_or_better_percent_less_125",
    "02414": "bogey_avoidance",
    "02416": "reverse_bounce_back",
    "02419": "bogey_avg",
    "118": "final_rd_scoring_avg",
    "219": "final_rd_performance",
    "220": "top_10_final_rd_performance",
    "309": "top_5_final_rd_performance",
    "310": "final_rd_performance_11_25",
    "311": "final_rd_performance_25_plus",
    "453": "final_rd_performance_6_10",
    "148": "rd1_scoring_avg",
    "149": "rd2_scoring_avg",
    "117": "rd3_scoring_avg",
    "285": "rd4_scoring_avg",
    "245": "front9_rd1_scoring_avg",
    "246": "back9_rd1_scoring_avg",
    "253": "front9_rd2_scoring_avg",
    "254": "back9_rd2_scoring_avg",
    "261": "front9_rd3_scoring_avg",
    "262": "back9_rd3_scoring_avg",
    "269": "front9_rd4_scoring_avg",
    "270": "back9_rd4_scoring_avg",
    "171": "par3_performance",
    "142": "par3_scoring_avg",
    "172": "par4_performance",
    "143": "par4_scoring_avg",
    "173": "par5_performance",
    "144": "par5_scoring_avg",
    "207": "front9_scoring_avg",
    "301": "front9_low_round",
    "208": "back9_scoring_avg",
    "302": "back9_low_round",
    "292": "early_scoring_avg",
    "303": "early_lowest_round",
    "209": "first_tee_early_scoring_avg",
    "210": "tenth_tee_early_scoring_avg",
    "247": "early_rd1_scoring_avg",
    "255": "early_rd2_scoring_avg",
    "263": "early_rd3_scoring_avg",
    "271": "early_rd4_scoring_avg",
    "249": "first_tee_early_rd1_scoring_avg",
    "250": "tenth_tee_early_rd1_scoring_avg",
    "257": "first_tee_early_rd2_scoring_avg",
    "258": "tenth_tee_early_rd2_scoring_avg",
    "265": "first_tee_early_rd3_scoring_avg",
    "266": "tenth_tee_early_rd3_scoring_avg",
    "273": "first_tee_early_rd4_scoring_avg",
    "274": "tenth_tee_early_rd4_scoring_avg",
    "305": "first_tee_early_lowest_rd",
    "306": "tenth_tee_early_lowest_rd",
    "293": "late_scoring_avg",
    "304": "late_lowest_round",
    "248": "late_rd1_scoring_avg",
    "256": "late_rd2_scoring_avg",
    "264": "late_rd3_scoring_avg",
    "272": "late_rd4_scoring_avg",
    "211": "first_tee_late_scoring_avg",
    "212": "tenth_tee_late_scoring_avg",
    "307": "first_tee_late_lowest_rd",
    "308": "tenth_tee_late_lowest_rd"
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
    response = requests.post(url, json=req, headers={"x-api-key": ""})
    
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
    INSERT INTO scoring_stats_table ({})
    VALUES %s
    ON CONFLICT ON CONSTRAINT scoring_stats_pid_year_unique DO NOTHING;
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
        for key, stat in scoring_stats.items():
            stat_df = get_df(year, key, stat)
            df = stat_df if df.empty else df.merge(stat_df, on=["PID", "PLAYER"], how="outer")
        
        rename_mapping = {"PLAYER": "player_name", "PID": "pid", "Year": "year"}
        rename_mapping.update(scoring_stats)
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
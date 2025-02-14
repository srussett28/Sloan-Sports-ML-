import requests
import pandas as pd

tournament_ids = {
    "R2024016": ["656"], 
    "R2024006": ["006"], 
    "R2024002": ["704"],
    "R2024002": ["233"],
    "R2024002": ["704"],
    "R2024004": ["004"],
    "R2024005": ["005"],
    "R2024005": ["205"],
    "R2024003": ["510"],
    "R2024007": ["500"],
    "R2024540": ["978"],
    "R2024010": ["928"],
    "R2024009": ["009"],
    "R2024483": ["742"],
    "R2024011": ["011"],
    "R2024475": ["665"],
    "R2024020": ["897"],
    "R2024041": ["770"],
    "R2024014": ["014"],
    "R2024012": ["012"],
    "R2024522": ["244"],
    "R2024018": ["714"],
    "R2024019": ["921"],
    "R2024480": ["872"],
    "R2024553": ["925"],
    "R2024033": ["671"],
    "R2024021": ["021"],
    "R2024032": ["874"],
    "R2024023": ["023"],
    "R2024026": ["528"],
    "R2024034": ["503"],
    "R2024524": ["876"],
    "R2024030": ["669"],
    "R2024541": ["997"],
    "R2024518": ["884"],
    "R2024100": ["100"],
    "R2024472": "Barracuda Championship",
    "R2024525": ["883"],
    "R2024013": ["752"],
    "R2024027": ["513"],
    "R2024028": ["406"],
    "R2024060": ["933"],
    "R2024464": ["926"],
    "R2024054": ["746"],
    "R2024554": ["930"],
    "R2024047": ["538"],
    "R2024527": ["875"],
    "R2024457": ["919"],
    "R2024528": ["765"],
    "R2024493": ["776"],
    "R2024478": ["478"],
    "R2024551": ["920"]    
    }

holes = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18]

def get_hole_details(tournament_id, course_id, hole, api_key):
    """
    Fetch hole-specific stats for a given tournament, course, and hole from the PGA API.

    Args:
        tournament_id (str): The tournament ID (e.g., "R2024540").
        course_id (str): The course ID (e.g., "978").
        hole (int): The hole number (e.g., 12).
        api_key (str): Your API key for the PGA Tour API.

    Returns:
        pd.DataFrame: DataFrame containing detailed hole statistics.
    """

    url = "https://orchestrator.pgatour.com/graphql"

    query = {
        "operationName": "HoleDetails",
        "variables": {"tournamentId": tournament_id, "courseId": course_id, "hole": hole},
        "query": """
        query HoleDetails($tournamentId: ID!, $courseId: ID!, $hole: Int!) {
          holeDetails(tournamentId: $tournamentId, courseId: $courseId, hole: $hole) {
            tournamentId
            courseId
            holeNum
            statsSummary {
              eagles
              eaglesPercent
              birdies
              birdiesPercent
              pars
              parsPercent
              bogeys
              bogeysPercent
              doubleBogeys
              doubleBogeysPercent
            }
            holeInfo {
              par
              yards
              scoringAverageDiff
              rank
              rounds
            }
          }
        }
        """
    }

    response = requests.post(
        url,
        json=query,
        headers={"x-api-key": api_key}
    )

    if response.status_code != 200:
        print(f"❌ API ERROR {response.status_code} for {tournament_id} - Course {course_id} - Hole {hole}")
        return pd.DataFrame()

    try:
        data = response.json()["data"]["holeDetails"]
    except KeyError:
        print(f"⚠️ No data available for {tournament_id} - Course {course_id} - Hole {hole}")
        return pd.DataFrame()

    stats = [{
        "tournament_id": data["tournamentId"],
        "course_id": data["courseId"],
        "hole_number": data["holeNum"],
        "par": data["holeInfo"]["par"],
        "yards": data["holeInfo"]["yards"],
        "scoring_avg_diff": data["holeInfo"]["scoringAverageDiff"],
        "rank": data["holeInfo"]["rank"],  # 🏆 Hole Rank Added
        "rounds_played": data["holeInfo"]["rounds"],  # 🏌️‍♂️ Rounds Played Added
        "eagles": data["statsSummary"]["eagles"],
        "eagles_percent": data["statsSummary"]["eaglesPercent"],
        "birdies": data["statsSummary"]["birdies"],
        "birdies_percent": data["statsSummary"]["birdiesPercent"],
        "pars": data["statsSummary"]["pars"],
        "pars_percent": data["statsSummary"]["parsPercent"],
        "bogeys": data["statsSummary"]["bogeys"],
        "bogeys_percent": data["statsSummary"]["bogeysPercent"],
        "double_bogeys": data["statsSummary"]["doubleBogeys"],
        "double_bogeys_percent": data["statsSummary"]["doubleBogeysPercent"]
    }]

    return pd.DataFrame(stats)


df = get_hole_details("R2024026", "528", 1, API )

# 🔹 Show the result
print(df)

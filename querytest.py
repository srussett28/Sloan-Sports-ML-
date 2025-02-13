import requests
import pandas as pd

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


df = get_hole_details("R2024026", "528", 1, "da2-gsrx5bibzbb4njvhl7t37wqyl4" )

# 🔹 Show the result
print(df)

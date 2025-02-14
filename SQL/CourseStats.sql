CREATE TABLE course_stats_table (
    year INT,
    tournament_id TEXT,
    course_id TEXT,
    hole_number INT,
    par INT,
    yards INT,
    scoring_avg_diff FLOAT,
    eagles INT,
    eagles_percent FLOAT,
    birdies INT,
    birdies_percent FLOAT,
    pars INT,
    pars_percent FLOAT,
    bogeys INT,
    bogeys_percent FLOAT,
    double_bogeys INT,
    double_bogeys_percent FLOAT,
    PRIMARY KEY (year, tournament_id, course_id, hole_number)
);


SELECT * FROM tournament_results LIMIT 500;

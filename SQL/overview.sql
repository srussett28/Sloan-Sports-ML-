CREATE TABLE overview_table (
    id SERIAL PRIMARY KEY,
    pid INT NOT NULL,
    player_name VARCHAR(255) NOT NULL,
    year INT NOT NULL,
    scoring_avg_adjusted FLOAT,
    birdie_avg FLOAT,
    sg_total FLOAT,
    driving_distance FLOAT,
    sg_approach_green FLOAT,
    gir_percent FLOAT,
    scrambling FLOAT,
    sg_putting FLOAT,
    wins INT,
    country VARCHAR(255)
);

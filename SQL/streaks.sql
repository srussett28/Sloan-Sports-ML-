DROP TABLE IF EXISTS streaks_stats_table;

CREATE TABLE streaks_stats_table (
    id SERIAL PRIMARY KEY,
    pid INT NOT NULL,
    player_name TEXT,
    year INT NOT NULL,
    consecutive_cuts FLOAT8,
    ytd_consecutive_cuts FLOAT8,
    current_streak_wo_3_putt FLOAT8,
    consecutive_fairways_hit FLOAT8,
    consecutive_gir FLOAT8,
    consecutive_sand_saves FLOAT8,
    best_ytd_1_putt_streak FLOAT8,
    best_ytd_streak_wo_3_putt FLOAT8,
    streak_rds_in_60s FLOAT8,
    ytd_streak_rds_in_60s FLOAT8,
    streak_sub_par_rds FLOAT8,
    ytd_streak_sub_par_rds FLOAT8,
    streak_par_or_better FLOAT8,
    ytd_streak_par_or_better FLOAT8,
    consecutive_par3_birdies FLOAT8,
    consecutive_par4_birdies FLOAT8,
    consecutive_par5_birdies FLOAT8,
    consecutive_holes_below_par FLOAT8,
    consecutive_birdie_streak FLOAT8,
    consecutive_birdies_eagles_streak FLOAT8,
    CONSTRAINT streaks_pid_year_unique UNIQUE (pid, year) -- Ensures no duplicates
);


SELECT * FROM streaks_stats_table LIMIT 100;
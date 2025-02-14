CREATE TABLE around_the_green_table (
    id SERIAL PRIMARY KEY,
    pid INT NOT NULL,
    player_name TEXT NOT NULL,
    year INT NOT NULL,
    scrambling_from_sand FLOAT8,
    scrambling_from_rough FLOAT8,
    scrambling_from_other FLOAT8,
    scrambling_from_more_30 FLOAT8,
    scrambling_from_20_30 FLOAT8,
    scrambling_from_10_20 FLOAT8,
    scrambling_from_less_10 FLOAT8,
    sand_save_percent FLOAT8,
    sand_save_from_more_30 FLOAT8,
    sand_save_from_20_30 FLOAT8,
    sand_save_from_10_20 FLOAT8,
    sand_save_from_less_10 FLOAT8,
    proximity_to_hole FLOAT8,
    proximity_to_hole_sand FLOAT8,
    proximity_to_hole_rough FLOAT8,
    proximity_to_hole_other FLOAT8,
    proximity_to_hole_more_30 FLOAT8,
    proximity_to_hole_20_30 FLOAT8,
    proximity_to_hole_10_20 FLOAT8,
    proximity_to_hole_less_10 FLOAT8,
    scrambling_avg_distance_to_hole FLOAT8,
    scrambling_rtp_more_30 FLOAT8,
    scrambling_rtp_20_30 FLOAT8,
    scrambling_rtp_10_20 FLOAT8,
    scrambling_rtp_rough FLOAT8,
    CONSTRAINT around_the_green_pid_year_unique UNIQUE (pid, year)
    );



SELECT * FROM around_the_green_table LIMIT 100;

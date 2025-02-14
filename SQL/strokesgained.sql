CREATE TABLE strokes_gained_table (
    pid INT NOT NULL,
    player_name TEXT NOT NULL,
    year INT NOT NULL,
    sg_off_tee FLOAT,
    sg_around_green FLOAT,
    sg_tee_to_green FLOAT,
    sg_approach_the_green FLOAT,
    PRIMARY KEY (pid, year)
);


SELECT * FROM strokes_gained_table LIMIT 10;

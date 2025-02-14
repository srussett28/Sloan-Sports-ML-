DROP TABLE IF EXISTS money_finishes_stats_table;

CREATE TABLE money_finishes_stats_table (
    id SERIAL PRIMARY KEY,
    pid FLOAT8 NOT NULL,
    player_name TEXT,
    year FLOAT8 NOT NULL,
    top_10_finishes FLOAT8,
    victory_leaders FLOAT8,
    official_money FLOAT8,
    career_money_leaders FLOAT8,
    career_earnings FLOAT8,
    non_member_earnings FLOAT8,
    non_member_wgc_earning FLOAT8,
    fedex_cup_bonus_money FLOAT8,
    earnings_per_event FLOAT8,
    total_money FLOAT8,
    percent_of_available_purse_won FLOAT8,
    percent_of_potential_money_won FLOAT8,
    CONSTRAINT money_finishes_pid_year_unique UNIQUE (pid, year)
);


SELECT * FROM money_finishes_stats_table LIMIT 100;


SELECT * FROM tournament_results tr LIMIT 100;
CREATE TABLE tournament_results (
    player_id VARCHAR(10) NOT NULL,         -- Unique ID for the player
    first_name VARCHAR(50),                -- Player's first name
    last_name VARCHAR(50),                 -- Player's last name
    country VARCHAR(50),                   -- Country of the player
    position INT,                          -- Final position in the tournament
    total_score INT,                       -- Total score for the tournament
    par_relative_score INT,                -- Score relative to par
    round_scores TEXT,                     -- List of round scores (JSON or text format)
    year INT NOT NULL,                     -- Year of the tournament
    tournament_id VARCHAR(20) NOT NULL,   -- Unique ID for the tournament
    PRIMARY KEY (player_id, tournament_id, year)  -- Composite primary key
);
        CREATE TABLE IF NOT EXISTS allPlayers (
            sleeper_player_id VARCHAR PRIMARY KEY,
            first_name VARCHAR,
            last_name VARCHAR,
            age INT,
            years_exp INT,
            position VARCHAR,
            fantasy_positions text[],
            team VARCHAR,
            team_abbr VARCHAR,
            depth_chart_position VARCHAR,
            depth_chart_order INT,
            practice_description VARCHAR,
            status VARCHAR,
            injury_status VARCHAR,
            injury_body_part VARCHAR,
            injury_start_date DATE
        )
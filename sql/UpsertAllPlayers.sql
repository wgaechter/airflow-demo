INSERT INTO allPlayers (sleeper_player_id, first_name, last_name, age, years_exp, position, fantasy_positions, team, team_abbr, depth_chart_position, depth_chart_order, practice_description, status, injury_status, injury_body_part, injury_start_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            ON CONFLICT (sleeper_player_id) DO UPDATE
            SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                age = EXCLUDED.age,
                years_exp = EXCLUDED.years_exp,
                position = EXCLUDED.position,
                fantasy_positions = EXCLUDED.fantasy_positions,
                team = EXCLUDED.team,
                team_abbr = EXCLUDED.team_abbr,
                depth_chart_position = EXCLUDED.depth_chart_position,
                depth_chart_order = EXCLUDED.depth_chart_order,
                practice_description = EXCLUDED.practice_description,
                status = EXCLUDED.status,
                injury_status = EXCLUDED.injury_status,
                injury_body_part = EXCLUDED.injury_body_part,
                injury_start_date = EXCLUDED.injury_start_date

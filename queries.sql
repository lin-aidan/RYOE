select rusher_player_id,rusher_player_name,ydstogo,yards_gained
from plays
where rush_attempt=1;


select ydstogo,avg(yards_gained) as avg_yards
from plays
where rush_attempt=1
group by ydstogo;

select 
    week, 
    row_number() over (partition by week order by play_id) as play_number, 
    home_team, 
    away_team, 
    posteam, 
    case 
        when posteam='PHI' then epa
        else -epa
    end as PHI_epa
from plays
where (home_team='PHI' or away_team='PHI') and posteam is not null
order by week, play_id;

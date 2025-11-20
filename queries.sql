select rusher_player_id,rusher_player_name,ydstogo,yards_gained
from plays
where rush_attempt=1


select ydstogo,avg(yards_gained) as avg_yards
from plays
where rush_attempt=1
group by ydstogo



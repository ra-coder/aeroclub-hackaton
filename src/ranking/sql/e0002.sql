drop table e0002_random_ranking_client_filter_policy;

create table e0002_random_ranking_client_filter_policy
(
    option_id  int primary key references client_requests,
    request_id int,
    score      float,
    selected   bool
);

create index on e0002_random_ranking_client_filter_policy (request_id);

insert into e0002_random_ranking_client_filter_policy (option_id, request_id, score, selected)
select id,
       requestid,
       case when intravelpolicy then random()
       else 0
       end,
       selectedvariant
from client_requests;


with scores as (select user_selected.request_id,
                       user_selected.score,
                       user_selected.score = max(other.score)                      as top_hit,
                       count(*) filter ( where other.score > user_selected.score ) as rank_error
                from e0002_random_ranking_client_filter_policy as user_selected
                         join e0002_random_ranking_client_filter_policy as other on other.request_id = user_selected.request_id
                where user_selected.selected = True
                group by user_selected.request_id, user_selected.score)
select count(*)                                        as total,
       count(*) filter ( where scores.top_hit = True ) as top_hit_count,
       sum(rank_error)                                 as rank_befor_count
from scores;

-- total,top_hit_count,rank_befor_count
-- 4309,382,305631



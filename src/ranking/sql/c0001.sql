drop table e0001_random_ranking_client;

create table e0001_random_ranking_client
(
    option_id  int primary key references client_requests,
    request_id int,
    score      float,
    selected   bool
);

create index on e0001_random_ranking_client (request_id);

insert into e0001_random_ranking_client (option_id, request_id, score, selected)
select id,
       requestid,
       random(),
       selectedvariant
from client_requests;


with scores as (select user_selected.request_id,
                       user_selected.score,
                       user_selected.score = max(other.score)                      as top_hit,
                       count(*) filter ( where other.score > user_selected.score ) as rank_error
                from e0001_random_ranking_client as user_selected
                         join e0001_random_ranking_client as other on other.request_id = user_selected.request_id
                where user_selected.selected = True
                group by user_selected.request_id, user_selected.score)
select count(*)                                        as total,
       count(*) filter ( where scores.top_hit = True ) as top_hit_count,
       sum(rank_error)                                 as rank_befor_count
from scores;

-- total,top_hit_count,rank_befor_count
-- 4309,123,485198


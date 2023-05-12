drop table a0001_random_ranking_agent;

create table a0001_random_ranking_agent
(
    option_id  int primary key references client_requests,
    request_id int,
    score_selected bool,
    agent_selected   bool
);

--create index on a0001_random_ranking_agent (request_id);

insert into a0001_random_ranking_agent (option_id, request_id, score_selected, agent_selected)
select id,
       requestid,
        case when intravelpolicy then random() > 0.5
       else false end,
       sentoption
from agent_requests;


select
    count(*) as total,
    count(*) filter ( where score_selected=agent_selected ) as hit_count
from a0001_random_ranking_agent


--- random
-- total,hit_count
-- 637402,318786

--- random with policy
-- total,hit_count
-- 637402,367315

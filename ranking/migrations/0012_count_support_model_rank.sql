select count(*) from support_model_001_on_006;

drop table if exists preprocess_scores_support_model_001_on_006;
create table preprocess_scores_support_model_001_on_006
(
    id            int primary key references agent_requests,
    request_id    int,
    predict       bool,  -- to use as feature
    score         float,  -- to use as feature
    rank          int,  -- to use as feature
    in_top5_rank  bool   -- to use as feature
);
create index on preprocess_scores_support_model_001_on_006 (id);


insert into preprocess_scores_support_model_001_on_006 (id, request_id, predict, score, rank, in_top5_rank)
SELECT agent_requests.id,
       requestid,
       predict,
       score,
       rank() OVER (
           PARTITION BY requestid
           ORDER BY score DESC
           ),
       (rank() OVER (
           PARTITION BY requestid
           ORDER BY score DESC
           ) < 5) as in_top5_rank
from agent_requests
         join support_model_001_on_006 predict on predict.id = agent_requests.id;

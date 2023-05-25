drop table if exists preprocess_scores_support_model_003_on_013_final_test;
create table preprocess_scores_support_model_003_on_013_final_test
(
    id            int primary key references final_test_requests,
    request_id    int,
    predict       bool,  -- to use as feature
    score         float,  -- to use as feature
    rank          int,  -- to use as feature
    in_top5_rank  bool   -- to use as feature
);
create index on preprocess_scores_support_model_003_on_013_final_test (id);


insert into preprocess_scores_support_model_003_on_013_final_test (id, request_id, predict, score, rank, in_top5_rank)
SELECT final_test_requests.id,
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
           ) <= 5) as in_top5_rank
from final_test_requests
         join support_model_003_on_013_final_test predict on predict.id = final_test_requests.id;
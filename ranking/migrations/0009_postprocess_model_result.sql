drop table if exists postprocess_model_005;
create table postprocess_model_005 (
    id  int primary key references agent_requests,
    request_id int,
    predict bool,
    score float,
    rank int,
    fixed_predict bool,
    sentoption bool
);
create index on postprocess_model_005 (id);

insert into postprocess_model_005 (id, request_id, predict, score, rank, fixed_predict, sentoption)
SELECT
    agent_requests.id,
    requestid,
    predict,
    score,
    rank() OVER (
        PARTITION BY requestid
        ORDER BY score DESC
    ),
    predict or  (rank() OVER (
        PARTITION BY requestid
        ORDER BY score DESC
    ) < 5) as fixed_predict,
    sentoption
from agent_requests
join model_005 predict on predict.id = agent_requests.id;

select
    count(*) filter ( where  sentoption=True and fixed_predict =False) as sentoption_miss,
    count(*) filter ( where fixed_predict =True and sentoption=True ) as positive_success_count,
    count(*) filter ( where  sentoption=True ) as sentoption_count,
    count(*) filter ( where fixed_predict =True) as positive_count,
    count(*) filter ( where  sentoption=True and fixed_predict =False and for_test=True ) as sentoption_miss_test,
    count(*) filter ( where fixed_predict =True and sentoption=True and for_test=True ) as positive_success_count_in_test,
    count(*) filter ( where  sentoption=True and for_test=True) as sentoption_count_in_test,
    count(*) filter ( where fixed_predict =True and for_test=True) as positive_count_in_test,
    count(*) filter ( where for_test=True ) as all_test_size,
    count(distinct sample.request_id) filter ( where for_test=True ) as all_test_requests
from postprocess_model_005
join agent_requests_sample_001 sample on postprocess_model_005.id = sample.id

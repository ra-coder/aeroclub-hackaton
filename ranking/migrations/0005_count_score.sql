--create index on model_001_naive_catboost (id);

select
    count(*) filter ( where predict =True and sentoption=True ) as positive_success_count,
    count(*) filter ( where  sentoption=True ) as sentoption_count,
    count(*) filter ( where predict =True) as positive_count,
    count(*) filter ( where predict =True and sentoption=True and for_test=True ) as positive_success_count_in_test,
    count(*) filter ( where  sentoption=True and for_test=True) as sentoption_count_in_test,
    count(*) filter ( where predict =True and for_test=True) as positive_count_in_test

from agent_requests
join agent_requests_sample_001 sample on agent_requests.id = sample.id
join model_001_naive_catboost predict on predict.id = agent_requests.id

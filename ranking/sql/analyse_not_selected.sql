select
    requestid,
    count(*) filter ( where  sentoption=True and predict =False) as sentoption_miss,
    count(*) filter ( where predict =True and sentoption=True ) as positive_success_count,
    count(*) filter ( where  sentoption=True ) as sentoption_count,
    count(*) filter ( where predict =True) as positive_count,
    count(*) filter ( where predict =True and sentoption=True and for_test=True ) as positive_success_count_in_test,
    count(*) filter ( where  sentoption=True and for_test=True) as sentoption_count_in_test,
    count(*) filter ( where predict =True and for_test=True) as positive_count_in_test

from agent_requests
join agent_requests_sample_001 sample on agent_requests.id = sample.id
join model_004_catboost_add_many_segments_features predict on predict.id = agent_requests.id
group by requestid
order by sentoption_miss desc ;
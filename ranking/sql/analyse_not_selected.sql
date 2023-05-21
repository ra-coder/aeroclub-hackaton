select
    for_test,
    count(distinct postpocess.request_id) as requests_count,
    count(*) filter ( where  postpocess.sentoption=True and fixed_predict =False) as sentoption_miss,
    count(*) filter ( where fixed_predict =True and postpocess.sentoption=True ) as positive_success_count,
    count(*) filter ( where  postpocess.sentoption=True ) as sentoption_count,
    count(*) filter ( where fixed_predict =True) as positive_count
--     count(*) filter ( where predict =True and sentoption=True and for_test=True ) as positive_success_count_in_test,
--     count(*) filter ( where sentoption=True and for_test=True) as sentoption_count_in_test,
--     count(*) filter ( where predict =True and for_test=True) as positive_count_in_test

from postprocess_model_012 postpocess
join agent_requests ar on postpocess.id = ar.id
join agent_requests_sample_001 a on ar.id = a.id
group by for_test
--group by employeeid
order by requests_count desc ;

-- request_id
-- 5959801
-- 4852320
-- 5871293
-- 4794334
-- 5214539
-- 4289633
-- 5743750
-- 5876095
-- 6194219

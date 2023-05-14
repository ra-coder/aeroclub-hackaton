select
    request_id,
    count(*) filter ( where  sentoption=True and fixed_predict =False) as sentoption_miss,
    count(*) filter ( where fixed_predict =True and sentoption=True ) as positive_success_count,
    count(*) filter ( where  sentoption=True ) as sentoption_count,
    count(*) filter ( where fixed_predict =True) as positive_count
--     count(*) filter ( where predict =True and sentoption=True and for_test=True ) as positive_success_count_in_test,
--     count(*) filter ( where sentoption=True and for_test=True) as sentoption_count_in_test,
--     count(*) filter ( where predict =True and for_test=True) as positive_count_in_test

from postprocess_model_007 postpocess
group by request_id
order by sentoption_miss desc ;

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

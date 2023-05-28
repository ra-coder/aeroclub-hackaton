drop table if exists postprocess_model_016;
create table postprocess_model_016
(
    id            int primary key references agent_requests,
    request_id    int,
    predict       bool,
    score         float,
    rank          int,
    sentoption_rank int,
    sentoption_fixed_rank int,
    fixed_predict bool,
    sentoption    bool,
    sentoption_fixed    bool
);
create index on postprocess_model_016 (id);

insert into postprocess_model_016 (id, request_id, predict, score, rank,  sentoption_rank, sentoption_fixed_rank,
                                   fixed_predict, sentoption, sentoption_fixed)
SELECT agent_requests.id,
       requestid,
       predict,
       score,
       rank() OVER (
           PARTITION BY requestid
           ORDER BY score DESC
           ),
       rank() OVER (
           PARTITION BY requestid
           ORDER BY (sentoption, predict.id) DESC
           ) as sentoption_rank,
       rank() OVER (
           PARTITION BY requestid
           ORDER BY (sentoption_fixed, predict.id) DESC
           ) as sentoption_fixed_rank,
       (rank() OVER (
           PARTITION BY requestid
           ORDER BY score DESC
           ) < 6) as fixed_predict,
       sentoption,
       sentoption_fixed
from agent_requests
         join model_016_pair_logit_1100 as predict on predict.id = agent_requests.id;

select
    sum(rank) filter ( where sentoption=True and for_test = True ) as test_rank_score,
    sum(sentoption_rank) filter ( where sentoption=True and for_test = True ) as best_rank_score,
    sum(rank) filter ( where sentoption_fixed=True and for_test = True ) as f_test_rank_score,
    sum(sentoption_fixed_rank) filter ( where sentoption_fixed=True and for_test = True ) as f_best_rank_score
       ,

    count(*)
       filter ( where sentoption_fixed = True and fixed_predict = False and for_test = True )          as f_sentoption_miss_test,
       count(*)
       filter ( where fixed_predict = True and sentoption_fixed = True and for_test = True )           as f_positive_success_count_in_test,
       count(*) filter ( where sentoption_fixed = True and for_test = True)                            as f_sentoption_count_in_test,
       count(*) filter ( where fixed_predict = True and for_test = True)                         as f_positive_count_in_test,
       count(*) filter ( where for_test = True )                                                 as all_test_size,
       count(distinct sample.request_id) filter ( where for_test = True )                        as all_test_requests
       ,


    count(*)
       filter ( where sentoption = True and fixed_predict = False and for_test = True )          as sentoption_miss_test,
       count(*)
       filter ( where fixed_predict = True and sentoption = True and for_test = True )           as positive_success_count_in_test,
       count(*) filter ( where sentoption = True and for_test = True)                            as sentoption_count_in_test,
       count(*) filter ( where fixed_predict = True and for_test = True)                         as positive_count_in_test,
       count(*) filter ( where for_test = True )                                                 as all_test_size,
       count(distinct sample.request_id) filter ( where for_test = True )                        as all_test_requests,
        sum(rank) filter ( where sentoption=True ) as rank_score,
        count(*) filter ( where sentoption = True and fixed_predict = False)                      as sentoption_miss,
       count(*) filter ( where fixed_predict = True and sentoption = True )                      as positive_success_count,
       count(*) filter ( where sentoption = True )                                               as sentoption_count,
       count(*) filter ( where fixed_predict = True)                                             as positive_count
from postprocess_model_016
         join agent_requests_sample_001 sample on postprocess_model_016.id = sample.id;




--               precision    recall  f1-score   support
--
--        False       0.97      1.00      0.98     57241
--         True       0.68      0.22      0.33      2323
--
--     accuracy                           0.97     59564

with _tmp as (select 'accuracy' as class,
                     null       as pricission,
                     null       as recall,
                     null       as f1_score,
                     count(*)   as support
              from postprocess_model_016 as postprocess
                       join agent_requests_sample_001 sample on postprocess.id = sample.id and for_test = True
              UNION
              select 'True'                                                          as class,
                     (count(*) filter ( where fixed_predict = True and sentoption = True ))::numeric(12, 6)
                         /
                     (count(*) filter ( where fixed_predict = True))::numeric(12, 6) as pricission,
                     (count(*) filter ( where fixed_predict = True and sentoption = True ))::numeric(12, 6)
                         /
                     (
                                     count(*) filter ( where fixed_predict = FALSE and sentoption = True)
                             +
                                     count(*) filter ( where fixed_predict = True and sentoption = True )
                         )::numeric(12, 6)                                           as recall,
                     null                                                            as f1_score,
                     count(*) filter ( where fixed_predict = True)                   as support
              from postprocess_model_016 as postprocess
                       join agent_requests_sample_001 sample on postprocess.id = sample.id and for_test = True
              UNION
              select 'False'                                                          as class,
                     (count(*) filter ( where fixed_predict = FALSE and sentoption = FALSE ))::numeric(12, 6)
                         /
                     (count(*) filter ( where fixed_predict = FALSE))::numeric(12, 6) as pricission,
                     (count(*) filter ( where fixed_predict = FALSE and sentoption = FALSE ))::numeric(12, 6)
                         /
                     (
                                     count(*) filter ( where fixed_predict = True and sentoption = FALSE)
                             +
                                     count(*) filter ( where fixed_predict = FALSE and sentoption = FALSE )
                         )::numeric(12, 6)                                            as recall,
                     null                                                             as f1_score,
                     count(*) filter ( where fixed_predict = FALSE)                   as support
              from postprocess_model_016 as postprocess
                       join agent_requests_sample_001 sample
                            on postprocess.id = sample.id and for_test = True)
select "class",
       pricission::numeric(12, 3),
       recall::numeric(12, 3),
       (2 * pricission * recall / (pricission + recall))::numeric(12, 3) as f1_score,
       support
from _tmp
order by support;

-- model 13
-- class,pricission,recall,f1_score,support
-- True,0.358,0.703,0.475,3630
-- False,0.986,0.942,0.963,38134
-- accuracy,,,,41764

--model 15
-- class,pricission,recall,f1_score,support
-- True,0.365,0.717,0.484,3636
-- False,0.986,0.942,0.964,38128
-- accuracy,,,,41764


with _tmp2 as (select 'accuracy' as class,
                     null       as pricission,
                     null       as recall,
                     null       as f1_score,
                     count(*)   as support
              from postprocess_model_016 as postprocess
                       join agent_requests_sample_001 sample on postprocess.id = sample.id and for_test = True
              UNION
              select 'True'                                                          as class,
                     (count(*) filter ( where fixed_predict = True and sentoption_fixed = True ))::numeric(12, 6)
                         /
                     (count(*) filter ( where fixed_predict = True))::numeric(12, 6) as pricission,
                     (count(*) filter ( where fixed_predict = True and sentoption_fixed = True ))::numeric(12, 6)
                         /
                     (
                                     count(*) filter ( where fixed_predict = FALSE and sentoption_fixed = True)
                             +
                                     count(*) filter ( where fixed_predict = True and sentoption_fixed = True )
                         )::numeric(12, 6)                                           as recall,
                     null                                                            as f1_score,
                     count(*) filter ( where fixed_predict = True)                   as support
              from postprocess_model_016 as postprocess
                       join agent_requests_sample_001 sample on postprocess.id = sample.id and for_test = True
              UNION
              select 'False'                                                          as class,
                     (count(*) filter ( where fixed_predict = FALSE and sentoption_fixed = FALSE ))::numeric(12, 6)
                         /
                     (count(*) filter ( where fixed_predict = FALSE))::numeric(12, 6) as pricission,
                     (count(*) filter ( where fixed_predict = FALSE and sentoption_fixed = FALSE ))::numeric(12, 6)
                         /
                     (
                                     count(*) filter ( where fixed_predict = True and sentoption_fixed = FALSE)
                             +
                                     count(*) filter ( where fixed_predict = FALSE and sentoption_fixed = FALSE )
                         )::numeric(12, 6)                                            as recall,
                     null                                                             as f1_score,
                     count(*) filter ( where fixed_predict = FALSE)                   as support
              from postprocess_model_016 as postprocess
                       join agent_requests_sample_001 sample
                            on postprocess.id = sample.id and for_test = True)
select "class",
       pricission::numeric(12, 3),
       recall::numeric(12, 3),
       (2 * pricission * recall / (pricission + recall))::numeric(12, 3) as f1_score,
       support
from _tmp2
order by support;


-- class,pricission,recall,f1_score,support
-- True,0.421,0.736,0.535,3636
-- False,0.986,0.947,0.966,38128
-- accuracy,,,,41764

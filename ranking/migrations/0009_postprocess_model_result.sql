drop table if exists postprocess_model_006;
create table postprocess_model_006
(
    id            int primary key references agent_requests,
    request_id    int,
    predict       bool,
    score         float,
    rank          int,
    fixed_predict bool,
    sentoption    bool
);
create index on postprocess_model_006 (id);

insert into postprocess_model_006 (id, request_id, predict, score, rank, fixed_predict, sentoption)
SELECT agent_requests.id,
       requestid,
       predict,
       score,
       rank() OVER (
           PARTITION BY requestid
           ORDER BY score DESC
           ),
       predict or (rank() OVER (
           PARTITION BY requestid
           ORDER BY score DESC
           ) < 6) as fixed_predict,
       sentoption
from agent_requests
         join model_006_airport_features predict on predict.id = agent_requests.id;

select count(*) filter ( where sentoption = True and fixed_predict = False)                      as sentoption_miss,
       count(*) filter ( where fixed_predict = True and sentoption = True )                      as positive_success_count,
       count(*) filter ( where sentoption = True )                                               as sentoption_count,
       count(*) filter ( where fixed_predict = True)                                             as positive_count,
       count(*)
       filter ( where sentoption = True and fixed_predict = False and for_test = True )          as sentoption_miss_test,
       count(*)
       filter ( where fixed_predict = True and sentoption = True and for_test = True )           as positive_success_count_in_test,
       count(*) filter ( where sentoption = True and for_test = True)                            as sentoption_count_in_test,
       count(*) filter ( where fixed_predict = True and for_test = True)                         as positive_count_in_test,
       count(*) filter ( where for_test = True )                                                 as all_test_size,
       count(distinct sample.request_id) filter ( where for_test = True )                        as all_test_requests
from postprocess_model_006
         join agent_requests_sample_001 sample on postprocess_model_006.id = sample.id;



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
              from postprocess_model_006
                       join agent_requests_sample_001 sample on postprocess_model_006.id = sample.id and for_test = True
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
              from postprocess_model_006
                       join agent_requests_sample_001 sample on postprocess_model_006.id = sample.id and for_test = True
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
              from postprocess_model_006
                       join agent_requests_sample_001 sample
                            on postprocess_model_006.id = sample.id and for_test = True)
select "class",
       pricission::numeric(12, 3),
       recall::numeric(12, 3),
       (2 * pricission * recall / (pricission + recall))::numeric(12, 3) as f1_score,
       support
from _tmp
order by support;


-- sentoption_count,positive_count,sentoption_miss_test,positive_success_count_in_test,sentoption_count_in_test,positive_count_in_test,all_test_size,all_test_requests
-- 25008,39360,694,1157,1851,3016,41764,774

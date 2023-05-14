drop table if exists postprocess_model_008_client;
create table postprocess_model_008_client
(
    id            int primary key references client_requests,
    request_id    int,
    predict       bool,
    score         float,
    rank          int,
    fixed_predict bool,
    selectedvariant    bool,
    learn_target bool
);
create index on postprocess_model_008_client (id);

insert into postprocess_model_008_client (id, request_id, predict, score, rank, fixed_predict, selectedvariant, learn_target)
SELECT client_requests.id,
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
           ) < 10) as fixed_predict,
       selectedvariant,
       selectedvariant
           or
       (
            rank() OVER (
                       PARTITION BY requestid
                       ORDER BY score DESC
                       )
            <= 10
        ) as learn_target
from client_requests
         join model_008_client predict on predict.id = client_requests.id;

with _tmp as (select rank,
                     count(*) as cnt
              from postprocess_model_008_client
              where selectedvariant is true
              group by rank
              order by rank
)
select
    _tmp.rank,
    _tmp.cnt,
    sum(_tmp2.cnt) filter ( where _tmp2.rank <= _tmp.rank ) as top_cnt,
    _tmp.rank * sum(_tmp2.cnt) - sum(_tmp2.cnt) filter ( where _tmp2.rank <= _tmp.rank ) as extra,
    sum(_tmp2.cnt) - sum(_tmp2.cnt) filter ( where _tmp2.rank <= _tmp.rank ) as missed,
    sum(_tmp2.cnt) as total_cnt,
    sum(_tmp2.cnt) filter ( where _tmp2.rank <= _tmp.rank ) / sum(_tmp2.cnt) as top_percent
from _tmp
join _tmp as _tmp2 on true
group by _tmp.rank, _tmp.cnt
order by _tmp.rank;

-- rank,cnt,top_cnt,extra,missed,total_cnt,top_percent
-- rank,cnt,top_cnt,extra,missed,total_cnt,top_percent
-- 1,244,244,4065,4065,4309,0.05662566720816894871
-- 2,218,462,8156,3847,4309,0.1072174518449756324
-- 3,175,637,12290,3672,4309,0.14783012299837549315
-- 4,187,824,16412,3485,4309,0.19122766303086563008
-- 5,134,958,20587,3351,4309,0.2223253655140403806
-- 6,130,1088,24766,3221,4309,0.25249477837085170573
-- 7,109,1197,28966,3112,4309,0.27779067068925504757
-- 8,110,1307,33165,3002,4309,0.30331863541424924576
-- 9,97,1404,37377,2905,4309,0.32582965885356231144
-- 10,87,1491,41599,2818,4309,0.34601995822696681365
-- 11,94,1585,45814,2724,4309,0.36783476444650731028
-- 12,72,1657,50051,2652,4309,0.38454397772104896728
-- 13,82,1739,54278,2570,4309,0.40357391506149918775
-- 14,75,1814,58512,2495,4309,0.42097934555581341379
-- 15,72,1886,62749,2423,4309,0.43768855883035507078
-- 16,57,1943,67001,2366,4309,0.45091668600603388257
-- 17,63,2006,71247,2303,4309,0.46553724762125783244
-- 18,65,2071,75491,2238,4309,0.48062195404966349501
-- 19,50,2121,79750,2188,4309,0.49222557437920631237
-- 20,54,2175,84005,2134,4309,0.50475748433511255512
-- 21,57,2232,88257,2077,4309,0.51798561151079136691
-- 22,42,2274,92524,2035,4309,0.52773265258760733349
-- 23,47,2321,96786,1988,4309,0.53864005569737758181
-- 24,37,2358,101058,1951,4309,0.54722673474123926665
-- 25,55,2413,105312,1896,4309,0.55999071710373636575
-- 26,40,2453,109581,1856,4309,0.56927361336737061963
-- 27,45,2498,113845,1811,4309,0.57971687166395915526
-- 28,35,2533,118119,1776,4309,0.58783940589463912741
-- 29,30,2563,122398,1746,4309,0.59480157809236481782
-- 30,32,2595,126675,1714,4309,0.60222789510327222093
-- 31,37,2632,130947,1677,4309,0.61081457414713390578
-- 32,34,2666,135222,1643,4309,0.61870503597122302158
-- 33,26,2692,139505,1617,4309,0.62473891854258528661
-- 34,29,2721,143785,1588,4309,0.63146901833372012068
-- 35,27,2748,148067,1561,4309,0.63773497331167324205
-- 36,29,2777,152347,1532,4309,0.64446507310280807612
-- 37,27,2804,156629,1505,4309,0.65073102808076119749
-- 38,17,2821,160921,1488,4309,0.6546762589928057554
-- 39,29,2850,165201,1459,4309,0.66140635878394058946
-- 40,24,2874,169486,1435,4309,0.6669760965421211418
-- 41,25,2899,173770,1410,4309,0.67277790670689255048
-- 42,26,2925,178053,1384,4309,0.6788117892782548155




--alter table client_requests add column sentoption bool default False;

update client_requests
set sentoption = learn_target
from postprocess_model_008_client where postprocess_model_008_client.id = client_requests.id
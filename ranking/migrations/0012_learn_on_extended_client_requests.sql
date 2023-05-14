drop table if exists postprocess_model_006_client;
create table postprocess_model_006_client
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
create index on postprocess_model_006_client (id);

insert into postprocess_model_006_client (id, request_id, predict, score, rank, fixed_predict, selectedvariant, learn_target)
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
           ) < 6) as fixed_predict,
       selectedvariant,
       selectedvariant
           or
       (
            rank() OVER (
                       PARTITION BY requestid
                       ORDER BY score DESC
                       )
            <= 20
        ) as learn_target
from client_requests
         join model_006_airport_features_client predict on predict.id = client_requests.id;

with _tmp as (select rank,
                     count(*) as cnt
              from postprocess_model_006_client
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
-- 1,250,250,4059,4059,4309,0.0580181016477140868
-- 2,206,456,8162,3853,4309,0.10582501740543049431
-- 3,160,616,12311,3693,4309,0.14295660245996750986
-- 4,153,769,16467,3540,4309,0.17846368066836853098
-- 5,121,890,20655,3419,4309,0.20654444186586214899
-- 6,113,1003,24851,3306,4309,0.23276862381062891622
-- 7,112,1115,29048,3194,4309,0.25876073334880482711
-- 8,93,1208,33264,3101,4309,0.28034346716175446739
-- 9,104,1312,37469,2997,4309,0.3044789974472035275
-- 10,81,1393,41697,2916,4309,0.32327686238106289162
-- 11,101,1494,45905,2815,4309,0.34671617544673938269
-- 12,83,1577,50131,2732,4309,0.3659781851937804595
-- 13,66,1643,54374,2666,4309,0.38129496402877697842
-- 14,79,1722,58604,2587,4309,0.39962868414945462984
-- 15,62,1784,62851,2525,4309,0.41401717335808772337
-- 16,56,1840,67104,2469,4309,0.42701322812717567881
-- 17,70,1910,71343,2399,4309,0.44325829658853562311
-- 18,65,1975,75587,2334,4309,0.45834300301694128568
-- 19,53,2028,79843,2281,4309,0.47064284056625667208
-- 20,46,2074,84106,2235,4309,0.48131817126943606405
-- 21,37,2111,88378,2198,4309,0.4899048503132977489
-- 22,42,2153,92645,2156,4309,0.49965189139011371548
-- 23,33,2186,96921,2123,4309,0.50731028080761197494
-- 24,38,2224,101192,2085,4309,0.51612903225806451613
-- 25,46,2270,105455,2039,4309,0.5268043629612439081
-- 26,40,2310,109724,1999,4309,0.53608725922487816199
-- 27,35,2345,113998,1964,4309,0.54420979345555813414
-- 28,35,2380,118272,1929,4309,0.55233232768623810629
-- 29,41,2421,122540,1888,4309,0.56184729635646321652
-- 30,30,2451,126819,1858,4309,0.56880946855418890694
-- 31,33,2484,131095,1825,4309,0.5764678579716871664
-- 32,23,2507,135381,1802,4309,0.58180552332327686238
-- 33,29,2536,139661,1773,4309,0.58853562311441169645
-- 34,28,2564,143942,1745,4309,0.59503365049895567417
-- 35,37,2601,148214,1708,4309,0.60362032954281735902
-- 36,21,2622,152502,1687,4309,0.60849385008122534231
-- 37,23,2645,156788,1664,4309,0.61383151543281503829
-- 38,30,2675,161067,1634,4309,0.62079368763054072871
-- 39,24,2699,165352,1610,4309,0.62636342538872128104
-- 40,24,2723,169637,1586,4309,0.63193316314690183337


alter table client_requests add column sentoption bool default False;

update client_requests
set sentoption = learn_target
from postprocess_model_006_client where postprocess_model_006_client.id = client_requests.id
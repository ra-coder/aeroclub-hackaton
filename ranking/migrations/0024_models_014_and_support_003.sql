drop table if exists postprocess_model_013_client;
create table postprocess_model_013_client
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
create index on postprocess_model_013_client (id);

insert into postprocess_model_013_client (id, request_id, predict, score, rank, fixed_predict, selectedvariant, learn_target)
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
           ) < 5) as fixed_predict,
       client_requests.selectedvariant,
       client_requests.selectedvariant
           or
       (
            rank() OVER (
                       PARTITION BY requestid
                       ORDER BY score DESC
                       )
            <= 6
        ) as learn_target
from client_requests
         join model_013_client predict on predict.id = client_requests.id;

with _tmp as (select rank,
                     count(*) as cnt
              from postprocess_model_013_client
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
-- 1,236,236,4073,4073,4309,0.05476908795544209793
-- 2,183,419,8199,3890,4309,0.09723833836156880947
-- 3,151,570,12357,3739,4309,0.13228127175678811789
-- 4,172,742,16494,3567,4309,0.17219772569041540961
-- 5,178,920,20625,3389,4309,0.21350661406358783941
-- 6,151,1071,24783,3238,4309,0.24854954745880714783
-- 7,117,1188,28975,3121,4309,0.27570201902993734045
-- 8,114,1302,33170,3007,4309,0.30215827338129496403
-- 9,92,1394,37387,2915,4309,0.32350893478765374797
-- 10,80,1474,41616,2835,4309,0.34207472731492225574
-- 11,92,1566,45833,2743,4309,0.36342538872128103968
-- 12,71,1637,50071,2672,4309,0.37990252958923184033
-- 13,63,1700,54317,2609,4309,0.39452309120445579021
-- 14,77,1777,58549,2532,4309,0.41239266651195172894
-- 15,77,1854,62781,2455,4309,0.43026224181944766767
-- 16,63,1917,67027,2392,4309,0.44488280343467161754
-- 17,62,1979,71274,2330,4309,0.45927129264330471107
-- 18,54,2033,75529,2276,4309,0.47180320259921095382
-- 19,67,2100,79771,2209,4309,0.48735205384079832908
-- 20,54,2154,84026,2155,4309,0.49988396379670457183
-- 21,49,2203,88286,2106,4309,0.51125551171965653284
-- 22,49,2252,92546,2057,4309,0.52262705964260849385
-- 23,49,2301,96806,2008,4309,0.53399860756556045486
-- 24,44,2345,101071,1964,4309,0.54420979345555813414
-- 25,42,2387,105338,1922,4309,0.55395683453237410072
-- 26,31,2418,109616,1891,4309,0.56115107913669064748
-- 27,41,2459,113884,1850,4309,0.57066604780691575772
-- 28,45,2504,118148,1805,4309,0.58110930610350429334
-- 29,36,2540,122421,1769,4309,0.58946391274077512184
-- 30,51,2591,126679,1718,4309,0.60129960547690879554
-- 31,31,2622,130957,1687,4309,0.60849385008122534231
-- 32,24,2646,135242,1663,4309,0.61406358783940589464
-- 33,34,2680,139517,1629,4309,0.62195404966349501044
-- 34,23,2703,143803,1606,4309,0.62729171501508470643
-- 35,34,2737,148078,1572,4309,0.63518217683917382223
-- 36,24,2761,152363,1548,4309,0.64075191459735437456
-- 37,32,2793,156640,1516,4309,0.64817823160826177767
-- 38,31,2824,160918,1485,4309,0.65537247621257832444
-- 39,24,2848,165203,1461,4309,0.66094221397075887677
-- 40,22,2870,169490,1439,4309,0.66604780691575771641
-- 41,30,2900,173769,1409,4309,0.67300997911348340682





--alter table client_requests add column sentoption bool default False;

update client_requests
set sentoption = learn_target
from postprocess_model_013_client where postprocess_model_013_client.id = client_requests.id
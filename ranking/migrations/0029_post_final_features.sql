drop table if exists request_features;
create table request_features
(
    request_id                           int primary key,
    has_intravelpolicy_variant           bool,
    has_intravelpolicy_variant_1_segment bool,
    min_price                            decimal(16, 2),
    has_not_economy_in_policy            bool,
    min_return_time                      int,
    min_to_time                          int,
    min_departure_diff_seconds           int,
    min_segments_count                   int,
    min_total_flight_time                int,
    departuredate_variability            int,
    flights_variability                  int
);
create index on request_features (request_id);


insert into request_features
(request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment, min_price, has_not_economy_in_policy,
 min_return_time, min_to_time, min_departure_diff_seconds, min_segments_count, min_total_flight_time,
 departuredate_variability, flights_variability)
select requestid                                                                               as request_id,
       bool_or(intravelpolicy)                                                                 as has_intravelpolicy_variant,
       bool_or(intravelpolicy and segmentcount = 1)                                            as has_intravelpolicy_variant_1_segment,
       min(amount)                                                                             as min_price,
       bool_or(class != 'E' and intravelpolicy)                                                as has_not_economy_in_policy,
       extract(epoch from min(returnarrivaldate - returndepatruredate))                        as min_return_time,
       extract(epoch from min(arrivaldate - departuredate))                                    as min_to_time,
       min(abs(EXTRACT(epoch FROM (requestdeparturedate - departuredate))))                    as min_departure_diff_seconds,
       min(segmentcount)                                                                       as min_segments_count,
       min(
                   extract(epoch from (arrivaldate - departuredate))::float
                   + (from_city.timezone - coalesce(return_city.timezone, to_city.timezone))::float * 3600
                   +
                   coalesce(extract(epoch from (returnarrivaldate - returndepatruredate)), 0)) as min_total_flight_time,
       count(distinct departuredate) as departuredate_variability,
       count(distinct fligtoption) as flights_variability

from agent_requests
         join agent_request_parsed_info pi on agent_requests.id = pi.agent_request_id
         join iata_codes from_iata on pi.to_departure_iata = from_iata.code
         join cities from_city on from_iata.city_id = from_city.id
         join iata_codes to_iata on pi.to_arrival_iata = to_iata.code
         join cities to_city on to_iata.city_id = to_city.id
         left join iata_codes return_iata on pi.return_arrival_iata = return_iata.code
         left join cities return_city on return_iata.city_id = return_city.id
group by requestid
;

drop table if exists agent_requests_features;
create table agent_requests_features
(
    id                                   int primary key references agent_requests,
    request_id                           int,
    has_intravelpolicy_variant           bool,
    has_intravelpolicy_variant_1_segment bool,
    min_price                            decimal(16, 2),
    price_diff                           decimal(16, 2),
    price_ratio                          float,
    has_not_economy_in_policy            bool,
    min_return_time                      int,
    return_time                          int,
    return_time_abs                      int,
    return_time_abs_ratio                float,
    min_to_time                          int,
    to_time                              int,
    to_time_abs                          int,
    to_time_abs_ratio                    float,
    departuredate_variability            int,
    min_departure_diff_seconds           int,
    departure_diff_seconds               int,
    client_has_travellergrade            bool,
    client_travellergrade                int,
    class_is_economy                     bool,
    class_is_business                    bool,
    min_segments_count                   int,
    segments_diff                        int,
    one_segment_trip                     bool,
    departure_hour                       int,
    arrival_hour                         int,
    return_departure_hour                int,
    return_arrival_hour                  int,
    total_flight_time                    int, -- to_time + return_time
    min_total_flight_time                int, -- to_time + return_time
    total_flight_ratio                   float,
    round_trip                           bool,
    operator_count                       int,
    operator_code                        varchar(2),
    is_international                     bool,
    timezone_diff                        decimal(6, 2),
    to_city_timezone                     decimal(6, 2),
    from_city_timezone                   decimal(6, 2),
    to_city_iatacode                     varchar(3),
    from_city_iatacode                   varchar(3),
    departure_week_day                   int,
    return_week_day                      int,
    request_before_x_days                 int,
    stay_x_days                          int,
    price_rank int,
    price_leg_rank int,
    duration_rank int,
    segments_rank int,
    client varchar(8),
    same_options_best_price bool default false,
    flights_variability int
);
create index on agent_requests_features (id);

insert into agent_requests_features (id, request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment,
                                     min_price, price_diff, price_ratio, has_not_economy_in_policy, min_return_time,
                                     return_time,
                                     return_time_abs,
                                     return_time_abs_ratio,
                                     min_to_time,
                                     to_time,
                                     to_time_abs,
                                     to_time_abs_ratio,
                                     departuredate_variability,
                                     min_departure_diff_seconds,
                                     departure_diff_seconds,
                                     client_has_travellergrade, client_travellergrade, class_is_economy,
                                     class_is_business, min_segments_count, segments_diff,
                                     one_segment_trip,
                                     departure_hour,
                                     arrival_hour,
                                     return_departure_hour,
                                     return_arrival_hour,
                                     total_flight_time, -- to_time + return_time
                                     min_total_flight_time, -- to_time + return_time
                                     total_flight_ratio,
                                     round_trip,
                                     operator_count,
                                     operator_code,
                                     is_international,
                                     timezone_diff,
                                     to_city_timezone,
                                     from_city_timezone,
                                     to_city_iatacode,
                                     from_city_iatacode,
                                     departure_week_day,
                                     return_week_day,
                                     request_before_x_days,
                                     stay_x_days,
                                     price_rank,
                                     price_leg_rank,
                                     duration_rank,
                                     segments_rank,
                                     client,
                                     flights_variability
                                     )
select agent_requests.id,
       rf.request_id,
       has_intravelpolicy_variant,
       has_intravelpolicy_variant_1_segment,
       min_price,
       amount - min_price,
       amount::float / NULLIF(min_price::float, 0)                                                        as price_ratio,
       has_not_economy_in_policy,
       min_return_time,
       extract(epoch from (returnarrivaldate - returndepatruredate))                                      as return_time,
       extract(epoch from (returnarrivaldate - returndepatruredate))
           + (to_city.timezone - return_city.timezone) *
             3600                                                                                         as return_time_abs,
       (min_return_time::float + (to_city.timezone - return_city.timezone) * 3600) /
       NULLIF((extract(epoch from (returnarrivaldate - returndepatruredate))::float +
               (to_city.timezone - return_city.timezone)::float * 3600)
           ,
              0)::float                                                                                   as return_time_abs_ratio,
       min_to_time,
       extract(epoch from (arrivaldate - departuredate))                                                  as to_time,
       extract(epoch from (arrivaldate - departuredate)) - (to_city.timezone - from_city.timezone) *
                                                           3600                                           as to_time_abs,
       (min_to_time::float - (to_city.timezone - from_city.timezone)::float * 3600)
           / NULLIF(
                   (extract(epoch from (arrivaldate - departuredate)))::float
                   - (to_city.timezone - from_city.timezone)::float * 3600
           , 0
           )
                                                                                                          as to_time_abs_ratio,
      departuredate_variability,
       min_departure_diff_seconds,
       EXTRACT(epoch FROM (requestdeparturedate - departuredate))                                         as departure_diff_seconds,
       travellergrade notnull                                                                             as client_has_travellergrade,
       coalesce(travellergrade, -1)                                                                       as client_travellergrade,
       class = 'E'                                                                                        as class_is_economy,
       class = 'B' or class = 'C'                                                                         as class_is_business,
       min_segments_count                                                                                 as min_segments_count,
       segmentcount - min_segments_count                                                                  as segments_diff,
       position('/' in searchroute) = 0                                                                   as one_segment_trip,
       extract(hour from departuredate)                                                                   as departure_hour,
       extract(hour from arrivaldate)                                                                     as arrival_hour,
       extract(hour from returndepatruredate)                                                             as return_departure_hour,
       extract(hour from returnarrivaldate)                                                               as return_arrival_hour,
       extract(epoch from (arrivaldate - departuredate))
           +
       coalesce(extract(epoch from (returnarrivaldate - returndepatruredate)), 0)
           - (from_city.timezone - coalesce(return_city.timezone, to_city.timezone)) *
             3600                                                                                         as total_flight_time,
       min_total_flight_time                                                                              as min_total_flight_time,
       (
               extract(epoch from (arrivaldate - departuredate))
               +
               coalesce(extract(epoch from (returnarrivaldate - returndepatruredate)), 0)
               +
               (from_city.timezone - coalesce(return_city.timezone, to_city.timezone)) * 3600
           ) /
       min_total_flight_time                                                                              as total_flight_ratio,
       pi.to_departure_iata = pi.return_arrival_iata                                                      as round_trip,
       first_flight_option_operator_count,
       case
           when first_flight_option_operator_code
               in ('SU', 'S7', 'EK', 'TK', 'DP', 'U6', 'UT', 'PC', 'EY', 'QR', 'FZ', 'MS', 'WZ', 'HY', '5N', 'DV', 'N4',
                   'A4', 'LH')
               then first_flight_option_operator_code
           else 'XX'
           end                                                                                            as operator_code,
       to_city.countrycode != from_city.countrycode                                                       as is_international,
       to_city.timezone - from_city.timezone                                                              as timezone_diff,
       to_city.timezone,
       from_city.timezone,
       case
           when to_city.iatacode
               in ('MOW', 'LED', 'IST', 'KJA', 'ALA', 'AER', 'VVO', 'IKT', 'DXB', 'GOJ', 'NQZ', 'SVX', 'KUF', 'OVB')
               then to_city.iatacode
           else 'XXX'
           end                                                                                            as from_city_iatacode,
       case
           when from_city.iatacode
               in ('MOW', 'LED', 'IST', 'KJA', 'ALA', 'AER', 'VVO', 'IKT', 'DXB', 'GOJ', 'NQZ', 'SVX', 'KUF', 'OVB')
               then from_city.iatacode
           else 'XXX'
           end                                                                                            as from_city_iatacode,
     extract(ISODOW from departuredate) as departure_week_day,
     extract(ISODOW from returnarrivaldate) as return_week_day,
     extract(days from(requestdeparturedate - requestdate)) as request_befox_x_days,
     extract(days from(requestreturndate - requestdeparturedate)) as stay_x_days,
    rank() OVER (
       PARTITION BY requestid
       ORDER BY (amount)
       ) as  price_rank,
    rank() OVER (
       PARTITION BY requestid
       ORDER BY (segmentcount, amount)
       ) as  price_leg_rank,
    rank() OVER (
       PARTITION BY requestid
       ORDER BY (coalesce(extract(epoch from (returnarrivaldate - returndepatruredate)), 0)
           - (from_city.timezone - coalesce(return_city.timezone, to_city.timezone)) *
             3600 )
       ) as duration_rank,
    rank() over (
        partition by requestid order by segmentcount
        )                                    segments_rank,
       case when clientid in (
            26390,
            54545,
            45001,
            45230,
            2519,
            40811,
            45112,
            2175
        ) then clientid::varchar
        else 'XX'
     end as client,
    flights_variability
from agent_requests
         join request_features rf on agent_requests.requestid = rf.request_id
         join agent_request_parsed_info pi on agent_requests.id = pi.agent_request_id
         join iata_codes from_iata on pi.to_departure_iata = from_iata.code
         join cities from_city on from_iata.city_id = from_city.id
         join iata_codes to_iata on pi.to_arrival_iata = to_iata.code
         join cities to_city on to_iata.city_id = to_city.id
         left join iata_codes return_iata on pi.return_arrival_iata = return_iata.code
         left join cities return_city on return_iata.city_id = return_city.id
;

---------

with _chippest_with_same_patams as (select count(*)      as cnt,
                                           min(amount)   as chippest_option_price,
                                           array_agg(id) as variant_ids,
                                           RequestID,
                                           RequestDate,
                                           ClientID,
                                           TravellerGrade,
                                           SearchRoute,
                                           RequestDepartureDate,
                                           RequestReturnDate,
                                           FligtOption,
                                           DepartureDate,
                                           ArrivalDate,
                                           ReturnDepatrureDate,
                                           ReturnArrivalDate,
                                           SegmentCount,
                                           "class",
                                           IsBaggage,
                                           isRefundPermitted,
                                           isExchangePermitted
                                           --isDiscount           bool,
                                    from agent_requests
                                    group by RequestID, RequestDate, ClientID, TravellerGrade, SearchRoute,
                                             RequestDepartureDate, RequestReturnDate,
                                             FligtOption, DepartureDate, ArrivalDate, ReturnDepatrureDate,
                                             ReturnArrivalDate, SegmentCount, "class",
                                             IsBaggage, isRefundPermitted, isExchangePermitted
                                    order by cnt desc)
update agent_requests_features
set same_options_best_price = True
from _chippest_with_same_patams
where id = any (_chippest_with_same_patams.variant_ids)
  and (price_diff + min_price) = chippest_option_price;


----------

select count(distinct requestid)                                                                      as requests_count,
       count(distinct requestid)
       filter (where sentoption = true and same_options_best_price = FALSE)                           as requests_with_erors,
       count(*) filter (where sentoption = true and same_options_best_price = FALSE)                  as erors,
       employeeid
from agent_requests_features
         join agent_requests ar on agent_requests_features.id = ar.id
group by employeeid;


-- requests_count,requests_with_erors,erors,employeeid
-- 1221,142,210,3498
-- 328,75,125,5106
-- 367,59,96,5007
-- 263,56,79,4776
-- 416,33,44,1524
-- 95,29,72,2137
-- 103,27,49,1845

select
    ar.*,
    same_options_best_price
from agent_requests_features
join agent_requests ar on agent_requests_features.id = ar.id
where  employeeid=2137
  --and same_options_best_price=FALSE and sentoption=True
    and fligtoption ilike 'SU0025 LEDSVO 2022.07.08'
order by requestid, fligtoption, sentoption, amount

alter table agent_requests
    add column sentoption_fixed bool default false;

update agent_requests
set sentoption_fixed = True
from (select distinct ar1.id as selected_id,
                      ar2.id as fixed_id
      from agent_requests ar1
               join agent_requests ar2
                    on
                                ar2.fligtoption = ar1.fligtoption
                            and ar2.requestid = ar1.requestid
                            and ar1.class = ar2.class
                            and ar1.isbaggage = ar2.isbaggage
                            and ar1.isrefundpermitted =
                                ar2.isrefundpermitted
                            and ar1.isexchangepermitted =
                                ar2.isexchangepermitted
               join agent_requests_features f1 on ar1.id = f1.id
               join agent_requests_features f2 on ar2.id = f2.id
      where ar1.sentoption = True
        and f2.same_options_best_price = True) as _tmp
where _tmp.fixed_id = id;

select
    count(*) filter ( where sentoption_fixed = true) as fixed,
    count(*) filter ( where sentoption = true) as raw
from agent_requests;


select distinct ar1.id as selected_id,
                      ar2.id as fixed_id,
                      f2.same_options_best_price
      from agent_requests ar1
               join agent_requests ar2
                    on
                                ar2.fligtoption = ar1.fligtoption
                            and ar2.requestid = ar1.requestid
                            and ar1.class = ar2.class
                            and ar1.isbaggage = ar2.isbaggage
                            and ar1.isrefundpermitted =
                                ar2.isrefundpermitted
                            and ar1.isexchangepermitted =
                                ar2.isexchangepermitted
               join agent_requests_features f1 on ar1.id = f1.id
               join agent_requests_features f2 on ar2.id = f2.id
      where ar1.sentoption = True and ar1.requestid=4193603;
        --and f2.same_options_best_price = True and

select distinct
    clientid,
    count(distinct requestid) as cnt
from agent_requests
group by clientid
order by cnt desc;

select count(*) from agent_requests_features;

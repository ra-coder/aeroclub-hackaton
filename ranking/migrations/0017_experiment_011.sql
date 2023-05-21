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
    min_total_flight_time                int
);
create index on request_features (request_id);


insert into request_features
(request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment, min_price, has_not_economy_in_policy,
 min_return_time, min_to_time, min_departure_diff_seconds, min_segments_count, min_total_flight_time)
select requestid                                                            as request_id,
       bool_or(intravelpolicy)                                              as has_intravelpolicy_variant,
       bool_or(intravelpolicy and segmentcount = 1)                         as has_intravelpolicy_variant_1_segment,
       min(amount)                                                          as min_price,
       bool_or(class != 'E' and intravelpolicy)                             as has_not_economy_in_policy,
       extract(epoch from min(returnarrivaldate - returndepatruredate))     as min_return_time,
       extract(epoch from min(arrivaldate - departuredate))                 as min_to_time,
       min(abs(EXTRACT(epoch FROM (requestdeparturedate - departuredate)))) as min_departure_diff_seconds,
       min(segmentcount)                                                    as min_segments_count,
       min(
           extract(epoch from (arrivaldate - departuredate))
            +
        extract(epoch from (returnarrivaldate - returndepatruredate))) as  min_total_flight_time

from agent_requests
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
    return_time_ratio                    float, -- bad since not utc
    min_to_time                          int,
    to_time                              int,
    to_time_ratio                        float, -- bad since not utc
    min_departure_diff_seconds           int,
    departure_diff_seconds               int,
    client_has_travellergrade            bool,
    client_travellergrade                int,
    class_is_economy                     bool,
    class_is_business                    bool,
    min_segments_count                   int,
    segments_diff                        int,
    one_segment_trip                           bool,
    departure_hour                       int,
    arrival_hour                         int,
    return_departure_hour                int,
    return_arrival_hour                  int,
    total_flight_time                    int,   -- to_time + return_time
    min_total_flight_time                int,   -- to_time + return_time
    total_flight_ratio                   float,
     round_trip bool,
    operator_count int,
    operator_code varchar(2),
    is_international bool,
    timezone_diff decimal(6,2),
    to_city_timezone decimal(6,2),
    from_city_timezone decimal(6,2)
);
create index on agent_requests_features (id);

insert into agent_requests_features (id, request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment,
                                     min_price, price_diff, price_ratio, has_not_economy_in_policy, min_return_time,
                                     return_time, return_time_ratio,
                                     min_to_time, to_time, to_time_ratio, min_departure_diff_seconds,
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
                                     from_city_timezone
                                     )
select agent_requests.id,
       rf.request_id,
       has_intravelpolicy_variant,
       has_intravelpolicy_variant_1_segment,
       min_price,
       amount - min_price,
       amount::float / NULLIF(min_price::float, 0)                                              as price_ratio,
       has_not_economy_in_policy,
       min_return_time,
       extract(epoch from (returnarrivaldate - returndepatruredate))                            as return_time,
       min_return_time::float /
       NULLIF(extract(epoch from (returnarrivaldate - returndepatruredate)), 0)::float          as return_time_ratio,
       min_to_time,
       extract(epoch from (arrivaldate - departuredate))                                        as to_time,
       min_to_time::float / NULLIF(extract(epoch from (arrivaldate - departuredate)), 0)::float as to_time_ratio,
       min_departure_diff_seconds,
       EXTRACT(epoch FROM (requestdeparturedate - departuredate))                               as departure_diff_seconds,
       travellergrade notnull                                                                   as client_has_travellergrade,
       coalesce(travellergrade, -1)                                                             as client_travellergrade,
       class = 'E'                                                                              as class_is_economy,
       class = 'B' or class = 'C'                                                               as class_is_business,
       min_segments_count                                                                       as min_segments_count,
       segmentcount - min_segments_count                                                        as segments_diff,
       position('/' in searchroute) = 0                                                         as one_segment_trip,
       extract(hour from departuredate)                                                         as departure_hour,
       extract(hour from arrivaldate)                                                           as arrival_hour,
       extract(hour from returndepatruredate)                                                   as return_departure_hour,
       extract(hour from returnarrivaldate)                                                     as return_arrival_hour,
      extract(epoch from (arrivaldate - departuredate))
        +
      extract(epoch from (returnarrivaldate - returndepatruredate))                             as  total_flight_time,
      min_total_flight_time,
      extract(epoch from (arrivaldate - departuredate))
        +
      extract(epoch from (returnarrivaldate - returndepatruredate)) / min_total_flight_time      as total_flight_ratio,
      pi.to_departure_iata = pi.return_arrival_iata as round_trip,
      first_flight_option_operator_count,
      case when first_flight_option_operator_code
          in ('SU', 'S7', 'EK', 'TK', 'DP', 'U6', 'UT', 'PC', 'EY', 'QR', 'FZ', 'MS', 'WZ', 'HY', '5N', 'DV', 'N4', 'A4', 'LH')
          then first_flight_option_operator_code
      else 'XX'
      end  as  operator_code,
      to_city.countrycode != from_city.countrycode as is_international,
      to_city.timezone - from_city.timezone as timezone_diff,
      to_city.timezone,
      from_city.timezone
from agent_requests
         join request_features rf on agent_requests.requestid = rf.request_id
         join agent_request_parsed_info pi on agent_requests.id = pi.agent_request_id
         join iata_codes from_iata on pi.to_departure_iata = from_iata.code
         join cities from_city on from_iata.city_id = from_city.id
         join iata_codes to_iata on pi.to_arrival_iata = to_iata.code
         join cities to_city on to_iata.city_id = to_city.id
;

---------

-- drop table if exists request_features_c;
-- create table request_features_c
-- (
--     request_id                           int primary key,
--     has_intravelpolicy_variant           bool,
--     has_intravelpolicy_variant_1_segment bool,
--     min_price                            decimal(16, 2),
--     has_not_economy_in_policy            bool,
--     min_return_time                      int,
--     min_to_time                          int,
--     min_departure_diff_seconds           int,
--     min_segments_count                   int,
--     min_total_flight_time                int
-- );
-- create index on request_features_c (request_id);
--
--
-- insert into request_features_c
-- (request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment, min_price, has_not_economy_in_policy,
--  min_return_time, min_to_time, min_departure_diff_seconds, min_segments_count, min_total_flight_time)
-- select requestid                                                            as request_id,
--        bool_or(intravelpolicy)                                              as has_intravelpolicy_variant,
--        bool_or(intravelpolicy and segmentcount = 1)                         as has_intravelpolicy_variant_1_segment,
--        min(amount)                                                          as min_price,
--        bool_or(class != 'E' and intravelpolicy)                             as has_not_economy_in_policy,
--        extract(epoch from min(returnarrivaldate - returndepatruredate))     as min_return_time,
--        extract(epoch from min(arrivadate - departuredate))                 as min_to_time,
--        min(abs(EXTRACT(epoch FROM (requestdeparturedate - departuredate)))) as min_departure_diff_seconds,
--        min(segmentcount)                                                    as min_segments_count,
--        min(
--            extract(epoch from (arrivadate - departuredate))
--             +
--         extract(epoch from (returnarrivaldate - returndepatruredate))) as  min_total_flight_time
-- from client_requests
-- group by requestid
-- ;
--
-- drop table if exists client_requests_features;
-- create table client_requests_features
-- (
--     id                                   int primary key references client_requests,
--     request_id                           int,
--     has_intravelpolicy_variant           bool,
--     has_intravelpolicy_variant_1_segment bool,
--     min_price                            decimal(16, 2),
--     price_diff                           decimal(16, 2),
--     price_ratio                          float,
--     has_not_economy_in_policy            bool,
--     min_return_time                      int,
--     return_time                          int,
--     return_time_ratio                    float,
--     min_to_time                          int,
--     to_time                              int,
--     to_time_ratio                        float,
--     min_departure_diff_seconds           int,
--     departure_diff_seconds               int,
--     client_has_travellergrade            bool,
--     client_travellergrade                int,
--     class_is_economy                     bool,
--     class_is_business                    bool,
--     min_segments_count                   int,
--     segments_diff                        int,
--     round_trip                           bool,
--     departure_hour                       int,
--     arrival_hour                         int,
--     return_departure_hour                int,
--     return_arrival_hour                  int,
--     total_flight_time                    int,   -- to_time + return_time
--     min_total_flight_time                int,   -- to_time + return_time
--     total_flight_ratio                   float
-- );
-- create index on client_requests_features (id);
--
-- insert into client_requests_features (id, request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment,
--                                      min_price, price_diff, price_ratio, has_not_economy_in_policy, min_return_time,
--                                      return_time, return_time_ratio,
--                                      min_to_time, to_time, to_time_ratio, min_departure_diff_seconds,
--                                      departure_diff_seconds,
--                                      client_has_travellergrade, client_travellergrade, class_is_economy,
--                                      class_is_business, min_segments_count, segments_diff,
--                                      round_trip,
--                                      departure_hour,
--                                      arrival_hour,
--                                      return_departure_hour,
--                                      return_arrival_hour,
--                                      total_flight_time, -- to_time + return_time
--                                      min_total_flight_time, -- to_time + return_time
--                                      total_flight_ratio)
-- select id,
--        request_id,
--        has_intravelpolicy_variant,
--        has_intravelpolicy_variant_1_segment,
--        min_price,
--        amount - min_price,
--        amount::float / NULLIF(min_price::float, 0)                                                   as price_ratio,
--        has_not_economy_in_policy,
--        min_return_time,
--        extract(epoch from (returnarrivaldate - returndepatruredate))                                 as return_time,
--        min_return_time::float /
--        NULLIF(extract(epoch from (returnarrivaldate - returndepatruredate)), 0)::float                          as return_time_ratio,
--        min_to_time,
--        extract(epoch from (arrivadate - departuredate))                                             as to_time,
--        min_to_time::float / NULLIF(extract(epoch from (arrivadate - departuredate)), 0)::float                 as to_time_ratio,
--        min_departure_diff_seconds,
--        EXTRACT(epoch FROM (requestdeparturedate - departuredate))                                    as departure_diff_seconds,
--        null notnull                                                                        as client_has_travellergrade,  -- TODO exclude or ask
--        coalesce(null, -1)                                                                  as client_travellergrade,  -- TODO exclude or ask
--        class = 'E'                                                                                   as class_is_economy,
--        class = 'B' or class = 'C'                                                                    as class_is_business,
--        min_segments_count                                                                            as min_segments_count,
--        segmentcount - min_segments_count                                                             as segments_diff,
--        position('/' in searchroute) > 0                                                              as round_trip,
--        extract(hour from departuredate)                                                         as departure_hour,
--        extract(hour from arrivadate)                                                           as arrival_hour,
--        extract(hour from returndepatruredate)                                                   as return_departure_hour,
--        extract(hour from returnarrivaldate)                                                     as return_arrival_hour,
--       extract(epoch from (arrivadate - departuredate))
--         +
--       extract(epoch from (returnarrivaldate - returndepatruredate))                             as  total_flight_time,
--       min_total_flight_time,
--       extract(epoch from (arrivadate - departuredate))
--         +
--       extract(epoch from (returnarrivaldate - returndepatruredate)) / min_total_flight_time      as total_flight_ratio
-- from client_requests
--          join request_features_c rf on client_requests.requestid = rf.request_id;


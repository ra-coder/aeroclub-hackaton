create table final_test_requests
(
    ID int primary key,
    RequestID            int,
    EmployeeId           int,
    RequestDate          timestamp,
    ClientID             int,
    TravellerGrade       int, --TravellerGrade
    SearchRoute          text,
    RequestDepartureDate timestamp,
    RequestReturnDate    timestamp,
    FligtOption          text,
    DepartureDate        timestamp,
    ArrivalDate          timestamp,
    ReturnDepatrureDate  timestamp,
    ReturnArrivalDate    timestamp,
    SegmentCount         int,
    Amount               numeric(16, 2),
    "class"                varchar,
    IsBaggage            bool,
    isRefundPermitted    bool,
    isExchangePermitted  bool,
    isDiscount           bool,
    InTravelPolicy       bool,
    rank int             --Position ( from 1 to n)
);
create index on final_test_requests (id);
create index on final_test_requests (RequestID);


drop table if exists final_test_request_parsed_info;

create table final_test_request_parsed_info
(
    final_test_request_id                   int primary key references final_test_requests,
    request_id                         int,
    to_departure_iata                  varchar(3) references iata_codes,
    to_arrival_iata                    varchar(3) references iata_codes,
    return_departure_iata              varchar(3) references iata_codes,
    return_arrival_iata                varchar(3) references iata_codes,
    flightoption_start_iata            varchar(3) references iata_codes,
    flightoption_end_iata              varchar(3) references iata_codes,
    flight_option_operator_codes       varchar[],
    first_flight_option_operator_code  varchar(2),
    first_flight_option_operator_count int
);
create index on final_test_request_parsed_info (final_test_request_id);


insert into final_test_request_parsed_info(final_test_request_id, request_id, to_departure_iata, to_arrival_iata,
                                      return_departure_iata, return_arrival_iata, flightoption_start_iata,
                                      flightoption_end_iata, flight_option_operator_codes,
                                      first_flight_option_operator_code, first_flight_option_operator_count)
with _tmp as (select id,
                     requestid                                                        as request_id,
                     SUBSTRING(searchroute, 1, 3)                                     as to_departure_iata,
                     SUBSTRING(searchroute, 4, 3)                                     as to_arrival_iata,
                     NULLIF(SUBSTRING(searchroute, 8, 3), '')                         as return_departure_iata,
                     NULLIF(SUBSTRING(searchroute, 11, 3), '')                        as return_arrival_iata,

                     substring(fligtoption, 8, 3)                                     as flightoption_start_iata,
                     substring("right"(fligtoption, 14), 1, 3)                        as flightoption_end_iata,
                     SUBSTRING(unnest(regexp_split_to_array(fligtoption, '/')), 1, 2) as operator_code

              from final_test_requests)
select id,
       request_id,
       to_departure_iata,
       to_arrival_iata,
       return_departure_iata,
       return_arrival_iata,

       flightoption_start_iata,
       flightoption_end_iata,
       array_agg(distinct operator_code)                         as flight_option_operator_codes,
       (mode() within group ( order by operator_code ))::varchar as first_flight_option_operator_code,
       array_length(array_agg(distinct operator_code), 1)        as first_flight_option_operator_count
from _tmp
group by id, request_id, flightoption_end_iata, flightoption_start_iata,
         to_departure_iata,
         to_arrival_iata,
         return_departure_iata,
         return_arrival_iata;


drop table if exists request_features_ft;
create table request_features_ft
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
create index on request_features_ft (request_id);


insert into request_features_ft
(request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment, min_price, has_not_economy_in_policy,
 min_return_time, min_to_time, min_departure_diff_seconds, min_segments_count, min_total_flight_time)
select requestid                                                            as request_id,
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
                   coalesce(extract(epoch from (returnarrivaldate - returndepatruredate)), 0)) as min_total_flight_time

from final_test_requests
         join final_test_request_parsed_info pi on final_test_requests.id = pi.final_test_request_id
         join iata_codes from_iata on pi.to_departure_iata = from_iata.code
         join cities from_city on from_iata.city_id = from_city.id
         join iata_codes to_iata on pi.to_arrival_iata = to_iata.code
         join cities to_city on to_iata.city_id = to_city.id
         left join iata_codes return_iata on pi.return_arrival_iata = return_iata.code
         left join cities return_city on return_iata.city_id = return_city.id
group by requestid
;

drop table if exists final_test_requests_features;
create table final_test_requests_features
(
    id                                   int primary key references final_test_requests,
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
    duration_rank int,
    segments_rank int
);
create index on final_test_requests_features (id);

insert into final_test_requests_features (id, request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment,
                                     min_price, price_diff, price_ratio, has_not_economy_in_policy, min_return_time,
                                     return_time,
                                     return_time_abs,
                                     return_time_abs_ratio,
                                     min_to_time,
                                     to_time,
                                     to_time_abs,
                                     to_time_abs_ratio, min_departure_diff_seconds,
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
                                     duration_rank,
                                     segments_rank
                                     )
select final_test_requests.id,
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
       min_departure_diff_seconds,
       EXTRACT(epoch FROM (requestdeparturedate - departuredate))                                         as departure_diff_seconds,
--        travellergrade notnull                                                                             as client_has_travellergrade,
--        coalesce(travellergrade, -1)                                                                       as client_travellergrade,
       false                                                                             as client_has_travellergrade,
       -1                                                                       as client_travellergrade,
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
       ORDER BY (coalesce(extract(epoch from (returnarrivaldate - returndepatruredate)), 0)
           - (from_city.timezone - coalesce(return_city.timezone, to_city.timezone)) *
             3600 )
       ) as duration_rank,
    rank() over (
        partition by requestid order by segmentcount
        )                                    segments_rank

from final_test_requests
         join request_features_ft rf on final_test_requests.requestid = rf.request_id
         join final_test_request_parsed_info pi on final_test_requests.id = pi.final_test_request_id
         join iata_codes from_iata on pi.to_departure_iata = from_iata.code
         join cities from_city on from_iata.city_id = from_city.id
         join iata_codes to_iata on pi.to_arrival_iata = to_iata.code
         join cities to_city on to_iata.city_id = to_city.id
         left join iata_codes return_iata on pi.return_arrival_iata = return_iata.code
         left join cities return_city on return_iata.city_id = return_city.id
;


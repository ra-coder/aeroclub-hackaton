drop table agent_request_parsed_info;

create table agent_request_parsed_info
(
    agent_request_id                   int primary key references agent_requests,
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
create index on agent_request_parsed_info (agent_request_id);


insert into agent_request_parsed_info(agent_request_id, request_id, to_departure_iata, to_arrival_iata,
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

              from agent_requests)
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
-- 637,402 rows affected in 39 s 477 ms

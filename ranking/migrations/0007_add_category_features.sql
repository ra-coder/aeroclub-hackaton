select distinct travellergrade
from agent_requests;
-- travellergrade
--
-- 1
-- 3
-- 5
-- 4
-- 2

select distinct "class"
from agent_requests;

-- class
-- B
-- C
-- E

drop table if exists agent_requests_features;
create table agent_requests_features
(
    id                                   int primary key references agent_requests,
    request_id                           int,
    has_intravelpolicy_variant           bool,
    has_intravelpolicy_variant_1_segment bool,
    min_price                            decimal(16, 2),
    price_diff                           decimal(16, 2),
    has_not_economy_in_policy            bool,
    min_return_time                      int,
    return_time                          int,
    min_to_time                          int,
    to_time                              int,
    min_departure_diff_seconds           int,
    departure_diff_seconds               int,
    client_has_travellergrade            bool,
    client_travellergrade                int,
    class_is_economy                     bool,
    class_is_business                    bool
);
create index on agent_requests_features (id);

insert into agent_requests_features (id, request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment,
                                     min_price, price_diff, has_not_economy_in_policy, min_return_time, return_time,
                                     min_to_time, to_time, min_departure_diff_seconds, departure_diff_seconds,
                                     client_has_travellergrade, client_travellergrade, class_is_economy,
                                     class_is_business)
select id,
       request_id,
       has_intravelpolicy_variant,
       has_intravelpolicy_variant_1_segment,
       min_price,
       amount - min_price,
       has_not_economy_in_policy,
       min_return_time,
       extract(epoch from (returnarrivaldate - returndepatruredate)) as return_time,
       min_to_time,
       extract(epoch from (arrivaldate - departuredate))             as to_time,
       min_departure_diff_seconds,
       EXTRACT(epoch FROM (requestdeparturedate - departuredate))    as departure_diff_seconds,
       travellergrade notnull                                        as client_has_travellergrade,
       coalesce(travellergrade, -1)                                  as client_travellergrade,
       class = 'E'                                                   as class_is_economy,
       class = 'B' or class = 'C'                                    as class_is_business

from agent_requests
         join request_features rf on agent_requests.requestid = rf.request_id;
create table request_features
(
    request_id                           int primary key,
    has_intravelpolicy_variant           bool,
    has_intravelpolicy_variant_1_segment bool,
    min_price                            decimal(16, 2),
    has_not_economy_in_policy            bool,
    min_return_time                      int,
    min_to_time                          int,
    min_departure_diff_seconds           int
);
create index on request_features (request_id);


insert into request_features
(request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment, min_price, has_not_economy_in_policy,
 min_return_time, min_to_time, min_departure_diff_seconds)
select requestid                                                            as request_id,
       bool_or(intravelpolicy)                                              as has_intravelpolicy_variant,
       bool_or(intravelpolicy and segmentcount = 1)                         as has_intravelpolicy_variant_1_segment,
       min(amount)                                                          as min_price,
       bool_or(class != 'E' and intravelpolicy)                             as has_not_economy_in_policy,
       extract(epoch from min(returnarrivaldate - returndepatruredate))     as min_return_time,
       extract(epoch from min(arrivaldate - departuredate))                 as min_to_time,
       min(abs(EXTRACT(epoch FROM (requestdeparturedate - departuredate)))) as min_departure_diff_seconds
from agent_requests
group by requestid
;

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
    departure_diff_seconds               int
);
create index on agent_requests_features (id);

insert into agent_requests_features (id, request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment,
                                     min_price, price_diff, has_not_economy_in_policy, min_return_time, return_time,
                                     min_to_time, to_time, min_departure_diff_seconds, departure_diff_seconds)
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
       EXTRACT(epoch FROM (requestdeparturedate - departuredate))    as departure_diff_seconds
from agent_requests
         join request_features rf on agent_requests.requestid = rf.request_id;
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
    min_segments_count                   int
);
create index on request_features (request_id);


insert into request_features
(request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment, min_price, has_not_economy_in_policy,
 min_return_time, min_to_time, min_departure_diff_seconds, min_segments_count)
select requestid                                                            as request_id,
       bool_or(intravelpolicy)                                              as has_intravelpolicy_variant,
       bool_or(intravelpolicy and segmentcount = 1)                         as has_intravelpolicy_variant_1_segment,
       min(amount)                                                          as min_price,
       bool_or(class != 'E' and intravelpolicy)                             as has_not_economy_in_policy,
       extract(epoch from min(returnarrivaldate - returndepatruredate))     as min_return_time,
       extract(epoch from min(arrivaldate - departuredate))                 as min_to_time,
       min(abs(EXTRACT(epoch FROM (requestdeparturedate - departuredate)))) as min_departure_diff_seconds,
       min(segmentcount)                                                    as min_segments_count
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
    class_is_business                    bool,
    min_segments_count                   int,
    segments_diff                        int
);
create index on agent_requests_features (id);

insert into agent_requests_features (id, request_id, has_intravelpolicy_variant, has_intravelpolicy_variant_1_segment,
                                     min_price, price_diff, has_not_economy_in_policy, min_return_time, return_time,
                                     min_to_time, to_time, min_departure_diff_seconds, departure_diff_seconds,
                                     client_has_travellergrade, client_travellergrade, class_is_economy,
                                     class_is_business, min_segments_count, segments_diff)
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
       class = 'B' or class = 'C'                                    as class_is_business,
       min_segments_count                                            as min_segments_count,
       segmentcount - min_segments_count                             as segments_diff
from agent_requests
         join request_features rf on agent_requests.requestid = rf.request_id;
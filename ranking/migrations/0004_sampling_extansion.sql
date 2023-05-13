CREATE EXTENSION tsm_system_rows;

SELECT *
FROM agent_requests TABLESAMPLE SYSTEM_ROWS(100);

select count(*)
from requests
where has_agent_data = True;
-- 10139

create index on agent_requests (id);
create index on agent_requests (requestid);

drop table agent_requests_sample_001;
create table agent_requests_sample_001
(
    id         int primary key references agent_requests,
    request_id int,
    for_test   bool
);
create index on agent_requests_sample_001 (id);

insert into agent_requests_sample_001 (id, request_id, for_test)
with _sample as (SELECT requests.id
                 FROM requests TABLESAMPLE SYSTEM_ROWS(1000)
                 where has_agent_data = True)
SELECT agent_requests.id,
       requestid           as request_id,
       _sample.id is not null as for_test
from agent_requests
         left join _sample on agent_requests.requestid = _sample.id;
;
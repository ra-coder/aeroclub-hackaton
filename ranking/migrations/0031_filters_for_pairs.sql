alter table agent_requests
    add column sentoption_flight bool default false;

with _sent as (select distinct requestid,
                               fligtoption
               from agent_requests
               where sentoption = True
               )
update agent_requests set sentoption_flight = True
from _sent where agent_requests.fligtoption = _sent.fligtoption and _sent.requestid=agent_requests.requestid;

select count(*) from agent_requests group by sentoption_flight;
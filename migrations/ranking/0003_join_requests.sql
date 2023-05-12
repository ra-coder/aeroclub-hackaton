create table requests (
    id int primary key,
    has_agent_data bool default false,
    has_client_data bool default false
);

insert into requests (id, has_agent_data)
select
    requestid as id,
    true as has_agent_data
from agent_requests
on conflict do nothing;


insert into requests (id, has_client_data)
select distinct
    requestid,
    true
from client_requests
on conflict (id) do update set has_client_data=true;

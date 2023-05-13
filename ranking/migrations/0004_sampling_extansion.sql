CREATE EXTENSION tsm_system_rows;

SELECT * FROM agent_requests TABLESAMPLE SYSTEM_ROWS(100);

with _sample as(
    SELECT requests.id
    FROM requests TABLESAMPLE SYSTEM_ROWS(100)
    where has_agent_data = True
)
SELECT * from _sample
    join agent_requests on agent_requests.requestid = _sample.id;

import pandas as pd
import sshtunnel
from sqlalchemy import create_engine

with sshtunnel.open_tunnel(
        ('84.252.142.119', 22),
        ssh_username="ra-coder",
        ssh_pkey="../../../.ssh/ra-coder_ed.ppk",
        remote_bind_address=('localhost', 5432),
        local_bind_address=('localhost', 5432)
) as server:
    engine = create_engine(f'postgresql://coder:coder@localhost:{server.local_bind_port}/ranking')
    data = pd.read_sql("SELECT * FROM agent_requests LIMIT 10;", engine)
    print(data)

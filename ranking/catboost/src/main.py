import logging

import sshtunnel
from sqlalchemy import create_engine

from model_002_add_features_catboost import NaiveCatboostTrainFlow2

logging.getLogger().setLevel(logging.INFO)


if __name__ == '__main__':
    with sshtunnel.open_tunnel(
            ('84.252.142.119', 22),
            ssh_username="ra-coder",
            ssh_pkey="../../../.ssh/ra-coder_ed.ppk",
            remote_bind_address=('localhost', 5432),
            local_bind_address=('localhost', 5432)
    ) as server:
        engine = create_engine(f'postgresql://coder:coder@localhost:{server.local_bind_port}/ranking')
        logging.info('START')

        train_flow = NaiveCatboostTrainFlow2(db_engine=engine, sampling_table_name='agent_requests_sample_001')
        data = train_flow.prepare_features(filter_for_test=True)  #, limit=10000)
        train_flow.learn(data)
        train_flow.save_model()
        # train_flow.load_model()
        train_flow.apply_model_in_db()

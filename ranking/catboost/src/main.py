import logging

import sshtunnel
from sqlalchemy import create_engine

from model_006 import NaiveCatboostTrainFlow6 as PrevTrainFlow
from support_model_001_on_006 import SupportModelCatboost1 as SupportTrainFlow
from model_007_with_support_score import CatBoostWithSupportScoresTrainFlow7 as TrainFlow
logging.getLogger().setLevel(logging.INFO)


def learn_on_agent_requests():
    train_flow = TrainFlow(db_engine=engine, sampling_table_name='agent_requests_sample_001')

    # data = train_flow.prepare_features(filter_for_test=True, limit=10000)
    # train_flow.learn(data)

    data = train_flow.prepare_features(filter_for_test=True)
    train_flow.learn(data)
    train_flow.save_model()
    # train_flow.load_model()
    train_flow.apply_model_in_db()


def learn_on_client_requests():
    prev_train_flow = PrevTrainFlow(db_engine=engine, sampling_table_name='agent_requests_sample_001')
    prev_train_flow.load_model()
    prev_train_flow.apply_model_in_db(to_client=True)

    # Some sQL from  0012_learn_on_extended_client_requests.sql TODO move to code

    support_train_flow = SupportTrainFlow(db_engine=engine)
    data = support_train_flow.prepare_features(table_prefix='client')  # , limit=30000)
    support_train_flow.learn(data)

    support_train_flow.save_model()
    # train_flow.load_model()
    support_train_flow.apply_model_in_db(to_client=False)


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

        # learn_on_client_requests()

        learn_on_agent_requests()

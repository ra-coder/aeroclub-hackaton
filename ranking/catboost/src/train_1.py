import logging
import pandas as pd
import sshtunnel
from catboost import CatBoostClassifier
from sklearn.model_selection import train_test_split
from sqlalchemy import create_engine

logging.getLogger().setLevel(logging.INFO)


def prepare_simple_train(train_data: pd.DataFrame):
    target = ['sentoption']
    category_features = []  # ['travellergrade', 'class']
    num_features = ['segmentcount', 'amount']
    bool_features = [
        'isbaggage',
        'isrefundpermitted',
        'isexchangepermitted',
        'isdiscount',
        'intravelpolicy',
    ]
    used = target + category_features + num_features + bool_features
    predictors = category_features + num_features + bool_features
    prepared_data = train_data.drop(columns=[col for col in train_data.columns if col not in used])
    return prepared_data, target, predictors


def train_catboost(train_data: pd.DataFrame, target: list[str], predictors: list[str]):
    X_train, X_test, y_train, y_test = train_test_split(
        train_data[predictors],
        train_data[target],
        test_size=0.2,
        random_state=41,
    )

    # Prepare model
    model = CatBoostClassifier(iterations=50, eval_metric='Accuracy', verbose=True)
    # Fit model
    model.fit(X_train[predictors], y_train, eval_set=(X_test, y_test))
    return model


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
        train_data = pd.read_sql("SELECT * FROM agent_requests; -- LIMIT 300000;", engine)
        logging.info('READ DONE')
        prepared_data, target, predictors = prepare_simple_train(train_data)
        logging.info('PREPARE DONE')
        model = train_catboost(prepared_data, target, predictors)
        logging.info('TRAIN DONE')
        model.save_model("model_1_naive")


# bestTest = 0.9619864921
# bestIteration = 29
#
# Shrink model to first 30 iterations.
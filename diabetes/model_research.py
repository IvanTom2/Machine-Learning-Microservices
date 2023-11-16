import pandas as pd
from pathlib import Path
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score

from notation import Feature
from pipeline import (
    DBAdapter,
    setup_nulls,
    setup_new_features,
    extract_columns,
    scale_features_learner,
    scale_features,
    save_model,
    upload_model,
)


RANDOM_STATE = 42
MODEL_PATH = Path(__file__).parent / "models"


def train_knn(data):
    X, y, std, std_cols = scale_features_learner(data)
    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.3,
        random_state=RANDOM_STATE,
    )

    knn = KNeighborsClassifier()
    knn.fit(X_train, y_train)

    save_model(MODEL_PATH / "std.pickle", std)
    save_model(MODEL_PATH / "knn.pickle", knn)

    pred = knn.predict(X_test)
    print(accuracy_score(y_test, pred))
    print(precision_score(y_test, pred))
    print(recall_score(y_test, pred))


if __name__ == "__main__":
    db_adapter = DBAdapter()

    data = db_adapter.get_data()
    data = setup_nulls(data)
    data = data.apply(setup_new_features, axis=1)
    data = extract_columns(data)

    # train_knn(data)

    std = upload_model(MODEL_PATH / "std.pickle")
    knn: KNeighborsClassifier = upload_model(MODEL_PATH / "knn.pickle")

    target = Feature.TARGET.name
    features_cols = [f.name for f in Feature.get_keys() if f != target]

    X = data[features_cols]
    y = data[target]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.3,
        random_state=RANDOM_STATE,
    )

    print(X_test)

    X_test: pd.DataFrame
    X_test = X_test.apply(scale_features, axis=1, args=(std,))

    print(X_test)

import pandas as pd
from pathlib import Path
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score
from sklearn.model_selection import train_test_split

from notation import Feature
from pipeline import (
    DBAdapter,
    LearnPipeline,
    ProductPipeline,
    save_model,
    upload_model,
    RANDOM_STATE,
    TEST_SAMPLE_SIZE,
    MODELS_PATH,
    MAIN_MODEL,
)


def train_knn():
    pipeline = LearnPipeline()
    X_train, X_test, y_train, y_test = pipeline.run()

    knn = KNeighborsClassifier()
    knn.fit(X_train, y_train)

    save_model(MODELS_PATH / MAIN_MODEL, knn)

    pred = knn.predict(X_test)
    print(accuracy_score(y_test, pred))
    print(precision_score(y_test, pred))
    print(recall_score(y_test, pred))


def test_knn_model():
    db = DBAdapter()
    pipeline = ProductPipeline()

    data = db.get_data()
    X = pipeline.run(data)
    y = data[[Feature.target]]

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y.values.ravel(),
        test_size=TEST_SAMPLE_SIZE,
        random_state=RANDOM_STATE,
    )

    knn: KNeighborsClassifier = upload_model(MODELS_PATH / MAIN_MODEL)

    pred = knn.predict(X_test)
    print(accuracy_score(y_test, pred))
    print(precision_score(y_test, pred))
    print(recall_score(y_test, pred))


if __name__ == "__main__":
    ## code for learning models
    # train_knn()

    ## code for test models usage
    test_knn_model()

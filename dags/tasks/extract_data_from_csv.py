import pandas as pd


def extract_data(uri: str, ti) -> pd.DataFrame:
    df = pd.read_csv(uri)
    ti.xcom_push(key="dataframe", value=df)


def get_shape(ti) -> dict[str, int]:
    df: pd.DataFrame = ti.xcom_pull(task_ids="get_dataframe", key="dataframe")
    return {
        "rows": df.shape[0],
        "columns": df.shape[1]
    }
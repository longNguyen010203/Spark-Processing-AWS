FROM apache/airflow:2.9.2

WORKDIR /opt/airflow

COPY requirements.txt /opt/airflow

RUN pip install --upgrade pip && pip install -r requirements.txt
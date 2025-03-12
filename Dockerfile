FROM apache/airflow:2.10.5                              

WORKDIR /app
COPY requirements.txt /tmp/requirements.txt
COPY dags/ /opt/airflow/dags/
COPY tasks/ /opt/airflow/tasks/
COPY key.json /opt/airflow/key.json
COPY vars.json /opt/airflow/vars.json

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor

RUN pip install --no-cache-dir -r /tmp/requirements.txt
# RUN airflow variables import vars.json
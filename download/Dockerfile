FROM apache/airflow:2.10.3-python3.11

USER root
# Cài đặt thêm các gói hệ thống nếu cần (ví dụ git, vim)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Copy requirements và cài đặt
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

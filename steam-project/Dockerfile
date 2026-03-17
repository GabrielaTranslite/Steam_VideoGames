FROM apache/airflow:2.7.2

USER root
# Zainstaluj systemowe zależności jeśli potrzebne (np. git)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
# Tutaj instalujemy biblioteki Python
RUN pip install --no-cache-dir \
    apache-airflow-providers-google \
    pandas \
    requests

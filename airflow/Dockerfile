FROM selenium/standalone-chrome

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED 1

RUN sudo apt update && sudo apt install -y \
  python3-pip

ENV AIRFLOW_HOME=/home/seluser/app/airflow

# DBT
# $DEL_BEGIN
ENV DBT_DIR=$AIRFLOW_HOME/dbt_lewagon
ENV DBT_TARGET_DIR=$DBT_DIR/target
ENV DBT_PROFILES_DIR=$DBT_DIR
ENV DBT_VERSION=1.1.1
# $DEL_END


WORKDIR $AIRFLOW_HOME

COPY scripts scripts
RUN sudo chmod +x scripts/entrypoint.sh

COPY pyproject.toml poetry.lock ./

USER root

RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --only main

# airflow/Dockerfile
FROM apache/airflow:2.7.1

USER root

# Install Java + build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-11-jdk-headless \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# create requirements dir
RUN mkdir -p /opt/airflow/requirements && chown airflow: /opt/airflow/requirements

USER airflow
COPY requirements.txt /opt/airflow/requirements/requirements.txt

RUN if [ -s /opt/airflow/requirements/requirements.txt ]; then \
      pip install --no-cache-dir -r /opt/airflow/requirements/requirements.txt ; \
    else \
      echo "No requirements found — skipping pip install"; \
    fi

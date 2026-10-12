# Runs the whole project (Python 3.11 + Java 17 for Spark) – nothing to install on the host except Docker.
FROM python:3.11-slim-bookworm

# Java runtime for Spark, `procps` (Spark's scripts need `ps`), `make`, and timezone data
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps make tzdata \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java
# The source data is in German local time, so run the container in German time too
ENV TZ=Europe/Berlin \
    JAVA_HOME=/opt/java \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MPLBACKEND=Agg

# Non-root user (uid 1000 matches the default user on most Linux machines, so files
# written into the mounted ./data folder stay owned by you)
RUN useradd --create-home --uid 1000 app
WORKDIR /app
RUN chown app:app /app

COPY requirements.txt requirements-dev.txt ./
RUN pip install -r requirements-dev.txt

USER app
# Download the Delta Lake jars once at build time, so pipeline runs don't need Maven
RUN python -c "from delta import configure_spark_with_delta_pip as c; from pyspark.sql import SparkSession as S; \
c(S.builder.master('local[1]').config('spark.ui.enabled','false')).getOrCreate().stop()"

COPY --chown=app:app . .
ENV PYTHONPATH=/app/src

EXPOSE 8501
CMD ["make", "all"]

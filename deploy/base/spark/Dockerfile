FROM spark:3.5.0-scala2.12-java11-ubuntu

USER root

RUN set -ex; \
    apt-get update; \
    apt-get install -y python3 python3-pip; \
    rm -rf /var/lib/apt/lists/*
RUN wget https://jdbc.postgresql.org/download/postgresql-42.7.5.jar
RUN mv postgresql-42.7.5.jar /opt/spark/jars
RUN mkdir /opt/spark/jobs
USER spark

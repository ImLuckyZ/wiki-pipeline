FROM apache/spark:3.5.3-python3

USER root
ENV PIP_BREAK_SYSTEM_PACKAGES=1
RUN python3 -m pip install --no-cache-dir psycopg2-binary==2.9.10

WORKDIR /app
COPY job.py .

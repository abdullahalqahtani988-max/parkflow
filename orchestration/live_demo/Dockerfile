# Live demo controller: gcloud (Bigtable/Dataflow control) + Apache Beam (job launcher) + publisher.
# python:3.12-slim = same Python as the environment where the Dataflow job was validated
# (the launcher's Beam/Python version decides the Dataflow worker image).
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends curl gnupg ca-certificates \
    && curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg \
       | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
       > /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y --no-install-recommends google-cloud-cli \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pipelines/requirements.txt pipelines/requirements.txt
RUN pip install --no-cache-dir setuptools wheel -r pipelines/requirements.txt

COPY setup.py setup.py
COPY config/ config/
COPY simulator/ simulator/
COPY pipelines/ pipelines/
COPY orchestration/live_demo/live_demo.sh live_demo.sh

ENTRYPOINT ["bash", "live_demo.sh"]

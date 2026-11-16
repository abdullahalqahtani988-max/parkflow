# Live demo controller: gcloud (Bigtable/Dataflow control) + Apache Beam (job launcher) + publisher.
FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:slim

WORKDIR /app
COPY pipelines/requirements.txt pipelines/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages setuptools wheel -r pipelines/requirements.txt

COPY setup.py setup.py
COPY config/ config/
COPY simulator/ simulator/
COPY pipelines/ pipelines/
COPY orchestration/live_demo/live_demo.sh live_demo.sh

ENTRYPOINT ["bash", "live_demo.sh"]

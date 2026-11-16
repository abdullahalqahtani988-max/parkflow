# Container for the ParkFlow daily batch job (Cloud Run Job).
# Python runs the simulator + GCS upload; Node runs the Dataform CLI.
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends nodejs npm \
    && npm install -g @dataform/cli@3.1.5 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
RUN pip install --no-cache-dir pyarrow google-cloud-storage

COPY config/ config/
COPY simulator/ simulator/
COPY dataform/ dataform/
COPY orchestration/daily_job/run_daily.sh run_daily.sh

# Dataform needs its npm package for the core version pinned in workflow_settings.yaml
RUN cd dataform && dataform install || true

ENTRYPOINT ["bash", "run_daily.sh"]

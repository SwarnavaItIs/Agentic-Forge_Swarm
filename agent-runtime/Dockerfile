FROM python:3.12.10-slim-bookworm@sha256:fd95fa221297a88e1cf49c55ec1828edd7c5a428187e67b5d1805692d11588db
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 HOME=/nonexistent
WORKDIR /runtime
COPY requirements.lock /runtime/requirements.lock
COPY wheels /wheels
RUN python -m pip install --no-cache-dir --no-index --find-links=/wheels --require-hashes -r requirements.lock
COPY forge_agent /runtime/forge_agent
COPY generated /runtime/generated
COPY pricing.json /runtime/pricing.json
USER 10002:10002
ENTRYPOINT ["python", "-m", "forge_agent.server"]

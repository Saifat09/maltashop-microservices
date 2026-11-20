# syntax=docker/dockerfile:1
# Build context is the repository root so the shared chassis library can be copied in:
#   docker build -f services/payment/Dockerfile -t maltashop/payment-service .

# ---------- Stage 1: build dependencies into a virtualenv ----------
FROM python:3.12-slim AS build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
COPY services/payment/requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt
COPY libs/chassis /tmp/chassis
RUN pip install /tmp/chassis

# ---------- Stage 2: small runtime image, no compilers or pip cache ----------
FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PATH="/opt/venv/bin:$PATH"
RUN groupadd --system --gid 10001 app \
 && useradd --system --uid 10001 --gid app --no-create-home app
COPY --from=build /opt/venv /opt/venv
WORKDIR /srv
COPY --chown=app:app services/payment/app ./app
USER app
EXPOSE 8003
# Liveness check without installing curl: the slim image already has Python.
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request as u; \
      u.urlopen('http://127.0.0.1:8003/health/live', timeout=2)" || exit 1
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8003", "--no-access-log"]

# NOTE: Cloud Run deployment is pending access provisioning.
# This Dockerfile is ready but not currently in use.
# When access is granted: bash scripts/deploy.sh
FROM python:3.11-slim
WORKDIR /app
COPY dashboards/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY dashboards/app.py .
EXPOSE 8080
CMD ["streamlit", "run", "app.py", "--server.port=8080", "--server.address=0.0.0.0", "--server.headless=true"]

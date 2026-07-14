# Lightweight official Python image
FROM python:3.11-slim

WORKDIR /app

# Copy dependency file first for build-cache friendliness.
COPY dashboards/streamlit/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["python", "-m", "streamlit", "run", "dashboards/streamlit/app.py", "--server.port=8501", "--server.address=0.0.0.0"]

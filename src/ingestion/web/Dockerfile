# Dockerfile
FROM python:3.12.3-slim


WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Expose port (Scaleway uses port 8080)
EXPOSE 8080

# Run FastAPI with Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]

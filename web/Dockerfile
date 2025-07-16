# Dockerfile
FROM python:3.12.3-slim

WORKDIR /app

COPY src src/
COPY web web/ 

RUN pip install --no-cache-dir -r web/requirements.txt


# Expose port (Scaleway uses port 8080)
EXPOSE 8080

# Run FastAPI with Uvicorn
CMD ["uvicorn", "web.app.main:app", "--host", "0.0.0.0", "--port", "8080"]





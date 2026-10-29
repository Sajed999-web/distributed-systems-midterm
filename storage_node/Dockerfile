FROM python:3.12-slim
WORKDIR /app
COPY storage_node/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY storage_node ./storage_node
VOLUME /data
EXPOSE 8000
CMD ["uvicorn", "storage_node.main:app", "--host", "0.0.0.0", "--port", "8000"]

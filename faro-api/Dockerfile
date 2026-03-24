FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
COPY api/ api/
RUN pip install -e .
EXPOSE 9000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "9000"]

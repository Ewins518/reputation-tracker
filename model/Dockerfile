FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9
COPY ./app /app
RUN pip install -r requirement.txt
EXPOSE 80
CMD ["uvicorn", "onnx_api:app", "--host", "0.0.0.0", "--port", "80"]

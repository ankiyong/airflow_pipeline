FROM alpine:3.16
RUN apk update && \
    apk add --no-cache python3 py3-pip

RUN ln -sf python3 /usr/bin/python
WORKDIR /app
VOLUME ["/app"]
COPY requirements.txt .
COPY log_generate.py .
COPY olist.csv .
RUN pip install --no-cache-dir -r /app/requirements.txt
CMD ["python", "/app/log_generate.py"]

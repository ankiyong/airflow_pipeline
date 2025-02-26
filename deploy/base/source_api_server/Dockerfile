FROM python:3.10.12



RUN apt update \
    && apt install -y \
    cron \
    vim \
    && apt clean \
    && rm -rf /var/lib/apt/lists/* \

RUN mkdir app
WORKDIR /app
COPY requirements.txt .
COPY .env .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY *.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM python:3.10.12-alpine

RUN apk update && apk add --no-cache cronie vim

RUN mkdir /app
WORKDIR /app
COPY requirements.txt .
COPY .env .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
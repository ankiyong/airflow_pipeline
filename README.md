<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=Python&logoColor=white">
  <img src="https://img.shields.io/badge/Apache Spark-E25A1C?style=flat-square&logo=Apache Spark&logoColor=white">
  <img src="https://img.shields.io/badge/Apache Airflow-017CEE?style=flat-square&logo=Apache Airflow&logoColor=white">
  <img src="https://img.shields.io/badge/Google BigQuery-669DF6?style=flat-square&logo=Google BigQuery&logoColor=white">
  <img src="https://img.shields.io/badge/Google Cloud Storage-AECBFA?style=flat-square&logo=Google Cloud Storage&logoColor=white">
</p>

# :rocket: e-Commerce Data Engineering Project
![Image](https://github.com/user-attachments/assets/de7c7081-afec-4009-a24a-0466d52cc6fd)



## Overview

본 프로젝트는 Spark와 Airflow 등을 통해 데이터 파이프라인을 구축하고, Brazil Olist의 소비자 구매 데이터를 분석하여 의미있는 결과를 도출하는 것을 목표로 합니다.
## Data Visualization

## Data Architecture
![Image](https://github.com/user-attachments/assets/dee00690-3749-4b73-bed2-c28212b7bb0a)
### 인프라
1. Local - k3s 환경 (3 Node 구성)
2. Cloud - GCP (Pub/Sub,CloudStorage,Bigquery,LookerStudio)
### 수집
#### 1. Data Source
- FastAPI + Postgresql 을 통한 API 서버 구축
#### 2. Python 수집
- Airflow의 DAG을 활용하여 API 서버에서 데이터 수집
- DAG 실행당 수집 데이터 수는 Random 하게 구성
- 실제 환경과 비슷한 상황을 위해 수집 DAG의 실행을 무한 반복
- 수집 된 데이터는 GCP Pub/Sub으로 전송

#### 3. Pub/Sub
- GCP의 Pub/Sub으로 데이터 전송
- Kafka와 동일한 기능을 하지만, 설치시 발생하는 오버헤드를 감안하여 선택함

### 처리
- Airflow,Kubernetes 위에서 Spakr Operator를 통해 동작
- SparkSQL을 활용
- Pub/Sub으로 부터 Pull 한 데이터는 df 형태에서 가공 후 parquet으로 변환하여여 CloudStorage로 적재

### 적재
#### 1. CloudStorage
- Spark로부터 데이터를 받아와 parquet으로 저장
- 실시간 데이터가 아닌 User,지리 정보와 같이 이미 생성 된 데이터 CSV 파일 적재
- User의 구매 행동 데이터를 활용하기 때문에 사이즈가 지속적으로 커질 것을 고려하여 Object Storage에 우선 저장

#### 2. BigQuery
- CloudStorage의 객체를 Table로 저장
- SQL을 활용하여 데이터 분석

### 시각화
- Looker Studio를 통해 BigQuery에 저장된 데이터를 활용하여 시각화

## Trouble Shooting
[Spark on K8S](https://aky123.tistory.com/66)

[psycopg2 install error](https://aky123.tistory.com/60)

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

### 사용 기술
1. FastAPI
- Django가 더욱 많은 패키지와 보안을 제공하고 있지만, Web개발이 메인이 아니기 때문에 비교적 구축이 쉽고 가벼운 FastAPI 선택
2. Kubernetes
- 파이프라인의 안정적인 운영을 위해 선택
- Airflow 및 Spark 사용시 리소스 관리를 pod 단위로 할 수 있고 pod의 사양을 yaml로 조절할 수 있음
+ GCP의 GKE에 비해 구축 및 운영에 오버헤드가 발생하지만 로컬 Kubernetes 경험이 있어 직접 구축 -> 추후 GKE에도 구축 예정 (2025.03.11 추가)
3. Pub/Sub
- Kafka와 유사한 기능을 가진 GCP의 메시징 서비스
- Pub/Sub은 완전 관리형 서비스로 Kafka에 비해 구축 오버헤드가 현저히 낮기 때문에 선택
4. Airflow
- 워크플로 자동화 및 관리를 통해 데이터 정제 및 적재 과정을 자동화하여 반복 작업을 줄이고 운영 효율성 향상을 위해 선택
- 다양한 Operator의 지원으로 Python,Spark,GCP 등 많은 기술과의 연계가 가능
+ 로컬에 구축된 Kubernetes와 연계 사용하기 위해 Cloud Composer 대신 선택 (2025.03.11 추가)
+ Dagster 같은 대안도 있었지만 현재 주류로 사용되는 Airflow 선택 (2025.03.11 추가)
5. GCP
- 기능
  - Bigquery의 실시간 분석 기능과 타 클라우드의 제품과 비교했을 때 관리에 이점이 있어 선택택
- 비용
  - 토이프로젝트로 진행되기 때문에 무료 크레딧을 제공해주는 GCP를 선택

### 수집
#### 1. Data Source
- FastAPI + Postgresql 을 통한 API 서버 구축
+ 수집 대상 테이블 조인 후 구체화 View 생성(2025.03.11 추가)
#### 2. Python 수집
- Airflow의 DAG을 활용하여 API 서버에서 데이터 수집
- ~~DAG 실행당 수집 데이터 수는 Random 하게 구성~~
+ DAG 실행당 1000건 수집 / 8000/Min (2025.03.11 추가)
- 실제 환경과 비슷한 상황을 위해 수집 DAG의 실행을 무한 반복
- 수집 된 데이터는 GCP Pub/Sub으로 전송

#### 3. Pub/Sub
- GCP의 Pub/Sub으로 데이터 전송
- Kafka와 동일한 기능을 하지만, 설치시 발생하는 오버헤드를 감안하여 선택함

### 처리
- Airflow,Kubernetes 위에서 Spakr Operator를 통해 동작
+ 5분에 한번 동작하여 회당 40000건의 데이터 처리(2025.03.11 추가)
- SparkSQL을 활용
- Pub/Sub으로 부터 Pull 한 데이터는 df 형태에서 가공 후 parquet으로 변환하여여 CloudStorage로 적재
+ 처리 된 데이터를 Bigquery에 적재 (2025.03.11 추가)
### 적재
#### 1. CloudStorage
- Spark로부터 데이터를 받아와 parquet으로 저장
- 실시간 데이터가 아닌 User,지리 정보와 같이 이미 생성 된 데이터 CSV 파일 적재
- User의 구매 행동 데이터를 활용하기 때문에 사이즈가 지속적으로 커질 것을 고려하여 Object Storage에 우선 저장

#### 2. BigQuery
- ~~CloudStorage의 객체를 Table로 저장~~ -> External Table 사용시 발생할 수 있는
+ Spark에서 직업 Bigquery에 데이터 저장 (2025.03.11 추가)
- SQL을 활용하여 데이터 분석

### 시각화
- Looker Studio를 통해 BigQuery에 저장된 데이터를 활용하여 시각화

## Trouble Shooting
[Spark on K8S](https://aky123.tistory.com/66)

[psycopg2 install error](https://aky123.tistory.com/60)

[Spark Postgres Driver Error](https://aky123.tistory.com/74)

[Spark Type Error](https://aky123.tistory.com/73)

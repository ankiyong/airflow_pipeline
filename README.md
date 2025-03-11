<p align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=Python&logoColor=white">
  <img src="https://img.shields.io/badge/Apache Spark-E25A1C?style=flat-square&logo=Apache Spark&logoColor=white">
  <img src="https://img.shields.io/badge/Apache Airflow-017CEE?style=flat-square&logo=Apache Airflow&logoColor=white">
  <img src="https://img.shields.io/badge/Google BigQuery-669DF6?style=flat-square&logo=Google BigQuery&logoColor=white">
  <img src="https://img.shields.io/badge/Google Cloud Storage-AECBFA?style=flat-square&logo=Google Cloud Storage&logoColor=white">
</p>

# :rocket: e-Commerce Data Engineering Project
![Image](https://github.com/user-attachments/assets/de7c7081-afec-4009-a24a-0466d52cc6fd)



## 프로젝트 개요

본 프로젝트는 Spark와 Airflow 등을 통해 데이터 파이프라인을 구축하고, Brazil Olist의 소비자 구매 데이터를 분석하여 의미있는 결과를 도출하는 것을 목표로 합니다.
## Data Visualization

## Data Architecture
![Image](https://github.com/user-attachments/assets/dee00690-3749-4b73-bed2-c28212b7bb0a)

### 1. 인프라 구성

✅ 로컬 환경 (k3s 기반)

- 3개의 노드로 구성된 k3s 클러스터

- Airflow 및 Spark 실행

✅ 클라우드 환경 (GCP)

- Pub/Sub: 데이터 스트리밍

- Cloud Storage: 데이터 저장

- BigQuery: 데이터 적재 및 분석

- Looker Studio: 시각화

### 2. 사용 기술

🚀 FastAPI

- 가볍고 빠른 API 서버 구축을 위해 선택

- Django 대비 적은 오버헤드로 간편한 설정 가능

🛠️ Kubernetes

- 파이프라인의 안정적인 운영 및 확장성을 고려하여 사용

- Airflow 및 Spark의 리소스를 pod 단위로 관리 가능

- GCP의 GKE에 비해 운영 오버헤드가 발생하지만, 추후 GKE로 이전 계획 (2025.03.11 추가)

📩 Pub/Sub

- Kafka와 유사한 기능을 제공하는 GCP의 메시징 서비스

- 완전 관리형 서비스로 구축 오버헤드가 낮아 선택

⏳ Airflow

- 데이터 정제 및 적재 자동화를 위한 워크플로 관리 도구

- 다양한 Operator를 지원하여 Python, Spark, GCP 등과 연계 가능

- Cloud Composer 대신 로컬 Kubernetes와 연계하여 사용 (2025.03.11 추가)

- Dagster와 같은 대안이 있지만, 현재 주류로 사용되는 Airflow를 선택 (2025.03.11 추가)

☁️ GCP

- BigQuery: 실시간 분석 기능 제공 및 관리 용이성 고려하여 선택

- 비용 측면: 무료 크레딧 제공으로 비용 절감 가능

## 3. 데이터 수집

📌 데이터 소스

- FastAPI + PostgreSQL 기반 API 서버 구축

- 수집 대상 테이블 조인 후 구체화 View 생성하여 사용 (2025.03.11 추가)

📥 Python을 활용한 데이터 수집

- Airflow DAG을 활용하여 API 서버에서 데이터 수집

- DAG 실행당 1000건의 데이터 수집, 최대 8000건/분 처리 (2025.03.11 추가)

- 실환경과 유사하게 동작하도록 무한 반복 실행

- 수집된 데이터는 GCP Pub/Sub으로 전송

🔄 Pub/Sub을 활용한 데이터 스트리밍

- Kafka와 유사한 메시징 서비스

- 운영 오버헤드를 줄이기 위해 GCP Pub/Sub 선택

4. 데이터 처리

- Airflow + Kubernetes에서 Spark Operator를 사용하여 처리

- SparkSQL을 활용하여 데이터 변환

- Pub/Sub에서 pull한 데이터를 Parquet 형식으로 변환 후 Cloud Storage에 저장

- 5분마다 실행하여 회당 40,000건의 데이터 처리 (2025.03.11 추가)

- 처리된 데이터를 BigQuery에 적재 (2025.03.11 추가)

### 5. 데이터 적재

📂 Cloud Storage

- Spark 처리 후 데이터를 Parquet 형식으로 저장

- User, 지리 정보 등 변하지 않는 데이터를 CSV 파일로 저장

- 구매 행동 데이터는 지속적으로 증가할 것을 고려하여 Object Storage 활용

📊 BigQuery

- ~~Cloud Storage에서 External Table로 저장~~ → Spark에서 직접 BigQuery에 저장 (2025.03.11 추가)

- SQL을 활용하여 데이터 분석

### 6. 데이터 시각화

📈 Looker Studio

- BigQuery에 저장된 데이터를 기반으로 시각화

- 대시보드를 활용하여 인사이트 도출

🔖 결론
- 본 프로젝트는 Kubernetes 기반에서 Airflow, Spark, 그리고 GCP 서비스를 연계하여 대규모 데이터 파이프라인 구축합니다. 데이터 수집부터 처리, 저장, 시각화까지 전 과정을 자동화하여 운영 효율성을 극대화하고, 확장성을 고려한 구조를 설계하였습니다. 추후 GKE로의 이전 및 성능 최적화를 진행할 예정입니다.
## Trouble Shooting
[Spark on K8S](https://aky123.tistory.com/66)

[psycopg2 install error](https://aky123.tistory.com/60)

[Spark Postgres Driver Error](https://aky123.tistory.com/74)

[Spark Type Error](https://aky123.tistory.com/73)

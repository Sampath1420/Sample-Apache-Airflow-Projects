# Apache Airflow ETL Project

## Overview

This project demonstrates how to build, schedule, and orchestrate ETL pipelines using Apache Airflow with Docker.
It includes example DAGs and a Weather Data ETL pipeline, showcasing best practices such as task separation,
environment configuration, and containerized deployment.

---

## Project Structure

Apache AirFlow/
├── dags/
│   ├── ETLweather.py
│   ├── exampledag.py
│   └── .airflowignore
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── packages.txt
├── airflow_settings.yaml
├── tests/
│   └── dags/
│       └── test_dag_example.py
├── plugins/
├── include/
├── .env
├── .dockerignore
├── .gitignore
└── README.md

---

## Key Features

- Apache Airflow DAG development
- Dockerized Airflow using Docker Compose
- Real-world ETL pipeline example
- Weather data extraction, transformation, and loading
- DAG testing and validation

---

## Weather ETL Pipeline

The Weather ETL DAG performs:
1. Extract: Fetches weather data from an external API
2. Transform: Cleans and structures the data
3. Load: Stores processed data into a target system

---

## Prerequisites

- Docker
- Docker Compose
- Git

---

## Setup Instructions

Clone the repository:
git clone <repo-url>
cd Apache AirFlow

Start Airflow:
docker-compose up --build

Access Airflow UI:
http://localhost:8080

Default login:
Username: airflow
Password: airflow

---

## Technologies Used

- Apache Airflow
- Python
- Docker & Docker Compose
- REST APIs

---

## Author

Kayagurla Sampath Kumar
Data Engineer | Airflow | Python | SQL
Hyderabad, India

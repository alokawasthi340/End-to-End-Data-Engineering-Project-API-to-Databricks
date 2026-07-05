
## Overview

This project demonstrates a complete modern data engineering workflow using Databricks. It covers API data extraction, ELT processing, orchestration, testing, data quality validation, and CI/CD automation following DataOps best practices.

The solution extracts data from external APIs using Python, loads it into Databricks Delta Lake, performs transformations using Databricks notebooks and PySpark, and automates workflows using Databricks Jobs.

---

## Project Objectives

### API Data Extraction
Build Python-based ingestion pipelines to extract data from REST APIs using endpoints tested and validated through Postman.

### Data Loading & ELT Processing
Load raw API data into Databricks Delta tables and implement scalable ELT transformations using PySpark and Databricks notebooks.

### Databricks Data Warehouse
Use Databricks SQL Warehouse and Delta Lake as the analytical data warehouse for storing, querying, and transforming data.

### Workflow Orchestration
Master orchestration and automation of data pipelines using Databricks Jobs, enabling reliable scheduling, monitoring, and dependency management.


### CI/CD Automation
Implement GitHub Actions to automate:

- Code Quality Checks
- Unit Test Execution
- Deployment Validation
- Continuous Integration
- Continuous Deployment (CI/CD)

for Databricks assets and data pipelines.

---

## Technology Stack

| Component | Technology |
|------------|------------|
| Programming Language | Python |
| API Testing | Postman |
| Data Processing | PySpark |
| Data Lakehouse | Databricks Delta Lake |
| Data Warehouse | Databricks SQL Warehouse |
| Orchestration | Databricks Jobs |
| CI/CD | GitHub Actions |
| Version Control | Git & GitHub |

---

## Architecture

API Source
↓
Python Extraction Layer
↓
Bronze Layer (Raw Data)
↓
Silver Layer (Cleansed Data)
↓
Gold Layer (Business-ready Data)
↓
Databricks SQL Warehouse
↓
Analytics & Reporting

---

## Features

- REST API Data Ingestion
- Delta Lake Storage
- ELT Data Transformation
- Medallion Architecture (Bronze, Silver, Gold)
- Databricks Job Orchestration
- Automated Testing with Pytest
- Data Quality Validation with Soda
- GitHub Actions CI/CD
- Scalable Data Engineering Workflows

---

## Learning Outcomes

- Build production-ready API ingestion pipelines.
- Implement ELT pipelines in Databricks.
- Develop scalable transformations using PySpark.
- Orchestrate workflows using Databricks Jobs.
- Perform Unit, Integration, and E2E testing.
- Implement automated data quality checks using Soda.
- Automate deployments with GitHub Actions.
- Apply DataOps and modern data engineering best practices.

---
Final Dashboard:
<img width="1737" height="814" alt="image" src="https://github.com/user-attachments/assets/2546befe-5b8c-4d49-bd7d-93c91375d87f" />

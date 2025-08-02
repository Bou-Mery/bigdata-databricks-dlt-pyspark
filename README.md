# 🚀 Enterprise Flights Data Pipeline - Databricks Medallion Architecture

## 🎯 Overview

This project demonstrates an **enterprise-grade data pipeline** built on **Databricks** using the **Medallion Architecture (Bronze-Silver-Gold)**. The pipeline processes flight booking data with advanced features including **Auto Loader**, **Delta Live Tables (DLT)**, **Change Data Capture (CDC)**, and **Slowly Changing Dimensions (SCD Type 1)**.

### 🧠 Project Motivation
The goal was to simulate a real-world data engineering scenario where:
- Raw flight data needs to be ingested incrementally
- Data quality and transformations are crucial
- Business analytics require a clean, dimensional model
- System must handle schema evolution and streaming updates

## 🏗️ Architecture

The pipeline follows the **Medallion Architecture** pattern with three distinct layers:

### 🥉 Bronze Layer (Raw Data)
- **Purpose**: Raw data ingestion with minimal processing
- **Storage**: Delta Tables with ACID compliance
- **Features**: 
  - Streaming ingestion using Auto Loader
  - Schema evolution support
  - Data lineage tracking

### 🥈 Silver Layer (Cleaned & Conformed)
- **Purpose**: Data cleansing, standardization, and CDC processing
- **Storage**: Delta Live Tables (DLT)
- **Features**:
  - Data quality checks and validation
  - Change Data Capture implementation
  - SCD Type 1 handling for dimension updates
  - Automated data lineage

### 🥇 Gold Layer (Business-Ready)
- **Purpose**: Dimensional modeling for analytics
- **Storage**: Star Schema with fact and dimension tables
- **Features**:
  - Optimized for BI tools (Tableau, Power BI)
  - Pre-aggregated business metrics
  - High-performance query patterns

<img width="4199" height="2379" alt="Image" src="https://github.com/user-attachments/assets/aa2da3be-b58c-4833-ba5d-731c15b70145" />

## 📊 Data Model

The pipeline implements a **Star Schema** design optimized for flight booking analytics:

<img width="1272" height="992" alt="Image" src="https://github.com/user-attachments/assets/0ffefd2f-46ca-4b05-b7c7-f5fba1fdf30e" />

### Fact Table
- **`fact_bookings`**: Central fact table containing booking transactions

### Dimension Tables
- **`dim_flights`**: Flight information (airline, origin, destination, dates)
- **`dim_airports`**: Airport details (location, codes, metadata)
- **`dim_passengers`**: Customer information with SCD Type 1 updates

### Key Relationships
- Each booking links to flights, airports, and passengers
- Star schema enables efficient OLAP queries
- Supports complex analytical workloads

## ✨ Features

### 🔄 Real-Time Data Processing
- **Auto Loader**: Incremental file ingestion from cloud storage
- **Streaming Architecture**: Near real-time data processing
- **Schema Evolution**: Automatic handling of schema changes

### 🛡️ Data Quality & Governance
- **Delta Lake**: Optimized storage with Z-ordering
- **Delta Live Tables**: Built-in data quality checks
- **ACID Transactions**: Ensures data consistency
- **Data Lineage**: Complete traceability of data transformations

### 📈 Change Data Capture (CDC)
- **SCD Type 1**: Updates dimension records in place
- **Incremental Processing**: Only processes changed records
- **Audit Trail**: Tracks all data modifications


## 🏃‍♂️ Pipeline Structure

The pipeline consists of multiple interconnected components:

1. **Data Sources**: CSV files with flight booking data
2. **Bronze Tables**: Raw data ingestion with Auto Loader
3. **Silver Tables**: Cleaned and transformed data
4. **Gold Tables**: Star schema for analytics
5. **Business Layer**: Ready for BI tools consumption

<img width="1238" height="512" alt="Image" src="https://github.com/user-attachments/assets/c886c7e6-610c-4a54-a521-62c4c1287115" />

<img width="1232" height="614" alt="Image" src="https://github.com/user-attachments/assets/7b120f30-c5f5-46e2-86ee-a2567af13246" />

## 🛠️ Technologies Used

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Platform** | Databricks | Unified analytics platform |
| **Storage** | Delta Lake | ACID-compliant data lake |
| **Processing** | Apache Spark | Distributed data processing |
| **Pipeline** | Delta Live Tables | Declarative ETL framework |
| **Ingestion** | Auto Loader | Incremental file processing |
| **Language** | Python/SQL | Data transformations |




## 📁 Project Structure


<img width="642" height="474" alt="Image" src="https://github.com/user-attachments/assets/290d059c-047f-4989-85b8-f956769b4c4f" />


## 🔮 Future Enhancements

### Phase 2 Roadmap
- [ ] **Machine Learning Integration**: Predictive analytics for booking patterns
- [ ] **Advanced CDC**: SCD Type 2 for historical tracking
- [ ] **Data Mesh**: Decentralized data architecture
- [ ] **Real-time Alerts**: Anomaly detection and monitoring
- [ ] **Multi-cloud Deployment**: Cross-cloud data replication

### Technical Improvements
- [ ] **Auto-scaling**: Dynamic cluster management
- [ ] **Advanced Security**: Column-level encryption
- [ ] **Data Catalog**: Automated metadata management
- [ ] **Testing Framework**: Comprehensive data quality testing


## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**BOUKHRAIS Meryem**

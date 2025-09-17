# **Project Pipeline on Azure Data Factory**
---

## ภาพรวมโปรเจกต์ (Overview)
โปรเจคนี้ถูกสร้างขึ้นมาเพื่อจำลองการใช้งาน Microsoft Azure Services ต่างๆ โดยบริการต่างๆดังนี้
1. Azure Data Lake Storage Gen2
2. Azure Databricks 
3. Azure Data Factory
4. Azure SQL Database
5. Azure Synapse Analytics
---

## Data Sources
- CSV
---

## สถาปัตยกรรม / Workflow Diagram
```mermaid
flowchart TD
    subgraph "Data Sources"
        A[customer.csv in ADLS Gen2]
        B[products.csv in ADLS Gen2]
        C[sales.csv in ADLS Gen2]
        D[Customers Table in Azure SQL Database]
    end

    subgraph "Data Processing in Databricks"
        E[Read data from CSV in ADLS Gen2] --> F[Read data from Parquet in ADLS Gen2]
        F --> G[Read data from Azure SQL Database]
        G --> H[Join DataFrames]
        H --> I[Clean and Transform Data]
    end

    subgraph "Output"
        J[Load processed data to Parquet in ADLS Gen2]
    end

    A --> E
    B --> F
    C --> E
    D --> G
    E --> H
    F --> H
    G --> H
    H --> I
    I --> J
```

---
## **Tech Stack**
1. Azure Data Lake Storage Gen2
2. Azure Databricks 
3. Azure Data Factory
4. Azure SQL Database
5. Azure Synapse Analytics

6. Python

7. SQL

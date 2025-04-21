# **GLOBAL HOUSING**
This project entails a comprehensive data engineering pipeline designed to ingest, process, and analyze data efficiently, enabling robust insights and decision-making.

![Image alt](https://github.com/asotechx/GLOBAL-HOUSING/blob/main/Pipeline.jpg?raw=true)

# **Tech Stack & Tools**
- Data Warehouse: PostgreSQL
- Database: PostgreSQL
- Orchestration: Apache Airflow
- Data Processing: Pandas
- ETL Scripts: Python
- Visualization: PowerBI

# **Pipeline Overview**
The pipeline begins by ingesting raw data from the Kaggle API, with Python used for data extraction and initial processing. Following the ETL (Extract, Transform, Load) process, the transformed data is stored in a PostgreSQL database, which serves as the centralized storage layer. The ETL workflow is orchestrated using Apache Airflow, ensuring tasks are scheduled and executed in a systematic and reliable manner. Finally, insights generated from the processed data are visualized using Power BI, delivering actionable intelligence through interactive reports and dashboards.

# **Getting Started**
This section provides step-by-step instructions to help you set up and run the project locally for development and testing purposes.

# **Installation & Setup**
- Clone the project repository to your local machine using Git:
git clone https://github.com/GLOBAL-HOUSING/GLOBAL-HOUSING.git
cd GLOBAL-HOUSING

# **Data Extraction from Kaggle API**
The data extraction process begins by exporting raw data from the Kaggle API. Using Python, we establish a connection to the Kaggle API to retrieve the required datasets. This extraction step is orchestrated by Apache Airflow, which ensures that data is pulled consistently and reliably into the processing environment, setting the stage for the subsequent ETL (Extract, Transform, Load) operations.

# **Transformation and Loading into the Data Warehouse Schema**
Once the data is extracted, transformation routines are applied to clean, normalize, and enrich the datasets. These transformations include:

- Data Cleaning: Handling missing values, removing duplicates, and correcting inconsistent formats.

- Normalization: Standardizing numerical and categorical values to ensure consistency across datasets.

The transformed data is subsequently loaded into a structured database schema within PostgreSQL. This schema is optimized for query performance and is specifically designed to support complex analytical queries, enabling efficient reporting and data visualization.


# **PowerBI Dashboards**
Our Power BI dashboards leverage the curated datasets to deliver interactive, insightful visualizations that support data-driven decision-making.

# **Sample Visualizations**
The following visualizations showcase key components of our Power BI dashboards, each designed to highlight critical insights and trends:

![Image alt](https://github.com/asotechx/GLOBAL-HOUSING/blob/main/Visualization.jpeg?raw=true)

# **Orchestration with Apache Airflow**

The ETL pipeline is orchestrated using Apache Airflow, which manages the scheduling and execution of all data processing tasks. Airflow’s robust framework enables seamless automation of the pipeline’s components—from data extraction to transformation and loading—ensuring a consistent and reliable data flow through each stage of the process.

Airflow also provides a user-friendly web interface for visualizing pipeline progress, monitoring task execution, and managing workflows effectively.
http://localhost:8080/dags/etl_global-housing/  

Note: The provided link assumes that Apache Airflow is running locally on port 8080. If your Airflow instance is hosted on a different machine or configured to use a different port, please adjust the URL accordingly.


# **Usage**
Here’s a well-structured section you can use for Instructions on executing ETL processes, scheduling via Airflow, and accessing Power BI reports:

# **Contributing**
We welcome contributions from the community! Please take a moment to read our contributing guidelines to get started.





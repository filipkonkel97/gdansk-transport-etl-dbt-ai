# gdansk-transport-etl-dbt-ai

## Project Description:

This project implements an end-to-end ETL pipeline for public transport data using Apache Airflow orchestration and Snowflake as the data warehouse, combined with a Streamlit-based analytics dashboard for data visualization.

The system extracts real-time public transport data from APIs provided by the [City of Gdańsk](https://www.gdansk.pl/otwarte-dane-w-gdansku?show=data&id=tristar&s=byGroup&i=transport), including:

* delays
* routes
* schedules
* vehicle positions

Data is then cleaned and transformed using dbt, loaded into Snowflake, and exposed through a Streamlit dashboard for interactive analysis.

The goal is to provide a full data pipeline from raw ingestion -> transformation -> analytics layer -> user-facing dashboard.

## Technologies Used:
* Apache Airflow: Workflow orchestration and scheduling
* Snowflake: Primary data warehouse for data storage
* Streamlit: Interactive data visualization dashboard
* Docker & Docker Desktop: Containerization platform
* Astro CLI: Airflow development environment
* dbt (Data Build Tool): Data transformation and modeling
* Python: Main programming language for data processing
* Ubuntu/WSL: Linux development environment

## Key Features:
* Automated extraction of public transport data from APIs
* Modular ETL pipeline orchestrated with Airflow DAGs
* Data validation and transformation layer using dbt
* Incremental loading strategies for efficiency
* Interactive Streamlit dashboard for exploring transport data
* Containerized architecture with Docker
* Scalable, production-oriented design

## Project Architecture:
* Data Extraction -> Public APIs -> Raw ingestion layer
* Data Transformation -> dbt models (cleaning + business logic)
* Data Loading -> Snowflake warehouse
* Orchestration -> Apache Airflow DAGs
* Data Visualization -> Streamlit dashboard
![Project diagram](diagrams/api_data_processing.png)

## Data Model:
The project follows a layered architecture:

* **Bronze** – raw ingested data from APIs  
* **Silver** – cleaned and standardized data  
* **Gold** – business-ready datasets (facts & dimensions)  
* **Marts** – analytical tables for reporting  

### Core Tables:
* `fct_delays` – delay events
* `fct_positions` – vehicle GPS positions
* `dim_routes`, `dim_stops`, `dim_trips`, `dim_vehicles`

## Pipeline Flow

1. Airflow triggers ingestion DAGs
2. Data is fetched from external APIs
3. Raw data is loaded into Snowflake (Bronze layer)
4. dbt runs transformations:
   - Silver layer (cleaning & normalization)
   - Gold layer (facts & dimensions)
   - Marts (aggregations)
5. Streamlit dashboard queries Snowflake for visualization

## Example Use Cases
* Identify routes with highest delays
* Analyze peak-hour congestion patterns
* Track vehicle movement in real-time
* Monitor transport system performance

## Project Setup:
1. **Install Ubuntu on Windows using WSL**`
   To run Ubuntu on Windows, first install WSL (Windows Subsystem for Linux). Open PowerShell as Administrator and run:
      ```sh
         wsl --install
      ```
   This will install WSL and the default Linux distribution. If you want a specific Ubuntu version, you can download it from the Microsoft Store after installing WSL.

2. **Install Docker Desktop**
   Download and install [Docker Desktop](https://www.docker.com/products/docker-desktop/) 

3. **Create free Snowflake account**
   Create a free Snowflake account and note the account details:
   * account
   * username
   * password
   * warehouse
   * database

4. **Install Astro CLI**
   Install Astro CLI by following the official [installation instructions](https://www.astronomer.io/docs/astro/cli/install-cli)

5. **Clone Repository**
   Clone project repository:
      ```sh
         git clone https://github.com/filipkonkel97/gdansk-transport-etl-dbt-ai.git
         cd gdansk-transport-etl-dbt-ai
      ```

6. **Configure Streamlit Secrets**
      ```sh
         cd streamlit/.streamlit
      ```
      Edit secrets_template.toml with Snowflake credentials and change it name to secrets.toml.

7. **Start Airflow**
   From the command prompt, run:
      ```sh
         astro dev start
      ```

8. **Start Streamlit**
   From the command prompt, run:
      ```sh
         docker build -f streamlit/Dockerfile -t streamlit-app .
         docker run -p 8501:8501 streamlit-app
      ```

9. **Configure Snowflake Connection in Airflow UI**
   Open your browser and go to:
      ```arduino
         localhost:8080
      ```
   Navigate to `Admin`→`Connections`→`Add Connection`, select 'Snowflake' and provide your login, password, account identifier, warehouse and database.

   In addition to Snowflake, configure the following API connections as Generic connections using base URL of the API as host:
      ```arduino
         delays_api = https://ckan2.multimediagdansk.pl/departures

         routes_api = https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/22313c56-5acf-41c7-a5fd-dc5dc72b3851/download/routes.json

         stops_api = https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/4c4025f0-01bf-41f7-a39f-d156d201b82b/download/stops.json

         trips_api = https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/b15bb11c-7e06-4685-964e-3db7775f912f/download/trips.json

         vehicles_db_api = https://files.cloudgdansk.pl/d/otwarte-dane/ztm/baza-pojazdow.json?v=2

         vehicles_psn_api = https://ckan2.multimediagdansk.pl/gpsPositions?v=2
      ```

## Future Improvements

- Implement an AI-powered data analyst for natural language queries
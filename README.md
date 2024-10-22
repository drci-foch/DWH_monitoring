# DWH Monitoring Dashboard

## Project Overview

This project is a comprehensive monitoring dashboard for a Data Warehouse (DWH) database. It provides various metrics and visualizations to help monitor database health, usage patterns, and data quality.

## Features

- Summary statistics of database contents
- User activity monitoring
- Document metrics and counts
- Archive status and management
- Data quality checks
- Visualization of trends over time

## Project Structure

```
DWH_monitoring/
│
├── src/
│   ├── backend/
│   │   ├── app/
│   │   │   ├── api/
│   │   │   │   ├── routes
│   │   │   │   │   ├── archives.py
│   │   │   │   │   ├── documents.py
│   │   │   │   │   ├── sources.py
│   │   │   │   │   ├── summary.py
│   │   │   │   │   └── users.py
│   │   │   │   ├── deps.py
│   │   │   │   └── main.py
│   │   │   ├── core/
│   │   │   │   ├── config.py
│   │   │   │   ├── db.py
│   │   │   │   └── security.py
│   │   │   ├── crud.py
│   │   │   ├── dependencies.py
│   │   │   └── main.py
│   │   └── .env
│   └── frontend/
         ├── src/
         │   ├── api/
         │   │   ├── __init__.py
         │   │   ├── client.py      
         │   │   ├── endpoints.py      
         │   │   └── exceptions.py          
         │   ├── data/
         │   │   ├── __init__.py
         │   │   └── generators.py      
         │   ├── services/
         │   │   ├── __init__.py
         │   │   └── data_service.py      
         │   └── views/
         │       ├── __init__.py
         │       ├── components/       
         │       │   ├── __init__.py
         │       │   ├── metrics.py
         │       │   └── charts.py
         │       └── pages/            
         │           ├── __init__.py
         └──           └── dashboard.py                 
├── venv/
├── README.md
├── requirements.txt
├── notes.txt
├── .gitattributes
└── .gitignore
```

## Setup and Installation

1. Clone the repository:
   ```
   git clone https://github.com/your-username/DWH_monitoring.git
   cd DWH_monitoring
   ```

2. Set up a virtual environment:
   ```
   python3 -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Running the Application

### Starting the Backend

1. Navigate to the backend directory:
   ```
   cd src_api/backend
   ```

2. Start the FastAPI server:
   ```
   uvicorn app.main:app --reload
   ```

   The API will be available at `http://localhost:8000`. You can access the API documentation at `http://localhost:8000/docs`.

### Starting the Frontend

1. Open a new terminal window and activate the virtual environment:
   ```
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

2. Navigate to the frontend directory:
   ```
   cd src/frontend/src/views/pages
   ```

3. Run the Streamlit app:
   ```
   streamlit run dashboard.py
   ```

   The dashboard will open in your default web browser, typically at `http://localhost:8501`.

## Usage

Once both the backend and frontend are running:

1. Open your web browser and go to `http://localhost:8501` to view the dashboard.
2. Use the various tabs and visualizations to explore the DWH monitoring data.
3. The "Refresh Data" button at the top of the dashboard will fetch the latest data from the backend.

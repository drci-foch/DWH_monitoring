# DWH Monitoring Project

## Overview
This project is designed to generate comprehensive reports on the quality and usage of a Data Warehouse (DWH). It provides insights into patient counts, document distributions, user activity, and more.

## Prerequisites
- Python 3.7+
- Oracle Database access
- Required Python packages (see `requirements.txt`)

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/your-username/DWH_monitoring.git
   cd DWH_monitoring
   ```

2. Create a virtual environment (optional but recommended):
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required packages:
   ```
   pip install -r requirements.txt
   ```

## Configuration

1. Update the `database_manager.py` file with your Oracle database connection details:
   ```python
   self.oracle_connection_params = {
       "user": "your_username",
       "password": "your_password",
       "dsn": "your_host:your_port/your_service_name"
   }
   ```

## Usage

Run the main script to generate the report:

```
python src/database_report_generator.py
```

This will create an Excel file named `database_quality_report.xlsx` in the chosen directory.

## Customization

You can customize the report by modifying the `DatabaseQualityReportGenerator` class in `database_report_generator.py`. Add new sheets or modify existing ones to suit your specific needs.

## Troubleshooting

If you encounter any database connection issues:
1. Ensure your Oracle client is properly installed and configured.
2. Verify that the connection parameters in `database_manager.py` are correct.
3. Check your network connectivity to the database server.


# Meter Data Generator and BigQuery Loader**

### Purpose

This script generates fake meter data (meter loads and readings) and loads it into Google BigQuery. Useful for testing or needing sample data.

### Features

* Generates random meter loads (meter ID, manufacturer, source, load date).
* Creates multiple meter readings for each load (timestamp included).
* Loads data into BigQuery.
* Customizable.

### Prerequisites

* Google Cloud Project (BigQuery API enabled)
* BigQuery dataset (script assumes "andresousa-demo")
* Python 3 
* Libraries: `google-cloud-bigquery`, `pandas`, `numpy`, `faker`, `dataclasses` 

### Setup

1. Clone this repo.
2. Install libraries: `pip install google-cloud-bigquery pandas numpy faker dataclasses`

### Usage

1. Edit `table_id` values in the script to match your BigQuery dataset and table names.
2. Run: `python meter_data_generator.py`

### Customization

* Change the number of generated samples.
* Adjust the data generation logic in the data classes.

### Important Notes

* Script overwrites existing BigQuery tables.
* Schema is fixed; edit as needed.

#### Example Generated Data

* **meter_load**
    * meter_id: Random integer
    * manufacturer: (e.g., 'KF8')
    * service_name: 'E'
    * source: 'Web' or 'App'
    * loaddate: Recent date

* **meter_readings_load**
    * readings_id: Random integer
    * meter_id: Matches a meter_load
    * readings: Random float
    * readings_time: Timestamp
    * source:  'Web' or 'App')
    * loaddate: Recent date

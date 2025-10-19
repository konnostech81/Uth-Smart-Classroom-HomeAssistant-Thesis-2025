import pandas as pd
import json
import sys
import os
import logging
from logging import getLogger
import datetime

# Configure default logging with timestamp
logger = getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [ha_meteo_uth] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Text file name which stores the last fetched unix time and status
LAST_STATUS = "/config/ha_meteo_uth/latest_status.json"

def read_status():
    # Reading the status from the JSON file
    try:
        with open(LAST_STATUS, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"The last fetched time is invalid or missing ({e}),")
        logger.info("Setting default value...")
        return {"last_time_fetched": 0, "match_count": -1}  # Default values if file doesn't exist

def write_status(status):
    logger.info(f"Saving last fetched status to {LAST_STATUS}")
    # Write the status to the JSON file 
    with open(LAST_STATUS, "w") as f:
        json.dump(status, f, indent=2)

def data_fetch(status):

    # Get the last fetched time
    last_fetch = int(float(status["last_time_fetched"]))
    match_count = status["match_count"]

    CSV_URL = "https://greendigital.uth.gr/data/meteo48h.csv.txt"
    csv_data=pd.read_csv(CSV_URL)

    # Selecting the relevant columns from the pulling csv file
    columns = ['Unix_time', 'Temp_Out', 'In_Temp', 'Low_Temp', 'Hi_Temp', 'Out_Hum', 'In_Hum', 'Dew_Pt', 'In_Dew', 'Solar_Rad']  
    values = csv_data[columns]

    # Converting non-numeric values to NaN
    values = values.apply(pd.to_numeric, errors='coerce')

    # Checking for NaN values and replace them with 0
    if values.isna().values.any():
        values = values.fillna(0)  # Replacing NaNs with 0
        logger.error(" NaN or broken values found and replaced with 0!")

    # Getting the lasts rows
    last_row = values.iloc[-1]
    sec_last_row = values.iloc[-2]

    # Convert unix to readable format
    date = datetime.datetime.utcfromtimestamp(last_row["Unix_time"]).strftime('%d %B %Y')
    time = datetime.datetime.utcfromtimestamp(last_row["Unix_time"]).strftime('%H:%M:%S')

    # Creating a dictionary with the last row data and the readable time
    data = {**{col: last_row[col] for col in columns}, "Date": date, "Time": time}

    if last_row["Unix_time"] == last_fetch:
        logger.warning(f"The last known data is fetched {match_count + 1} times already (Unix Time: {last_fetch})!")
        if match_count >= 2:
            logger.error("The returned data have been set to 'unavailable'.\nCheck if the source data is updated.")
            data = {**{col: "unavailable" for col in columns}, "Date": "unavailable", "Time": "unavailable"}
        else:
            # Increment of the match_count
            status["match_count"] += 1    

    if last_row["Unix_time"] != last_fetch:
        # If no match, reset the match count
        status["match_count"] = -1
        # Updating the last fetched time
        status["last_time_fetched"] = last_row["Unix_time"]

    # Writing the updated status to the file
    write_status(status)

    json_output = json.dumps(data, indent=12)
    print(json_output)

def main_job():
    # Reading the status from the JSON file
    status = read_status()

    # Fetching the data
    data_fetch(status)

main_job()

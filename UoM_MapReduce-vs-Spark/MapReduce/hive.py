#!/usr/bin/env python3

import subprocess
import json
from collections import defaultdict
import time

# Define Amazon S3 paths
S3_DATA_SOURCE_PATH = 's3://hivevsapachesqls3/data/'

# Define Hive SQL queries
QUERIES = {
    'q1': 'SELECT Year, AVG((CarrierDelay/ArrDelay)*100) as avgCarrierDelay FROM all_data GROUP BY Year',
    'q2': 'SELECT Year, AVG((WeatherDelay/ArrDelay)*100) as avgWeatherDelay FROM all_data GROUP BY Year',
    'q3': 'SELECT Year, AVG((NASDelay/ArrDelay)*100) as avgNASDelay FROM all_data GROUP BY Year',
    'q4': 'SELECT Year, AVG((SecurityDelay/ArrDelay)*100) as avgSecurityDelay FROM all_data GROUP BY Year',
    'q5': 'SELECT Year, AVG((LateAircraftDelay/ArrDelay)*100) as avgLateAircraftDelay FROM all_data GROUP BY Year'
}

def create_external_table():
    # Create external table in Hive
    subprocess.run(['hive', '-e', f"CREATE EXTERNAL TABLE IF NOT EXISTS all_data (Year INT, CarrierDelay INT, ArrDelay INT, WeatherDelay INT, NASDelay INT, SecurityDelay INT, LateAircraftDelay INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '{S3_DATA_SOURCE_PATH}'"])

def run_hive_query(query):
    # Execute Hive query and measure execution time
    start_time = time.time()
    subprocess.run(['hive', '-e', f'USE default; {query}'])
    end_time = time.time()
    return end_time - start_time

def main():
    create_external_table()

    q_executions = defaultdict(list)
    q_avg = []

    for query_name, query in QUERIES.items():
        for _ in range(5):
            execution_time = run_hive_query(query)
            q_executions[query_name].append(execution_time)

        q_avg.append(sum(q_executions[query_name]) / len(q_executions[query_name]))

    print('Average query durations:', q_avg)
    print('all exec times:', q_executions)

if __name__ == '__main__':
    main()
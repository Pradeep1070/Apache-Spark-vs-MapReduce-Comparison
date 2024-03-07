import json
from pyspark.sql import SparkSession
import numpy as np
from collections import defaultdict
import time

S3_DATA_SOURCE_PATH = 's3://hivevsapachesqls3/data/DelayedFlights-updated.csv'
S3_DATA_OUTPUT_PATH = 's3://hivevsapachesqls3/output/Spark/'

def main():
    # Start the SparkSession
    spark = SparkSession.builder.appName('mapReduceVsSparkApp').getOrCreate()

    # Load the data from S3 bucket
    all_data = spark.read.csv(S3_DATA_SOURCE_PATH, header=True)
    all_data.createOrReplaceTempView("all_data")

    queries = {
        'q1': 'SELECT Year, AVG((CarrierDelay/ArrDelay)*100) as avgCarrierDelay FROM all_data GROUP BY Year',
        'q2': 'SELECT Year, AVG((WeatherDelay/ArrDelay)*100) as avgWeatherDelay FROM all_data GROUP BY Year',
        'q3': 'SELECT Year, AVG((NASDelay/ArrDelay)*100) as avgNASDelay FROM all_data GROUP BY Year',
        'q4': 'SELECT Year, AVG((SecurityDelay/ArrDelay)*100) as avgSecurityDelay FROM all_data GROUP BY Year',
        'q5': 'SELECT Year, AVG((LateAircraftDelay/ArrDelay)*100) as avgLateAircraftDelay FROM all_data GROUP BY Year'
    }

    q_executions = defaultdict(list)
    q_results = []
    q_avg = []

    for k, v in queries.items():
        for i in range(5):
            start_time = time.time()
            query_result = spark.sql(v)
            end_time = time.time()
            execution_time = end_time - start_time
            q_executions[k].append(execution_time)

            if i == 4:
                q_results.append(query_result)
                q_avg.append(np.mean(q_executions[k]))

    cum_exec_times = [q_executions['q1'][0]]
    for i in range(1, 5):
        cum_exec_times.append(cum_exec_times[i-1] + q_executions['q1'][i])

    # Convert data to JSON strings
    json_q_executions = json.dumps(q_executions)
    json_cum_exec_times = json.dumps(cum_exec_times)
    json_q_avg = json.dumps(q_avg)

    # Write JSON strings to CSV and text files in S3
    spark.createDataFrame([(json_q_executions,)]).write.mode('overwrite').csv(S3_DATA_OUTPUT_PATH + 'q_executions.csv')
    spark.createDataFrame([(json_cum_exec_times,)]).write.mode('overwrite').csv(S3_DATA_OUTPUT_PATH + 'cum_exec_times.csv')
    spark.createDataFrame([(json_q_avg,)]).write.mode('overwrite').csv(S3_DATA_OUTPUT_PATH + 'q_avg.csv')

    for result in q_results:
        result.write.mode('overwrite').csv(S3_DATA_OUTPUT_PATH + 'query_results_' + result.__class__.__name__ + '.csv')

    print('cumulative iteration durations:', cum_exec_times)
    print('average query durations:', q_avg)
    print('all exec times:', q_executions)

    # Stop the SparkSession
    spark.stop()

if __name__ == '__main__':
    main()
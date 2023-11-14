from pyspark import SparkContext, SparkConf
import datetime

# Creating Spark context
conf = SparkConf().setAppName("Log Analysis")
sc = SparkContext(conf=conf)

# Remote file path of the log file
log_file = "/home/hduser/BGL.log"
logs = sc.textFile(log_file)

# Defining the logic for filtering and counting fatal log entries
def filter_and_count(line):
    columns = line.split()
    if columns[8] == "FATAL":
        date_str = columns[2]
        date = datetime.datetime.strptime(date_str, '%Y.%m.%d')
        message = " ".join(columns[9:])
        if date.month == 9 and "major internal error" in message:
            return True
    return False

# Filtering the RDD to keep only the relevant log entries
filtered_logs = logs.filter(filter_and_count)

# Counting the number of fatal log entries
total_count = filtered_logs.count()

# Printing the result
print("Total number of fatal log entries in September with a major internal error:", total_count)

# Stoping Spark context
sc.stop()

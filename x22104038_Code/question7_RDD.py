from pyspark import SparkContext, SparkConf
import datetime
# Function to parse the date from a log line
def date(line):
    columns = line.split()
    date_string = columns[2]
    date = datetime.datetime.strptime(date_string, "%Y.%m.%d")
    return date
# Function getting the start of the week for the date provided
def start_of_week(date):
    start_of_week = date - datetime.timedelta(days=date.weekday())
    return start_of_week
# Function to parse the duration from log
def duration(line):
    columns = line.split()
    date_time = columns[4]
    # Spliting the date and time using the "-" separator
    date_parts = date_time.split("-")
    # Extracting the time part and split it further using the "." separator
    time_parts = date_parts[3].split(".")
    # Extracting the hours, minutes, and seconds from the time part
    hours = int(time_parts[0])
    minutes = int(time_parts[1])
    seconds = int(time_parts[2])
    # Calculating the total duration in seconds
    total_seconds = hours * 3600 + minutes * 60 + seconds
    return total_seconds
# Creating Spark context
conf = SparkConf().setAppName("DDR Error Detection")
sc = SparkContext(conf=conf)
# File path of the log file
log_file = "/home/hduser/BGL.log"
logs = sc.textFile(log_file)
# Mapping each line to (week_start, duration) pair for "ddr errors"
week_durations = logs.filter(lambda line: "ddr errors(s) detected and corrected" in line).map(lambda line: (start_of_week(date(line)), duration(line))).filter(lambda x: x[1] > 0)
# Reducing by key to calculate the total duration for each week
weekly_totals = week_durations.reduceByKey(lambda x, y: x + y)
# Counting the number of occurrences for each week
weekly_counts = week_durations.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
# Calculating the average duration for each week
weekly_averages = weekly_totals.join(weekly_counts).mapValues(lambda x: x[0] / x[1])
# Sorting the results by start date
sorted_averages = weekly_averages.sortByKey()
# Printing the result
for week_start, average_duration in sorted_averages.collect():
    print("Week Start: {}, Average Duration (seconds): {:.2f}".format(week_start.strftime("%Y-%m-%d"), average_duration))

 

# Stop Spark context
sc.stop()

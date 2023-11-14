from pyspark import SparkContext, SparkConf
import datetime

# Function to get the day of the week from a log line
def day_of_week(line):
    columns = line.split()
    timestamp = int(columns[1])
    date = datetime.datetime.fromtimestamp(timestamp)
    day_of_week = date.strftime("%A")
    return day_of_week

# Creating Spark context
conf = SparkConf().setAppName("Day of Week Analysis")
sc = SparkContext(conf=conf)

# Remote file path of the log file
log_file = "/home/hduser/BGL.log"
logs = sc.textFile(log_file)

# Mapping each line to (day_of_week, 1) pair
day_counts = logs.map(lambda line: (day_of_week(line), 1))

# Reducing by key to count occurrences of each day of the week
daily_totals = day_counts.reduceByKey(lambda x, y: x + y)

# Sorting the counts in descending order
sorted_totals = daily_totals.sortBy(lambda x: x[1], ascending=False)

# Taking the top 5 most frequently occurring days
top_5_days = sorted_totals.take(5)

# Printing the result
for day, count in top_5_days:
    print("Day of the week: {}, Count: {}".format(day, count))

sc.stop()

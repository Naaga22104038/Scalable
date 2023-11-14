from pyspark import SparkContext
import re

def process_line(line):
    # Spliting the line into fields using whitespace as a delimiter
    fields = line.strip().split()

    # Checking if the line contains a fatal kernel error with "Power Good signal deactivated" message
    if len(fields) >= 8 and fields[7] == "KERNEL" and fields[8] == "FATAL" and "Power Good signal deactivated" in line:
        # Extract the date from the line
        date = re.search(r'(\d{4}\.\d{2}\.\d{2})', fields[2])
        if date:
            return [date.group(0)]

    return []

# Creating a SparkContext
sc = SparkContext("local", "BGLDataProcessing")

# File path of the log file
log_file = "/home/hduser/BGL.log"

# Loading the log file as an RDD
logs_rdd = sc.textFile(log_file)

# Applying the mapper function to each line and filter out non-matching lines
dates_rdd = logs_rdd.flatMap(process_line)

# Removing empty values from the RDD
filtered_dates= dates_rdd.filter(lambda x: x)

# Finding the earliest date
earliest_date = filtered_dates.min()

# Result
print("Earliest Date of Fatal Kernel Error:", earliest_date)

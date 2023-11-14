from mrjob.job import MRJob
import re

class power(MRJob):

    def mapper(self, _, line):
        # Spliting the line into fields using whitespace as a delimiter
        fields = line.strip().split()

        # Checking if the line contains a fatal kernel error with "Power Good signal deactivated" message
        if len(fields) >= 8 and fields[7] == "KERNEL" and fields[8] == "FATAL" and "Power Good signal deactivated" in line:
            # Extract the date from the line
            date = re.search(r'(\d{4}\.\d{2}\.\d{2})', fields[2])
            if date:
                yield None, date.group(0)

    def reducer(self, key, values):
        # Finding the earliest date
        earliest_date = min(values)
        yield "Earliest Date of Fatal Kernel Error:", earliest_date

if __name__ == '__main__':
    power.run()

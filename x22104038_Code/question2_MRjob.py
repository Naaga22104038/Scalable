import datetime
from mrjob.job import MRJob
from mrjob.step import MRStep

class loganalyzer(MRJob):

    def mapper(self, _, line):
        # Spliting the line contents into columns
        columns = line.split()

        # Checking if the line represents a fatal log entry
        if columns[8] == "FATAL":
            # Extracting the date string from the third column
            date_str = columns[2]
            date = datetime.datetime.strptime(date_str, '%Y.%m.%d')  # Use the correct format string

            # Extracting the message from the last column
            message = " ".join(columns[9:])

            # Checking if the log entry occurred in September and is related to a major internal error
            if date.month == 9 and "major internal error" in message:
                yield "fatal_log_entry", 1

    def reducer(self, key, counts):
        # Suming up the counts of fatal log entries
        total = sum(counts)

        # Yielding the final result
        yield "Total number of fatal log entries in September with a major internal error", total

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer)
        ]

if __name__ == '__main__':
    loganalyzer.run()

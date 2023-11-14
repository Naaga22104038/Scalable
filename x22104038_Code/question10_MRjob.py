import datetime
from mrjob.job import MRJob
from mrjob.step import MRStep

class days(MRJob):

    # Configuring command-line arguments
    def configure_args(self):
        super(MRLogAnalysisJob, self).configure_args()
        self.add_passthru_arg('--log-file', help='Path to the log file')

    # Initializing the mapper with the log file path
    def mapper_init(self):
        self.log_file = self.options.log_file

    # Map function: extracting day of the week and giving a count of 1
    def mapper(self, _, line):
        # Define a function to extract day of the week
        def day_from_week(line):
            columns = line.split()
            timestamp = int(columns[1])
            date = datetime.datetime.fromtimestamp(timestamp)
            day_of_week = date.strftime("%A")
            return day_of_week

        day_of_week = day_from_week(line)
        yield day_of_week, 1

    # Combiner function: combining counts for the same day of the week
    def combiner(self, day_of_week, counts):
        total = sum(counts)
        yield day_of_week, total

    # Reducer function: calculating total occurrences for each day of the week
    def reducer(self, day_of_week, counts):
        total = sum(counts)
        yield None, (total, day_of_week)

    # Reducer initialization for the top 5 days
    def reducer_init(self):
        self.top_5_days = []

    # Function to find the top 5 days with the most occurrences
    def top_5_days_reducer(self, _, occurrences):
        for total, day_of_week in occurrences:
            self.top_5_days.append((total, day_of_week))
            self.top_5_days.sort(reverse=True)
            self.top_5_days = self.top_5_days[:5]

    # Finalizing the top 5 days reducer
    def final(self):
        for total, day_of_week in self.top_5_days:
            yield day_of_week, total

    # Defining the steps for the MapReduce job
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                   mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer_init=self.reducer_init,
                   reducer=self.top_5_days_reducer,
                   reducer_final=self.final)
        ]

# Running the MapReduce 
if __name__ == '__main__':
    days.run()

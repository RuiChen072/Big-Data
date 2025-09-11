import sys
import csv
import re
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env


class proj1(MRJob):

    
    SORT_VALUES = True

    JOBCONF = {
      'mapreduce.map.output.key.field.separator':' ',
      'mapreduce.partition.keypartitioner.options':'-k1,1', 
            'mapreduce.job.output.key.comparator.class':'org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator',
      'stream.num.map.output.key.fields': '2', 
    }



    def configure_args(self):
        super(proj1, self).configure_args()


    def steps(self):
        return [ MRStep(
            mapper=self.mapper,
            combiner=self.combiner_count,
            reducer_init=self.reducer_init,
            reducer=self.reducer,
            reducer_final=self.reducer_final
        ) ]

    def mapper(self, _, value):
        value = value.strip()
        if not value:
            return
    
        line = None
        try:
            row = next(csv.reader([value]))
            if len(row) == 4:
                line = row
        except:
            pass

        if not line:
            line = value.split(',')
            if len(line) != 4:
                return

        # country, _, age_str, sales_str = line
        country = line[0].strip().strip('"')
    
        try:
            age = int(line[2].strip('"'))
            sales = float(line[3].strip('"'))
        except:
            return

        age_group = age - (age % 10)
        # yield country, (sales, 1)
        yield (country, '000_ALL'), (sales, 1)
        yield (country, f'100_{age_group:03d}'), (sales, 1)

    def combiner_count(self, key, values):
        country, tag = key
        total = 0.0
        count = 0
        for s, c in values:
            total += s
            count += c
        yield key, (total, count)

    def reducer_init(self):
        self.tau = float(jobconf_from_env('myjob.settings.tau') or self.options.tau)
                        
        self.current_country = None       
        self.country_avg = None
        self.age_records = []

    def reducer(self, key, values):
        country, tag = key
        total = 0.0
        count = 0
        for s, c in values:
            total += s
            count += c
        avg = total / count

        if self.current_country and self.current_country != country:
           
            for age_avg, group_avg in sorted(self.age_records, key=lambda x: x[0]):
                ads = group_avg / self.country_avg
                if ads >= self.tau:
                    yield self.current_country, f"{age_avg},{ads}"
            self.country_avg = None
            self.age_records = []

        self.current_country = country
        if tag == '000_ALL':
            self.country_avg = avg
        else:
            age_avg = int(tag.split('_',1)[1])
            self.age_records.append((age_avg, avg))

    def reducer_final(self):
        
        if self.current_country:
            for age_avg, group_avg in sorted(self.age_records, key=lambda x: x[0]):
                ads = group_avg / self.country_avg
                if ads >= self.tau:
                    yield self.current_country, f"{age_avg},{ads}"


if __name__ == '__main__':
    proj1.run()


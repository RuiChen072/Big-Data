from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

class Project2:           
    def run(self, inputPath, outputPath, stopwords,topic, k):
        spark = SparkSession.builder.master("local").appName("project2_df").getOrCreate()
        
        # Fill in your code here
        n = int(stopwords)
        m = int(topic)
        k = int(k)
        
        df = spark.read.text(inputPath).select(split(col("value"), ",", 2).getItem(0).alias("date"), split(col("value"), ",", 2).getItem(1).alias("text"))
        df_terms = df.withColumn("terms", split(col("text"), " ")).withColumn("unique_terms", array_distinct(col("terms")))

        exploded = df_terms.select(explode(col("terms")).alias("term"))
        terms_freq = exploded.groupBy("term").count()
        stopwords = [row["term"] for row in terms_freq.orderBy(desc("count"), asc("term")).limit(n).collect()]

        sw_array = array(*[lit(w) for w in stopwords])
        df_filtered = df_terms.withColumn("filtered_terms", array_except(col("unique_terms"), sw_array))
        exploded_ft = df_filtered.select(explode(col("filtered_terms")).alias("term"))
        doc_freq = exploded_ft.groupBy("term").count()
        topics = [row["term"] for row in doc_freq.orderBy(desc("count"), asc("term")).limit(m).collect()]

        df_year = df_filtered.withColumn("year", substring(col("date"), 1, 4))
        df_yt = df_year.select("year", explode(col("filtered_terms")).alias("term")).filter(col("term").isin(topics))
        df_yr_terms = df_yt.groupBy("year", "term").count()

        data = df_yr_terms.collect()
        year_map = {}
        for row in data:
            year_map.setdefault(row["year"],[]).append((row["term"], row["count"]))

        output = []
        for year in sorted(year_map):
            items = sorted(year_map[year], key= lambda x: (-x[1],x[0]))
            covered = len(items)
            topk = items[:k]
            line = f"{year}:{covered}"
            for term, c in topk:
                line += f"\n{term}      {c}"
            output.append(line)
        spark.sparkContext.parallelize(output, 1).saveAsTextFile(outputPath)
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])


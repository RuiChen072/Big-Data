from pyspark import SparkContext, SparkConf
import sys
import math

class Project2:           
    def run(self, inputPath, outputPath, stopwords, topic, k):
        conf = SparkConf().setAppName("project2_rdd").setMaster("local")
        sc = SparkContext(conf=conf)

        # Fill in your code here
        n = int(stopwords)
        m = int(topic)
        k = int(k)
        lines = sc.textFile(inputPath)

        terms = lines.map(lambda line: line.split(',',1)[1].split())
        terms_freq = terms.flatMap(lambda x:x).map(lambda t: (t,1)).reduceByKey(lambda a,b: a+b)
        stopwords_lt = terms_freq.sortBy(lambda y: (-y[1],y[0])).map(lambda y: y[0]).take(n)
        bc_stop = sc.broadcast(set(stopwords_lt))

        doc_terms = terms.map(lambda x: set(x)).map(lambda x: [t for t in x if t not in bc_stop.value])
        doc_terms_freq = doc_terms.flatMap(lambda x: [(t,1) for t in x]).reduceByKey(lambda a,b: a+b)
        topic_lt = doc_terms_freq.sortBy(lambda y: (-y[1],y[0])).map(lambda y: y[0]).take(m)
        bc_topic = sc.broadcast(set(topic_lt))

        year = lines.map(lambda line: (line.split(",",1)[0][:4],set(line.split(",",1)[1].split())))
        yr_terms = year.map(lambda yr: (yr[0], [t for t in yr[1] if t in bc_topic.value])).flatMap(lambda yr: [((yr[0],t),1) for t in yr[1]])
        yr_terms_sum = yr_terms.reduceByKey(lambda a,b : a+b)

        per_yr = yr_terms_sum.map(lambda y: (y[0][0],(y[0][1],y[1]))).groupByKey().mapValues(list)

        def format_line(year, terms_sum):
            items = [(t,c) for t, c in terms_sum if c >0]
            covered = len(items)
            topk = sorted(items, key=lambda y: (-y[1], y[0]))[:k]
            parts = "\n".join(f"{t}     {c}" for t,c in topk)
            return f"{year}:{covered}\n{parts}"
        
        output = per_yr.map(lambda yr: format_line(yr[0], yr[1])).sortBy(lambda line: line.split(":")[0])
        output.coalesce(1).saveAsTextFile(outputPath)


        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])



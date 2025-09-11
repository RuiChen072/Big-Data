import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

class project3:
    def run(self, inputpathA, inputpathB, outputpath, d, s):
        # You can use either RDD or DataFrame APIs
        def run(self, inputpathA, inputpathB, outputpath, d_str, s_str):
            d = float(d_str)
            s = float(s_str)
            #Spark 
            conf = SparkConf().setAppName("SpatioTextualSimilarityJoins")
            sc = SparkContext(conf= conf)

            def parse_line(line):
                parts = line.strip().split('#')
                rid = parts[0]
                coord = parts[1].strip('()').split(',')
                x ,y = float(coord[0]), float(coord[1])
                terms = parts[2].split()
                num = int(rid[1:]) if len(rid) > 1 and rid[1:].isdigit() else 0
                return (rid, num, (x, y), set(terms))

            rddA = sc.textFile(inputpathA).map(parse_line).cache()
            rddB = sc.textFile(inputpathB).map(parse_line).cache()

            terms_count = rddA.flatMap(lambda r: r[3]).union(rddB.flatMap(lambda r: r[3])).map(lambda t: (t,1)).reduceByKey(lambda a,b :a+b)
            freq_map = dict(terms_count.collect())
            bc_freq = sc.broadcast(freq_map)

            def assign_grid(record):
                rid, num, (x,y), terms = record
                grid_x = int(math.floor(x/d))
                grid_y = int(math.floor(x/d))
                for dx in (-1, 0, 1):
                    for dy in (-1, 0, 1):
                        yield ((grid_x + dx, grid_y + dy), record)
            gridA = rddA.flatMap(assign_grid)
            gridB = rddB.flatMap(assign_grid)
            spatial_candidate = gridA.join(gridB).map(lambda p: p[1])

            def euclidean(p1, p2):
                return math.hypot(p1[0] - p2[0], p1[1] - p2[1])

            spatial_filter = spatial_candidate.map(lambda pair: (pair[0], pair[1], euclidean(pair[0][2], pair[1][2]))).filter(lambda x: x[2] <= d)

            def prefix_jaccard(r):
                recA, recB, dist = r
                termsA, termsB = recA[3], recB[3]
                lenA, lenB = len(termsA), len(termsB)
                t = math.ceil(s * (lenA + lenB) / (1.0+s))

                prA = max(lenA - t + 1,0)
                prB = max(lenB - t + 1,0)

                sortedA = sorted(termsA, key = lambda w: bc_freq.value.get(w, 0))
                sortedB = sorted(termsB, key = lambda w: bc_freq.value.get(w, 0))
                prefixA, prefixB = sortedA[:prA], sortedB[:prB]

                if not set(prefixA).intersection(prefixB):
                    return  None
                
                inter = len(termsA & termsB)
                union = len(termsA | termsB)
                js = float(inter) / union if union > 0 else 0.0
                if js >= s:
                    return (recA[1],recB[1], recA[0], recB[0], dist, js)
                return None
            filtered = spatial_filter.map(prefix_jaccard).filter(lambda x: x is not None)

            results = filtered.distinct().sortBy(lambda x: (x[0], x[1]))
            output = results.map(lambda r: f"({r[2]}, {r[3]}):{r[4]:.6f}, {r[5]:.6f}")
            output.saveAsTextFile(outputpath)
            sc.stop()
        
if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    project3().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

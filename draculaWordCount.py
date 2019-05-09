from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("draculaJob").getOrCreate()

df = spark.read.text("/Users/quikr/BigData_Playground/Spark/GreatLearning/dracula.txt");

words = df.rdd.flatMap(lambda line: line.value.strip("  ").split(" "))

words_map = words.map(lambda word: (word, 1))

words_reduced_map = words_map.reduceByKey(lambda a,b: a + b)

words_reduced_map.take(50)

words_reduced_map.saveAsTextFile("dracula_wordCount")

spark.stop()


from pyspark.sql import Row
from pyspark.sql import SparkSession


def problem_1(spark):	
    sc = spark.sparkContext
    lines = sc.textFile("/home/mqp/input/transaction.csv")
    parts = lines.map(lambda l: l.split(","))
    trans = parts.map(lambda t: Row(transID=t[0], cusID=t[1], transTotal=float(t[2]), transNum=int(t[3]), transDesc=t[4]))
    schemaTrans = spark.createDataFrame(trans)
    schemaTrans.createOrReplaceTempView("transaction")
    t1 = spark.sql("SELECT * FROM transaction WHERE transTotal>=200")
    t1.createOrReplaceTempView("t1")
    t2 = spark.sql("SELECT sum(transTotal),avg(transTotal),min(transTotal),max(transTotal) FROM t1 GROUP BY transNum")
    t2.show()
    t3 = spark.sql("SELECT cusID, count(*) as ct FROM t1 GROUP BY cusID")
    t3.createOrReplaceTempView("t3")
    t4 = spark.sql("SELECT * FROM transaction WHERE transTotal>=600")
    t4.createOrReplaceTempView("t4")
    t5 = spark.sql("SELECT cusID, count(*) as ct FROM t4 GROUP BY cusID")
    t5.createOrReplaceTempView("t5")
    t6 = spark.sql("SELECT t3.cusID FROM t3,t5 WHERE t3.cusID=t5.cusID and t3.ct>3*t5.ct")
    t6.show()


if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("Problem 1") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    problem_1(spark)
    spark.stop()

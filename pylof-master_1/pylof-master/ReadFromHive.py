from pyspark.sql import SparkSession


# if __name__ == '__main__':

def getInstance():
    spark = SparkSession \
        .builder \
        .appName("readHive") \
        .enableHiveSupport() \
        .getOrCreate()

    filterDF=spark.sql(
        """SELECT cast(trim(longitude) as string) longitude,cast(trim(latitude) as string) latitude,eventtime currentTime FROM dc.ods_gps
    WHERE workdate='20171207' and gprsId='671' and citycode='130400'""")
    list=[]
    filterDF.select(filterDF.longitude,filterDF.latitude).show()
    filterDF.createOrReplaceTempView("filter")
    personsDF = spark.sql("select * from filter")
    list=personsDF.rdd.map(lambda t: (float(t[0]), float(t[1]))).collect()
    # personsDF.rdd.foreach(lambda t:list.append(t))
    return list
    # list.foreach(print)
    #  print(list)
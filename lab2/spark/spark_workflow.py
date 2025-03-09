#from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
import random



def main():
    spark = SparkSession.builder \
                        .appName("RandomDataExample") \
                        .master("spark://spark-master:7077") \
                        .getOrCreate()
                        #.config("spark.driver.memory", "4g") \
                        #.config("spark.executor.memory", "4g") \
                        #.config("spark.cores.max", "2") \
                            
                        
                        

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("value", FloatType(), False),
    ])


    data = [(i, random.random()) for i in range(100)] 

    df = spark.createDataFrame(data, schema=schema)

    print("Первые 10 строк данных: ", df.show(10))
    
    filtered_df = df.filter(df["value"] > 0.5)
    grouped_df = filtered_df.groupBy("value").count()
    print("Сгруппированные по значениям: ", grouped_df.show(10))
    spark.stop()
    
if __name__ == '__main__':
    main()



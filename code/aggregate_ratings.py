from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.sql.functions import udf

if __name__ == "__main__":
    #Instantiate a new spark session
    sparkSession = SparkSession\
        .builder\
        .appName("join")\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    
    #define the schema
    personal_details_schema=StructType([StructField('Customer_ID',StringType(),True),\
                                        StructField('Gender',StringType(),True),\
                                        StructField('Age',StringType(),True)])

    # read customer details
    customerDF=sparkSession.read\
        .format("csv").option("header","true")\
            .schema(personal_details_schema)\
            .load("../datasets/customerDatasets/static_datasets/join_static_personal_details.csv")
    #transactional schema                                       
    transaction_details_schema=StructType([StructField('Customer_ID',StringType(),True),\
                                        StructField('Transaction_amount',StringType(),True),\
                                        StructField('Transaction_Rating',StringType(),True)])
    # read transaction details
    fileStreamDF =sparkSession.readStream\
                            .option("header","true")\
                            .option("maxFilesPerTrigger",1)\
                            .schema(transaction_details_schema)\
                            .csv("../datasets/customerDatasets/streaming_datasets/join_streaming_transaction_details")
    # join 2 data sets
    joinDF =customerDF.join(fileStreamDF,"Customer_ID")
    
    #split customers by age groups
    def add_age_group(age):
        if int(age) <25: return 'less than 25'
        elif int(age) <40: return '25 to 40'
        else: return 'more than 40'
    
    #create UDF
    add_age_group_udf=udf(
                            add_age_group,
                            StringType()
                         )
    # group by geneder and aggregate transaction amount
    ratings_age_group =joinDF.withColumn("Age_Group",
                                            add_age_group_udf(joinDF.Age))
    # aggregate the age group
    ratings_age_group=ratings_age_group.groupby('Age_Group').agg({"Transaction_Rating":"avg"})
    
    # round function
    def round_func(amount):
        return ("%.2f"% amount)
    
    #define udf
    round_udf=udf(
                    round_func,
                    StringType()
                )
    #add average transaction amount
    spending_per_gender=ratings_age_group.withColumn(
                                                       "Average_Transaction_Rating",
                                                        round_udf("avg(Transaction_Rating)")
                                                    )\
                                            .drop("avg(Transaction_Rating)")
    #query
    query= spending_per_gender\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()\
        .awaitTermination()
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lit, to_timestamp, lead, min
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("fpt_PoC").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Exercise 1

flight = spark.read.format('csv')\
            .option('header', True)\
            .option('delimiter', ';')\
            .load('./src/flight_source.csv')

flight = flight.withColumn('actl_dep_lcl_tms', to_timestamp('actl_dep_lcl_tms'))\
                     .withColumn('actl_arr_lcl_tms', to_timestamp('actl_arr_lcl_tms'))\
                     .withColumn('airborne_lcl_tms', to_timestamp('airborne_lcl_tms'))\
                     .withColumn('landing_lcl_tms', to_timestamp('landing_lcl_tms'))

windowSpec = Window.partitionBy(col('acft_regs_cde'))\
                     .orderBy(col('actl_dep_lcl_tms'))

flight = flight.withColumn('next_flight_id', lead(col('id')).over(windowSpec))

flight.show()

# Exercise 2

# Define date start regarding our dataset
min_date = flight.select(min('actl_dep_lcl_tms')).first()[0]
start_date = datetime(min_date.year, min_date.month, min_date.day, 0, 0, 0)

# Multiply the hours of the day by the fraction per hour that it require 
num_intervals = 4 * 24
interval_minutes = 15

# Create dataframe with iteration results
df_date = spark.range(num_intervals)
df_date = df_date.withColumn('tms', lit(start_date + df_date.id * timedelta(minutes=interval_minutes)))\
                 .drop('id')

# Identify the airports uniques in our dataset
airport_id = flight.select('orig').distinct()
flight_out_in = airport_id.crossJoin(df_date)
flight_out_in = flight_out_in.withColumnRenamed('orig', 'airport_code')

# Create flight_date dataframe
flight_date = flight.join(flight_out_in, (flight.orig == flight_out_in.airport_code)\
                     & (flight.actl_dep_lcl_tms >= flight_out_in.tms - timedelta(hours=2))\
                     & (flight.actl_dep_lcl_tms <= flight_out_in.tms), 'right_outer')

# Create out and in columns
flight_out = flight_date.groupBy(col('orig').alias('orig_out'), col('tms').alias('tms_out')).count().withColumnRenamed('count', 'out')
flight_in = flight_date.groupBy(col('dest').alias('dest_in'), col('tms').alias('tms_in')).count().withColumnRenamed('count', 'in')

# Join flight_out and flight_in with flight_out_in
flight_out_in = flight_out_in.join(flight_out, (flight_out_in.airport_code == flight_out.orig_out)\
                                & (flight_out_in.tms == flight_out.tms_out), 'left_outer')\
                                .join(flight_in, (flight_out_in.airport_code == flight_in.dest_in)\
                                & (flight_out_in.tms == flight_in.tms_in), 'left_outer')\
                                .select('airport_code', 'tms', 'out', 'in')

# Fill null values with 0
flight_out_in = flight_out_in.fillna(0)

flight_out_in.show()





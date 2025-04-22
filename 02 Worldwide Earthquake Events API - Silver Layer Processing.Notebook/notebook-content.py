# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "da9f7461-3a2c-419e-b9e7-1682dda9a8bb",
# META       "default_lakehouse_name": "earthquake_lakehouse",
# META       "default_lakehouse_workspace_id": "a681b978-057d-4235-a33b-0f94ab11513e"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Worldwide Earthquake Events API - Silver Layer Processing

# CELL ********************

from pyspark.sql.functions import col
from pyspark.sql.types import TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# df now is a Spark DataFrame containing JSON data
df = spark.read.option("multiline", "true").json(f"Files/{start_date}_earthquake_data.json")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reshape earthquake data by extracting and renaming key attributes for further analysis.
df = \
df.\
    select(
        'id',
        col('geometry.coordinates').getItem(0).alias('longitude'),
        col('geometry.coordinates').getItem(1).alias('latitude'),
        col('geometry.coordinates').getItem(2).alias('elevation'),
        col('properties.title').alias('title'),
        col('properties.place').alias('place_description'),
        col('properties.sig').alias('sig'),
        col('properties.mag').alias('mag'),
        col('properties.magType').alias('magType'),
        col('properties.time').alias('time'),
        col('properties.updated').alias('updated')
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Convert 'time' and 'updated' columns from milliseconds to timestamp format for clearer datetime representation.
df = df.\
    withColumn('time', col('time')/1000).\
    withColumn('updated', col('updated')/1000).\
    withColumn('time', col('time').cast(TimestampType())).\
    withColumn('updated', col('updated').cast(TimestampType()))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# appending the data to the gold table
df.write.mode('append').saveAsTable('earthquake_events_silver')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

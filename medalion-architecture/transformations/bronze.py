
from pyspark.sql import functions as F

import dlt  # Ensure dlt is imported for Lakeflow Declarative Pipelines

@dlt.table
def airbnb_listings_bronze():
    listings = (
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'csv')  # Correct option name
        .option('header', 'true')
        .option('inferSchema', 'true')
        .option("quote", '"')  # <- handles fields with commas
        .option("escape", '"')  # <- handles quotes inside the field
        .option("multiline", "true")
        .option('schemaLocation', 's3://abdullah-de-airbnb-catalog/schema/listings_raw/')
        .option('cloudFiles.includeExistingFiles', 'true')
        .load('s3://abdullah-de-airbnb-raw/*/*/*/*/*/listings.csv')
    ).withColumn('metadata', F.col('_metadata'))

    return listings

@dlt.table
def airbnb_reviews_bronze():
    reviews = (
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'csv')  # Correct option name
        .option('header', 'true')
        .option('inferSchema', 'true')
        .option("quote", '"')  # <- handles fields with commas
        .option("escape", '"')  # <- handles quotes inside the field
        .option("multiline", "true")
        .option('schemaLocation', 's3://abdullah-de-airbnb-catalog/schema/reviews_raw/')
        .option('cloudFiles.includeExistingFiles', 'true')
        .load('s3://abdullah-de-airbnb-raw/*/*/*/*/*/reviews.csv')
    ).withColumn('metadata', F.col('_metadata'))

    return reviews

@dlt.table
def airbnb_neighbourhoods_bronze():
    nh = (
        spark.readStream
        .format('cloudFiles')
        .option('cloudFiles.format', 'csv')  # Correct option name
        .option('header', 'true')
        .option('inferSchema', 'true')
        .option("quote", '"')  # <- handles fields with commas
        .option("escape", '"')  # <- handles quotes inside the field
        .option("multiline", "true")
        .option('schemaLocation', 's3://abdullah-de-airbnb-catalog/schema/neighbourhoods_raw/')
        .option('cloudFiles.includeExistingFiles', 'true')
        .load('s3://abdullah-de-airbnb-raw/*/*/*/*/*/neighbourhoods.csv')
    ).withColumn('metadata', F.col('_metadata'))

    return nh
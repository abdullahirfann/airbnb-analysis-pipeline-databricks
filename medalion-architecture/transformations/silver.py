import pyspark.sql.functions as F
import dlt


@dlt.table(
  name="airbnb_cities_silver"
)
def airbnb_cities_silver():
    return (
        dlt.read_stream("airbnb_listings_bronze")
        .select(
            F.split(
                F.replace("metadata.file_path", F.lit("s3://abdullah-de-airbnb-raw/"), F.lit("")),
                F.lit("/")
            )[0].alias("country"),
            F.split(
                F.replace("metadata.file_path", F.lit("s3://abdullah-de-airbnb-raw/"), F.lit("")),
                F.lit("/")
            )[1].alias("city"),
        )
        .dropDuplicates(["country", "city"])
    )

@dlt.table(
    name="airbnb_neighbourhoods_silver"
)
def airbnb_neighbourhoods_silver():
    return (
        dlt.read_stream("airbnb_neighbourhoods_bronze")
        .select(
            F.split(
                F.replace("metadata.file_path", F.lit("s3://abdullah-de-airbnb-raw/"), F.lit("")),
                F.lit("/")
            )[0].alias("country"),
            F.split(
                F.replace("metadata.file_path", F.lit("s3://abdullah-de-airbnb-raw/"), F.lit("")),
                F.lit("/")
            )[1].alias("city"),
            F.col("neighbourhood_group"),
            F.col("neighbourhood"),
        )
        .dropDuplicates([
            "country",
            "city",
            "neighbourhood_group",
            "neighbourhood"
        ])
    )

@dlt.table(name="airbnb_hosts_silver")
def airbnb_hosts_silver():
    return (
        dlt.read_stream("airbnb_listings_bronze")
        .select(
            F.col("host_id").alias("id"),
            "host_url",
            "host_name",
            F.to_date("host_since").alias("host_since"),
            "host_location",
            F.trim("host_about").alias("host_about"),
            "host_response_time",
            F.when(F.col("host_response_rate") == "N/A", None)
             .otherwise(F.regexp_replace("host_response_rate", "%", "").cast("int"))
             .alias("host_response_rate"),
            F.when(F.col("host_acceptance_rate") == "N/A", None)
             .otherwise(F.regexp_replace("host_acceptance_rate", "%", "").cast("int"))
             .alias("host_acceptance_rate"),
            (F.col("host_is_superhost") == "t").alias("host_is_superhost"),
            "host_thumbnail_url",
            "host_picture_url",
            "host_neighbourhood",
            F.coalesce(
                "host_total_listings_count",
                "calculated_host_listings_count"
            ).alias("host_total_listings_count"),
            F.transform(
                F.split(
                    F.regexp_replace("host_verifications", r"[\[\]']", ""),  # note raw string `r""`
                    ","
                ),
                lambda x: F.trim(x)
            ).alias("host_verifications"),
            (F.col("host_has_profile_pic") == "t").alias("host_has_profile_pic"),
            (F.col("host_identity_verified") == "t").alias("host_identity_verified"),
        )
        .dropDuplicates(["id"])
    )

@dlt.table(name="airbnb_listings_silver")
def airbnb_listings_silver():
    return (
        dlt.read_stream("airbnb_listings_bronze")
        .select(
            "id",
            "listing_url",
            "name",
            "description",
            "neighborhood_overview",
            "picture_url",
            "host_id",
            F.split(
                F.replace("metadata.file_path", F.lit("s3://abdullah-de-airbnb-raw/"), F.lit("")),
                F.lit("/")
            )[0].alias("country"),
            F.split(
                F.replace("metadata.file_path", F.lit("s3://abdullah-de-airbnb-raw/"), F.lit("")),
                F.lit("/")
            )[1].alias("city"),
            F.col("neighbourhood_group_cleansed").alias("neighbourhood_group"),
            F.col("neighbourhood_cleansed").alias("neighbourhood"),
            F.struct(
                F.col("latitude").cast("double"),
                F.col("longitude").cast("double")
            ).alias("coordinates"),
            "property_type",
            "room_type",
            F.col("accommodates").cast("int"),
            F.col("bathrooms").cast("double"),
            F.col("bedrooms").cast("int"),
            F.col("beds").cast("int"),
            F.transform(
                F.split(
                    F.regexp_replace("amenities", r"[\[\]']", ""),  # note raw string `r""`
                    ","
                ),
                lambda x: F.trim(x)
            ).alias("amenities"),
            F.regexp_replace("price", "[^0-9.]", "").cast("double").alias("price"),
            F.col("maximum_nights").cast("int"),
            F.col("minimum_nights").cast("int"),
            F.col("minimum_minimum_nights").cast("int"),
            F.col("maximum_minimum_nights").cast("int"),
            F.col("minimum_maximum_nights").cast("int"),
            F.col("maximum_maximum_nights").cast("int"),
            F.col("estimated_occupancy_l365d").cast("double").alias("estimated_occupancy"),
            F.col("estimated_revenue_l365d").cast("double").alias("estimated_revenue"),
            F.col("review_scores_rating").cast("double"),
            F.col("review_scores_accuracy").cast("double"),
            F.col("review_scores_communication").cast("double"),
            F.col("review_scores_checkin").cast("double"),
            F.col("review_scores_cleanliness").cast("double"),
            F.col("review_scores_location").cast("double"),
            F.col("review_scores_value").cast("double"),
            (F.col("instant_bookable") == "t").alias("instant_bookable"),
            F.col("reviews_per_month").cast("double"),
        )
        .dropDuplicates(["id"])
    )

@dlt.table(name="airbnb_reviewers_silver")
def airbnb_reviewers_silver():
    return (
        dlt.read_stream("airbnb_reviews_bronze")
        .select("reviewer_id", "reviewer_name")
        .where("reviewer_id IS NOT NULL")
        .dropDuplicates(["reviewer_id"])
        .withColumnRenamed("reviewer_id", "id")
    )


@dlt.table(name="airbnb_reviews_silver")
def airbnb_reviews_silver():
    return (
        dlt.read_stream("airbnb_reviews_bronze")
        .withColumn("date", F.to_date("date"))
        .dropDuplicates(["id"])
    )
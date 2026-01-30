import dlt
import pyspark.sql.functions as F
from pyspark.sql.window import Window

# ------------------------------
# Host-level listing counts
# ------------------------------
@dlt.table(
    name="airbnb_host_listing_counts_gold",
    comment="Batch aggregation of host-level listing counts"
)
def airbnb_host_listing_counts_gold():
    hosts = dlt.read("airbnb_hosts_silver")
    listings = dlt.read("airbnb_listings_bronze")

    return (
        hosts.alias("h")
        .join(
            listings.alias("l"),
            F.col("h.id") == F.col("l.host_id"),
            "inner"
        )
        .groupBy(F.col("l.host_id").alias("host_id"))
        .agg(
            F.first(F.col("h.host_name"), ignorenulls=True).alias("name"),
            F.count(F.lit(1)).alias("listing_count")
        )
    )


# ------------------------------
# Host revenue aggregation
# ------------------------------
@dlt.table(
    name="airbnb_hosts_revenue_gold",
    comment="Batch aggregation of total estimated revenue per host"
)
def airbnb_hosts_revenue_gold():
    hosts = dlt.read("airbnb_hosts_silver")
    listings = dlt.read("airbnb_listings_silver").filter(
        F.col("estimated_revenue").isNotNull()
    )

    return (
        hosts.alias("h")
        .join(
            listings.alias("l"),
            F.col("h.id") == F.col("l.host_id"),
            "inner"
        )
        .groupBy(F.col("h.id").alias("id"))
        .agg(
            F.first(F.col("h.host_name"), ignorenulls=True).alias("name"),
            F.concat(
                F.lit("$"),
                F.sum(F.col("l.estimated_revenue")).cast("string")
            ).alias("total_revenue")
        )
    )


# ------------------------------
# Host reviews aggregation
# ------------------------------
@dlt.table(
    name="airbnb_hosts_reviews_gold",
    comment="Batch aggregation of reviews per month and average rating per host"
)
def airbnb_hosts_reviews_gold():
    hosts = dlt.read("airbnb_hosts_silver")
    listings = dlt.read("airbnb_listings_silver").filter(
        F.col("review_scores_rating").isNotNull()
    )

    return (
        hosts.alias("h")
        .join(
            listings.alias("l"),
            F.col("h.id") == F.col("l.host_id"),
            "inner"
        )
        .groupBy(F.col("h.id").alias("id"))
        .agg(
            F.first(F.col("h.host_name"), ignorenulls=True).alias("name"),
            F.round(F.sum(F.col("l.reviews_per_month")), 2).alias("reviews_per_month"),
            F.round(F.avg(F.col("l.review_scores_rating")), 2).alias("average_rating")
        )
    )


# ------------------------------
# City ratings aggregation
# ------------------------------
@dlt.table(
    name="airbnb_city_ratings_gold",
    comment="Batch aggregation of average rating per city and country"
)
def airbnb_city_ratings_gold():
    cities = dlt.read("airbnb_cities_silver")
    listings = dlt.read("airbnb_listings_silver")

    cleaned_cities = cities.withColumn(
        "city_clean",
        F.initcap(F.regexp_replace(F.trim(F.col("city")), "[-_]+", " "))
    )

    return (
        cleaned_cities.alias("c")
        .join(
            listings.alias("l"),
            (F.col("c.city") == F.col("l.city")) & (F.col("c.country") == F.col("l.country")),
            "left"
        )
        .groupBy(
            F.col("c.country").alias("country"),
            F.col("city_clean").alias("city")
        )
        .agg(
            F.round(F.avg(F.col("l.review_scores_rating")), 2).alias("average_rating")
        )
    )


# ------------------------------
# Neighbourhood ratings aggregation
# ------------------------------
@dlt.table(
    name="airbnb_neighbourhood_ratings_gold",
    comment="Batch aggregation of average review scores per neighbourhood"
)
def airbnb_neighbourhood_ratings_gold():
    neighbourhoods = dlt.read("airbnb_neighbourhoods_silver")
    listings = dlt.read("airbnb_listings_silver")

    neighbourhoods_clean = neighbourhoods.withColumn(
        "ng_group",
        F.coalesce(F.col("neighbourhood_group"), F.lit("NULL"))
    )
    listings_clean = listings.withColumn(
        "ng_group",
        F.coalesce(F.col("neighbourhood_group"), F.lit("NULL"))
    )

    return (
        neighbourhoods_clean.alias("n")
        .join(
            listings_clean.alias("l"),
            (F.col("n.country") == F.col("l.country")) &
            (F.col("n.city") == F.col("l.city")) &
            (F.col("n.ng_group") == F.col("l.ng_group")) &
            (F.col("n.neighbourhood") == F.col("l.neighbourhood")),
            "left"
        )
        .groupBy(
            F.col("n.country").alias("country"),
            F.col("n.city").alias("city"),
            F.col("n.neighbourhood_group").alias("neighbourhood_group"),
            F.col("n.neighbourhood").alias("neighbourhood")
        )
        .agg(
            F.round(F.avg(F.col("l.review_scores_rating")), 2).alias("avg_reviews")
        )
    )


# ------------------------------
# Listing skew vs host average
# ------------------------------
@dlt.table(
    name="airbnb_listing_skew_gold",
    comment="Batch calculation of listing review skew vs host average"
)
def airbnb_listing_skew_gold():
    hosts = dlt.read("airbnb_hosts_silver").alias("hosts")
    listings = dlt.read("airbnb_listings_silver").alias("listings")

    # Compute average review per host
    host_avg_reviews = (
        listings.groupBy("host_id")
        .agg(F.round(F.avg(F.col("review_scores_rating")), 2).alias("user_reviews_avg"))
    )

    # Join listings with host info and host averages
    joined = (
        listings
        .join(hosts, listings.host_id == hosts.id, "inner")
        .join(host_avg_reviews, "host_id", "inner")
        .groupBy(
            F.col("hosts.id").alias("host_id"),
            F.col("listings.id").alias("listing_id")
        )
        .agg(
            F.first(F.col("hosts.host_name"), ignorenulls=True).alias("host_name"),
            F.first(F.col("listings.name"), ignorenulls=True).alias("listing_name"),
            F.first(F.col("user_reviews_avg")).alias("user_reviews_avg"),
            F.first(F.col("listings.review_scores_rating")).alias("listing_review"),
            F.round(
                F.first(F.col("listings.review_scores_rating")) - F.first(F.col("user_reviews_avg")), 2
            ).alias("skew")
        )
    )

    return joined



# ------------------------------
# AI sentiment and categories
# ------------------------------
@dlt.table(
    name="airbnb_reviews_gold",
    comment="Batch reviews with AI sentiment and extracted categories"
)
def airbnb_reviews_gold():
    reviews = dlt.read("airbnb_reviews_silver")

    return (
        reviews.select(
            F.col("id"),
            F.col("listing_id"),
            F.col("date"),
            F.col("reviewer_id"),
            F.col("reviewer_name"),
            F.expr("ai_analyze_sentiment(comments)").alias("sentiment"),
            F.expr("""
                TRANSFORM(
                    SPLIT(
                        ai_gen(
                            CONCAT(
                                "I have the following fixed set of classifiers for Airbnb reviews:\n\n",
                                "Cleanliness, Location, Host Interaction, Amenities, Value for Money, Check-in / Check-out, Noise / Disturbances, Safety / Security, Maintenance, Food / Breakfast, Transport / Parking, Miscellaneous\n\n",
                                "Please output **only** these classifiers as a **comma-separated list**, without any extra text, quotes, or explanation.\n\n",
                                "Review:\n",
                                comments
                            )
                        ),
                        ","
                    ),
                    x -> TRIM(x)
                )
            """).alias("review_categories")
        )
    )


@dlt.table(
    name="airbnb_listing_top_categories_gold",
    comment="Batch table of top 5 review categories per listing"
)
def airbnb_listing_top_categories_gold():
    # Read static tables
    hosts = dlt.read("airbnb_hosts_silver")
    listings = dlt.read("airbnb_listings_silver")
    reviews = dlt.read("airbnb_reviews_gold")

    # 1️⃣ Explode review categories and count per listing per category
    category_counts = (
        reviews
        .select("listing_id", F.explode("review_categories").alias("category"))
        .groupBy("listing_id", "category")
        .agg(F.count("*").alias("count"))
    )

    # 2️⃣ Aggregate categories per listing with structs
    categories_per_listing = (
        listings
        .join(hosts, listings.host_id == hosts.id, "inner")
        .join(category_counts, listings.id == category_counts.listing_id, "inner")
        .groupBy(
            hosts.id.alias("id"),
            listings.id.alias("listing_id")
        )
        .agg(
            F.first(hosts.host_name, ignorenulls=True).alias("name"),
            F.first(listings.name, ignorenulls=True).alias("listing_name"),
            F.array_sort(
                F.collect_list(
                    F.struct(category_counts.category.alias("category"), category_counts["count"])
                ),
                # Sort descending by count
                lambda x, y: F.when(x["count"] > y["count"], -1)
                             .when(x["count"] < y["count"], 1)
                             .otherwise(0)
            ).alias("sorted_categories")
        )
    )

    # 3️⃣ Pick top 5 categories and extract only category names
    result = categories_per_listing.select(
        "id",
        "name",
        "listing_id",
        "listing_name",
        F.slice(
            F.expr("TRANSFORM(sorted_categories, x -> x.category)"),
            1,
            5
        ).alias("top_5_categories")
    )

    return result


# base_path = "abfss://olistdatagolistdatastorageaccount.dfs.core.windows.net/bronze/"

# orders_path = base_path + "olist_orders_dataset.csv"
# payments_path = base_path + "olist_order_payments_dataset.csv"
# reviews_path = base_path + "olist_order_reviews_dataset.csv"
# items_path = base_path + "olist_order_items_dataset.csv"
# customers_path = base_path + "olist_customers_dataset.csv"
# sellers_path = base_path + "olist_sellers_dataset.csv"
# geolocation_path = base_path + "olist_geolocation_dataset.csv"
# products_path = base_path + "olist_products_dataset.csv"

# orders_df = spark.read.format("csv").option("header", "true").load(orders_path)
# payments_df = spark.read.format("csv").option("header", "true").load(payments_path)
# reviews_df = spark.read.format("csv").option("header", "true").load(reviews_path)
# items_df = spark.read.format("csv").option("header", "true").load(items_path)
# customers_df = spark.read.format("csv").option("header", "true").load(customers_path)
# sellers_df = spark.read.format("csv").option("header", "true").load(sellers_path)
# geolocation_df = spark.read.format("csv").option("header", "true").load(geolocation_path)
# products_df = spark.read.format("csv").option("header", "true").load(products_path)

# display(product_category_df)
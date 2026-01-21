# from pymongo import MongoClient

# hostname = "p8io3.h.filess.io"
# database = "olistDataNoSQL_becomingit"
# port = "27018"
# username = "olistDataNoSQL_becomingit"
# password = "dcea9f94c68907e3b1f73fcbba26645ef83470e3"

# uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# # Connect with the portnumber and host
# client = MongoClient(uri)

# # Access database
# mydatabase = client[database]

     
import pandas as pd
from pymongo import MongoClient

# Load the product_category CSV file into a pandas DataFrame
try:
  product_category_df = pd.read_csv("product_category_name_translation.csv")
except FileNotFoundError:
  print("Error: 'product_category_name_translation.csv' not found.")
  exit() # Exit the script if the file is not found


# MongoDB connection details (assuming these are already defined in your script)
hostname = "p8io3.h.filess.io"
database = "olistDataNoSQL_becomingit"
port = "27018"
username = "olistDataNoSQL_becomingit"
password = "dcea9f94c68907e3b1f73fcbba26645ef83470e3"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

try:
    # Establish a connection to MongoDB
    client = MongoClient(uri)
    db = client[database]

    # Select the collection (or create if it doesn't exist)
    collection = db["product_categories"]  # Choose a suitable name for your collection

    # Convert the DataFrame to a list of dictionaries for insertion into MongoDB
    data_to_insert = product_category_df.to_dict(orient="records")

    # Insert the data into the collection
    collection.insert_many(data_to_insert)

    print("Data uploaded to MongoDB successfully!")


except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the MongoDB connection
    if client:
        client.close()



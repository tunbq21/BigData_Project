from pyspark.sql import SparkSession
from pyspark.sql import functions 



spark = SparkSession.builder.appName("DatabricksConnectAzureStorage")\
        .getOrCreate() # Create Spark Session if not exists


# ==========================================
# 1. CẤU HÌNH THÔNG SỐ (CHỈ SỬA PHẦN NÀY)
# ==========================================
storage_account = "datastaccount" #example: yourstorageaccount
container_name  = "olistdata" #example: yourcontainername
folder_path     = "bronze"  # Để trống "" nếu file nằm ở root container 

# Thông tin từ App Registration (Service Principal)
application_id  = "" 
directory_id    = ""
secret_value    = ""

# ==========================================
# 2. THIẾT LẬP KẾT NỐI (GIỮ NGUYÊN)
# ==========================================
# Cấu hình xác thực OAuth2
spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", secret_value)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# Tạo đường dẫn gốc (Base Path)
base_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{folder_path}/"

print(f"Kết nối thành công tới: {base_path}")

# ==========================================
# 3. VÍ DỤ ĐỌC FILE (SỬ DỤNG DISPLAY ĐỂ TRÁNH LỖI)
# ==========================================
# file_name = "olist_orders_dataset.csv"
# df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(base_path + file_name)
df = spark.read.csv(base_path + "product_category_name_translation.csv", header=True, inferSchema=True)
# display(df.limit(5))
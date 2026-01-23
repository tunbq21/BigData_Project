from pyspark.sql.functions import col, when, lit, min, max, sum, avg, count, upper, trim, datediff, col, row_number, spark_sum
from pyspark.sql.window import Window

# def cast_column_if_exists(df, column_name, new_type):#Cast the column to new_type if it exists
#     if column_name in df.columns:
#         return df.withColumn(column_name, col(column_name).cast(new_type))
#     return df

# def delete_column_if_exists(df, column_name):#Delete the column if it exists
#     if column_name in df.columns:
#         return df.drop(column_name)           
#     return df


# Example: Cast '_id' column to string if it exists
# orders_df = cast_column_if_exists(orders_df, "_id", "string")
# display(orders_df)

# # Example: Delete '_id' column if it exists
# orders_df_deleted = delete_column_if_exists(orders_df, "_id")
# display(orders_df_deleted)


# ==================================================================


def cast_column_if_exists(df, column_name, target_type):
    """
    Ép kiểu cột nếu cột đó tồn tại trong DataFrame.
    target_type: "string", "int", "double", "date", "timestamp", v.v.
    """
    if column_name in df.columns:
        return df.withColumn(column_name, col(column_name).cast(target_type))
    else:
        print(f"Bỏ qua Cast: Cột '{column_name}' không tồn tại.")
        return df

# ================================================================================================================

def delete_column_if_exists(df, column_name):
    """
    Xóa cột nếu cột đó tồn tại trong DataFrame.
    """
    if column_name in df.columns:
        return df.drop(column_name)
    else:
        print(f"Bỏ qua Xóa: Cột '{column_name}' không tồn tại.")
        return df
    
# ================================================================================================================

def filter_data(df, condition_string):
    """
    Lọc dữ liệu dựa trên chuỗi điều kiện (ví dụ: "price > 100").
    """
    try:
        return df.filter(condition_string)
    except Exception as e:
        print(f"Lỗi khi lọc với điều kiện '{condition_string}': {e}")
        return df
    
# ================================================================================================================

def add_conditional_col(df, new_col, base_col, condition, value_if_true, value_if_false):
    """
    Tạo cột mới dựa trên điều kiện của cột cũ.
    Ví dụ: df = add_conditional_col(df, "is_expensive", "price", col("price") > 100, "Yes", "No")
    """
    if base_col in df.columns:
        return df.withColumn(new_col, when(condition, value_if_true).otherwise(value_if_false))
    return df

# ================================================================================================================

def rename_column_if_exists(df, old_col_name, new_col_name):
    """
    Đổi tên cột nếu cột đó tồn tại trong DataFrame.
    """
    if old_col_name in df.columns:
        return df.withColumnRenamed(old_col_name, new_col_name)
    else:
        print(f"Bỏ qua Đổi tên: Cột '{old_col_name}' không tồn tại.")
        return df

# ================================================================================================================

def fill_nulls_in_column(df, column_name, fill_value):
    """
    Điền giá trị vào các ô null trong cột nếu cột đó tồn tại.
    """
    if column_name in df.columns:
        return df.fillna({column_name: fill_value})
    else:
        print(f"Bỏ qua Điền null: Cột '{column_name}' không tồn tại.")
        return df
    
# ================================================================================================================

def drop_duplicates_if_column_exists(df, column_name):
    """
    Xóa các bản ghi trùng lặp dựa trên cột nếu cột đó tồn tại.
    """
    if column_name in df.columns:
        return df.dropDuplicates([column_name])
    else:
        print(f"Bỏ qua Xóa trùng lặp: Cột '{column_name}' không tồn tại.")
        return df
    
# ================================================================================================================


def sort_dataframe_if_column_exists(df, column_name, ascending=True):
    """
    Sắp xếp DataFrame dựa trên cột nếu cột đó tồn tại. Ví dụ: def sort_data(df, column_name, ascending=True)
    """
    if column_name in df.columns:
        return df.sort(col(column_name).asc() if ascending else col(column_name).desc())
    else:
        print(f"Bỏ qua Sắp xếp: Cột '{column_name}' không tồn tại.")
        return df   
    
# ================================================================================================================

def limit_rows(df, num_rows):
    """
    Giới hạn số lượng hàng trong DataFrame. Ví dụ: def limit_rows(df, num_rows)
    """
    try:
        return df.limit(num_rows)
    except Exception as e:
        print(f"Lỗi khi giới hạn số hàng '{num_rows}': {e}")
        return df
    
# ================================================================================================================

def select_columns_if_exist(df, column_list):
    """
    Chọn các cột từ danh sách nếu chúng tồn tại trong DataFrame. Ví dụ: def select_columns_if_exist(df, column_list = ['col1', 'col2'])
    """
    existing_columns = [col_name for col_name in column_list if col_name in df.columns]
    if not existing_columns:
        print("Không có cột nào từ danh sách tồn tại trong DataFrame.")
        return df
    return df.select(existing_columns)

# ================================================================================================================

def add_constant_column(df, column_name, constant_value):
    """
    Thêm cột mới với giá trị hằng số. Ví dụ: def add_constant_column(df, column_name, constant_value)
            """
    return df.withColumn(column_name, lit(constant_value))

# ================================================================================================================

# def smart_join(left_df, right_df, key_column, join_type="left"):
#     """
#     Hàm Join an toàn:
#     - left_df: Bảng chính
#     - right_df: Bảng phụ cần nối vào
#     - key_column: Tên cột chung để join (ví dụ: 'order_id')
#     - join_type: 'inner', 'left', 'full', 'anti' (mặc định là 'left')
#     """
#     # 1. Kiểm tra cột khóa có tồn tại ở cả 2 bảng không
#     if key_column not in left_df.columns:
#         print(f"LỖI: Bảng bên trái không có cột '{key_column}'")
#         return left_df
    
#     if key_column not in right_df.columns:
#         print(f"LỖI: Bảng bên phải không có cột '{key_column}'")
#         return left_df

#     # 2. Thực hiện Join
#     print(f"Đang thực hiện {join_type} join trên cột '{key_column}'...")
#     joined_df = left_df.join(right_df, on=key_column, how=join_type)
    
#     return joined_df


# ================================================================================================================

def smart_join(left_df, right_df, left_key, right_key=None, join_type="left"):
    """
    Hàm Join linh hoạt:
    - left_df: Bảng chính
    - right_df: Bảng phụ cần nối vào
    - left_key: Tên cột khóa ở bảng trái.
    - right_key: Tên cột khóa ở bảng phải (nếu None, mặc định lấy giống left_key).
    - join_type: 'inner', 'left', 'full', 'anti' (mặc định là 'left')
    """
    # Nếu không truyền right_key, coi như 2 bảng dùng chung tên cột
    if right_key is None:
        right_key = left_key

    # 1. Kiểm tra an toàn
    if left_key not in left_df.columns:
        print(f"LỖI: Bảng trái thiếu cột '{left_key}'")
        return left_df
    if right_key not in right_df.columns:
        print(f"LỖI: Bảng phải thiếu cột '{right_key}'")
        return left_df

    # 2. Thực hiện Join theo điều kiện tên cột
    print(f"Đang {join_type} join: [{left_key}] == [{right_key}]")
    
    if left_key == right_key:
        # Nếu trùng tên, dùng cú pháp rút gọn để tránh bị lặp cột sau khi join
        return left_df.join(right_df, on=left_key, how=join_type)
    else:
        # Nếu khác tên, dùng biểu thức so sánh
        return left_df.join(right_df, left_df[left_key] == right_df[right_key], how=join_type)
# ================================================================================================================  

def aggregate_if_column_exists(df, group_by_col, agg_col, agg_func):
    """
    Tổng hợp dữ liệu nếu cột tồn tại.
    group_by_col: Cột để nhóm (ví dụ: 'category')
    agg_col: Cột để tổng hợp (ví dụ: 'price')
    agg_func: Hàm tổng hợp ('sum', 'avg', 'max', 'min', 'count')
    """
    if group_by_col not in df.columns:
        print(f"Bỏ qua Tổng hợp: Cột nhóm '{group_by_col}' không tồn tại.")
        return df
    
    if agg_col not in df.columns:
        print(f"Bỏ qua Tổng hợp: Cột tổng hợp '{agg_col}' không tồn tại.")
        return df
    


    agg_functions = {
        'sum': sum,
        'avg': avg,
        'max': max,
        'min': min,
        'count': count
    }

    if agg_func not in agg_functions:
        print(f"LỖI: Hàm tổng hợp '{agg_func}' không được hỗ trợ.")
        return df

    aggregated_df = df.groupBy(group_by_col).agg(agg_functions[agg_func](col(agg_col)).alias(f"{agg_func}_{agg_col}"))
    
    return aggregated_df

# ================================================================================================================

def union_dataframes_if_compatible(df1, df2):
    """
    Nối hai DataFrame nếu chúng có cùng cấu trúc cột.
    """
    if set(df1.columns) != set(df2.columns):
        print("LỖI: Hai DataFrame không có cùng cấu trúc cột. Bỏ qua nối.")
        return df1
    
    return df1.unionByName(df2) 

# ================================================================================================================

def sample_dataframe(df, fraction, seed=None):
    """
    Lấy mẫu ngẫu nhiên từ DataFrame.
    fraction: Tỷ lệ mẫu (ví dụ: 0.1 cho 10%)
    seed: Giá trị seed để tái lập mẫu (mặc định là None)
    """
    try:
        return df.sample(withReplacement=False, fraction=fraction, seed=seed)
    except Exception as e:
        print(f"Lỗi khi lấy mẫu DataFrame: {e}")
        return df   
    
# ================================================================================================================

def standardize_ecommerce_df(df, string_cols=[], date_pair=None):
    """
    Chuẩn hóa nhanh: Viết hoa chữ, xóa khoảng trắng và tính ngày chênh lệch.
    - string_cols: Danh sách cột chuỗi cần chuẩn hóa.
    - date_pair: Tuple (cột_kết_thúc, cột_bắt_đầu)
    """
    # 1. Làm sạch chuỗi
    for c in string_cols:
        if c in df.columns:
            df = df.withColumn(c, upper(trim(col(c))))
            
    # 2. Tính thời gian xử lý nếu có cặp ngày
    if date_pair:
        end_c, start_c = date_pair
        if end_c in df.columns and start_c in df.columns:
            df = df.withColumn("process_days", datediff(col(end_c), col(start_c)))
            
    return df



# ================================================================================================================




# Window functions can be very useful for various operations in PySpark DataFrames.




# ================================================================================================================

def get_top_n_records(df, partition_col, sort_col, n=1, desc=True):
    """
    Giữ lại Top N dòng cho mỗi nhóm.
    Ví dụ: Lấy 1 đơn hàng mới nhất của mỗi khách.
    partition_col: cột để phân nhóm (ví dụ: "customer_id").
    sort_col: cột để sắp xếp (ví dụ: "order_purchase_timestamp").
    n: số dòng cần giữ lại cho mỗi nhóm.
    desc: True để lấy giá trị lớn nhất, False để lấy giá trị nhỏ nhất.
    Ví dụ sử dụng:
    latest_orders = get_top_n_records(orders_df, "customer_id", "order_purchase_timestamp", n=1)
    """
    # 1. Định nghĩa hướng sắp xếp
    sort_order = col(sort_col).desc() if desc else col(sort_col).asc()
    
    # 2. Tạo Window Spec
    window_spec = Window.partitionBy(partition_col).orderBy(sort_order)
    
    # 3. Đánh số và lọc lấy Top N
    return df.withColumn("row_num", row_number().over(window_spec)) \
             .filter(col("row_num") <= n) \
             .drop("row_num")

# Ví dụ: Lấy 1 đơn hàng mới nhất của mỗi khách
# latest_orders = get_top_n_records(orders_df, "customer_id", "order_purchase_timestamp", n=1)



def calculate_running_total(df, partition_col, sort_col, value_col):
    """
    Tính tổng cộng dồn của một cột theo thời gian.
    partition_col: cột để phân nhóm (ví dụ: "customer_id").
    sort_col: cột để sắp xếp theo thời gian (ví dụ: "order_purchase_timestamp").
    value_col: cột giá trị cần tính tổng cộng dồn (ví dụ: "payment_value").
    Ví dụ: Tổng tiền khách hàng đã mua tích lũy qua từng đơn.
    """
    # Window này chạy từ dòng đầu tiên của nhóm đến dòng hiện tại
    window_spec = Window.partitionBy(partition_col) \
                        .orderBy(sort_col) \
                        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    new_col_name = f"running_total_{value_col}"
    return df.withColumn(new_col_name, spark_sum(col(value_col)).over(window_spec))

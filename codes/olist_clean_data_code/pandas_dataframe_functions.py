import pandas as pd
import numpy as np

def standardize_ecommerce_df(df, string_cols=[], date_pair=None):
    """
    Chuẩn hóa nhanh: Viết hoa chữ, xóa khoảng trắng và tính ngày chênh lệch.
    - string_cols: Danh sách cột chuỗi cần chuẩn hóa.
    - date_pair: Tuple (cột_kết_thúc, cột_bắt_đầu)
    """
    # 1. Làm sạch chuỗi
    for col in string_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.upper().str.strip()

    # 2. Tính ngày chênh lệch   
    if date_pair:
        end_col, start_col = date_pair
        if end_col in df.columns and start_col in df.columns:
            df[f'diff_{end_col}_{start_col}'] = (pd.to_datetime(df[end_col]) - pd.to_datetime(df[start_col])).dt.days
    return df


#================================================================================================================

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
    sort_order = df[sort_col].sort_values(ascending=not desc)

    # 2. Sắp xếp và lấy Top N
    top_n_df = (df.sort_values(by=[partition_col, sort_col], ascending=[True, not desc])
                  .groupby(partition_col)
                  .head(n)
                  .reset_index(drop=True))
    return top_n_df 
#================================================================================================================

def calculate_running_total(df, partition_col, sort_col, value_col):
    """
    Tính tổng cộng dồn của một cột theo thời gian.
    partition_col: cột để phân nhóm (ví dụ: "customer_id").
    sort_col: cột để sắp xếp theo thời gian (ví dụ: "order_purchase_timestamp").
    value_col: cột giá trị cần tính tổng cộng dồn (ví dụ: "payment_value").
    Ví dụ: Tổng tiền khách hàng đã mua tích lũy qua từng đơn.
    """
    # Sắp xếp dữ liệu
    df = df.sort_values(by=[partition_col, sort_col])

    # Tính tổng cộng dồn
    df[f'running_total_{value_col}'] = (df.groupby(partition_col)[value_col]
                                          .cumsum())
    return df   


#================================================================================================================

def smart_join_pandas(left_df, right_df, left_key, right_key=None, join_type="left"):
    """
    Thực hiện join thông minh giữa hai DataFrame pandas.
    - left_df: DataFrame bên trái.
    - right_df: DataFrame bên phải.
    - left_key: Cột khóa bên trái.
    - right_key: Cột khóa bên phải (nếu khác với bên trái).
    - join_type: Loại join (mặc định là "left").
    """
    if right_key is None:
        right_key = left_key

    # Kiểm tra nếu cột khóa bên phải không tồn tại, trả về DataFrame bên trái
    if right_key not in right_df.columns:
        print(f"Cột khóa '{right_key}' không tồn tại trong DataFrame bên phải. Trả về DataFrame bên trái.")
        return left_df

    # Thực hiện join
    merged_df = pd.merge(left_df, right_df, how=join_type, left_on=left_key, right_on=right_key)
    return merged_df

#================================================================================================================



def cast_column_if_exists_pd(df, column_name, target_type):
    """
    Docstring for cast_column_if_exists_pd

    :param df: DataFrame pandas
    :param column_name: Tên cột cần cast
    :param target_type: Kiểu dữ liệu mục tiêu (ví dụ: 'int64', 'float64', 'datetime64[ns]')
    """
    if column_name in df.columns:
        return df.astype({column_name: target_type})
    return df
    

def delete_column_if_exists_pd(df, column_name):
    if column_name in df.columns:
        return df.drop(columns=[column_name])
    return df

def rename_column_if_exists_pd(df, old_col_name, new_col_name):
    if old_col_name in df.columns:
        return df.rename(columns={old_col_name: new_col_name})
    return df                    




def filter_data_pd(df, condition_query):
    """
    Pandas dùng query() rất mạnh. Ví dụ: "price > 100 and status == 'delivered'"
    """
    try:
        return df.query(condition_query)
    except Exception as e:
        print(f"Lỗi khi lọc: {e}")
        return df

def add_conditional_col_pd(df, new_col, base_col, condition_mask, value_if_true, value_if_false):
    """
    Pandas dùng np.where tương đương với when/otherwise của Spark.
    condition_mask: df['price'] > 100
    """
    if base_col in df.columns:
        df[new_col] = np.where(condition_mask, value_if_true, value_if_false)
    return df

def fill_nulls_in_column_pd(df, column_name, fill_value):
    if column_name in df.columns:
        df[column_name] = df[column_name].fillna(fill_value)
    return df



#================================================================================================================



def smart_join_pd(left_df, right_df, left_key, right_key=None, join_type="left"):
    if right_key is None: right_key = left_key
    
    if left_key not in left_df.columns or right_key not in right_df.columns:
        print("Lỗi thiếu cột khóa!")
        return left_df
        
    # Pandas merge() tương đương Spark join()
    return left_df.merge(right_df, left_on=left_key, right_on=right_key, how=join_type)

def aggregate_pd(df, group_by_col, agg_col, agg_func):
    """
    agg_func: 'sum', 'mean' (thay cho avg), 'max', 'min', 'count'
    """
    if agg_func == 'avg': agg_func = 'mean'
    if group_by_col in df.columns and agg_col in df.columns:
        return df.groupby(group_by_col)[agg_col].agg(agg_func).reset_index()
    return df

#================================================================================================================


def get_top_n_records_pd(df, partition_col, sort_col, n=1, desc=True):
    """
    Tương đương row_number() over(partitionBy... orderBy...)
    """
    return df.sort_values(by=[partition_col, sort_col], ascending=[True, not desc]) \
             .groupby(partition_col) \
             .head(n)

def calculate_running_total_pd(df, partition_col, sort_col, value_col):
    """
    Tương đương sum() over(partitionBy... orderBy... rowsBetween UNBOUNDED PRECEDING and CURRENT ROW)
    """
    df = df.sort_values(by=[partition_col, sort_col])
    df[f"running_total_{value_col}"] = df.groupby(partition_col)[value_col].cumsum()
    return df


#================================================================================================================
# print dataframe columns and types 

def print_df_schema_pd(df):
    print(df.dtypes)

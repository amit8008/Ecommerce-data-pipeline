import glob
import os
from datetime import datetime, timedelta

import pandas as pd

from scr.utility import configuration
from scr.utility.logger import logger


def top_10_customer(customer_data_path, order_data_path) :
    # customer data
    customer_df = pd.read_json(customer_data_path)
    modified_cust_df = customer_df[['customer_id', 'customer_name']]

    # read order data from orders_stream dir
    # order_df = pd.read_csv(order_data_path)
    # order_df['order_time'] = pd.to_datetime(order_df['order_time'])
    orders_directory_path = order_data_path
    all_csv_files = glob.glob(os.path.join(orders_directory_path, "order*.csv"))
    print(all_csv_files)
    list_of_dfs = []
    for file_path in all_csv_files :
        df = pd.read_csv(file_path)
        list_of_dfs.append(df)

    combined_df = pd.concat(list_of_dfs, ignore_index = True)
    combined_df['order_time'] = pd.to_datetime(combined_df['order_time'])

    # filter on date of last 7 days (1 week)
    today = datetime.now()
    date_7_before = today - timedelta(weeks = 1)
    logger.info((today, date_7_before))
    filtered_order_df = combined_df[(combined_df['order_time'] > date_7_before) & (combined_df['order_time'] < today)]

    # merged data
    merged_df = pd.merge(modified_cust_df, filtered_order_df, on = "customer_id", how = "right")
    count_per_cust = (merged_df.groupby(['customer_id', 'customer_name'])['customer_id']
                      .size()
                      .reset_index(name = "cust_count")
                      .sort_values('cust_count', ascending = False))

    # print(count_per_cust[:10])

    for index, row in count_per_cust[:10].iterrows() :
        print(
            f"Index: {index}   customer_id: {row['customer_id']}  customer_name: {row['customer_name']}  count: {row['cust_count']}")


top_10_customer(configuration.data_dir + "customer_175.json", configuration.data_dir + "orders_stream")

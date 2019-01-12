import random

import pandas as pd
from fastparquet import write
from fastparquet import ParquetFile

# simple data schema
print('---------------------simple.snappy.parquet----------------------')
simple_parquet_file_name = 'simple.snappy.parquet'
sample_data = {'col1': [1, 2, None], 'col2': [3, 4, 5]}
simple_df = pd.DataFrame(data=sample_data)
write(simple_parquet_file_name, simple_df, compression={
    '_default': {
        "type": "SNAPPY",
        "args": None
    }
})

pf = ParquetFile(simple_parquet_file_name)
simple_out_df = pf.to_pandas()
print(simple_out_df.head())

append_data = {'col1': [11, 12, None], 'col2': [13, 14, 15]}
append_df = pd.DataFrame(data=append_data)
write(simple_parquet_file_name, append_df, compression={
    '_default': {
        "type": "SNAPPY",
        "args": None
    }
}, append=True)

pf = ParquetFile(simple_parquet_file_name)
total_df = pf.to_pandas()
print(total_df.head(10))

print()
print()
print()

# complex data schema
print('---------------------user.snappy.parquet----------------------')
user_parquet_file_name = 'user.snappy.parquet'
colum_name_array = ['id', 'name', 'country', 'age']
user_df = pd.DataFrame(columns=colum_name_array)
for i in range(0, 10000):
    if i % 3 == 0:
        country = 'cn'
    else:
        country = 'us'
    if i % 4 == 0:
        name = 'lisi'
    elif i % 3 == 0:
        name = 'zhangsan'
    else:
        name = 'wangwu'
    age = random.randint(1, 99)
    row = pd.DataFrame([[i, name, country, age]], columns=colum_name_array)
    user_df = user_df.append(row, ignore_index=True)

write(user_parquet_file_name, user_df, compression={
    '_default': {
        "type": "SNAPPY",
        "args": None
    }
})
pf = ParquetFile(user_parquet_file_name)
user_out_df = pf.to_pandas()
print(user_out_df.head())

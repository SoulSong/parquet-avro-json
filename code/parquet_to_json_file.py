import json
import math

from fastparquet import ParquetFile

user_parquet_file_name = 'user.snappy.parquet'
pf = ParquetFile(user_parquet_file_name)
df = pf.to_pandas()
# get column names
columns = df.columns.values.tolist()
print('all columns:',end='')
for column in columns:
    print(column, end='')
    print('\t', end='')

print()


with open('user.json', 'w', encoding='UTF-8') as f:
    for index, row in df.iterrows():
        user = {}
        for column in columns:
            if type(row[column]) is int:
                if math.isnan(row[column]):
                    continue
                else:
                    user[column] = row[column]
            else:
                if row[column]:
                    user[column] = row[column]
        user_str = json.dumps(user, ensure_ascii=False)
        f.write(user_str)
        f.write('\n')

with open('user.json', 'r', encoding='UTF-8') as f:
    lines = f.readlines()
    for line in lines:
        print(line, end='')

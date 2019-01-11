import math

from fastparquet import ParquetFile

import avro.schema
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader

schema = avro.schema.Parse(open("user.avsc", "rb").read())

user_parquet_file_name = 'user.snappy.parquet'
pf = ParquetFile(user_parquet_file_name)
df = pf.to_pandas()

# get column names
columns = df.columns.values.tolist()
print('all columns:', end='')
for column in columns:
    print(column, end='')
    print('\t', end='')
print()

avro_file_name = 'user.avro'
with DataFileWriter(open(avro_file_name, "wb"), DatumWriter(),
                    schema) as writer:
    for index, row in df.iterrows():
        item = {}
        for column in columns:
            if type(row[column]) is int:
                if math.isnan(row[column]):
                    continue
                else:
                    item[column] = row[column]
            else:
                if row[column]:
                    item[column] = row[column]

        writer.append(item)

reader = DataFileReader(open(avro_file_name, "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()
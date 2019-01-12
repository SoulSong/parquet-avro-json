import random

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schema = avro.schema.Parse(open("user.avsc", "rb").read())

writer = DataFileWriter(open("users.avro", "wb"), DatumWriter(), schema)
for i in range(0, 10000):
    if i % 3 == 0:
        country = 'cn'
        hobby = None
    else:
        country = 'us'
        hobby = 'drink'
    if i % 4 == 0:
        name = 'lisi'
    elif i % 3 == 0:
        name = 'zhangsan'
    else:
        name = 'wangwu'
    age = random.randint(1, 99)

    writer.append({"id": i, "name": name, "age": age, "country": country, "hobby": hobby})

writer.close()

reader = DataFileReader(open("users.avro", "rb"), DatumReader())
for user in reader:
    print(user)
reader.close()

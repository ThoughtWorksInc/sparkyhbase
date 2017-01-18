import random
from datetime import datetime, timedelta
import pyspark.sql.functions as F
import uuid
import sys

output_path = sys.argv[1]

def rand_date():
    delta = random.randint(0, 10000)
    dt = datetime.now() - timedelta(days=delta)
    return dt.date().isoformat()

def rand_string():
    return str(uuid.uuid4())

df = sqlContext.range(1000000, 2000000)
df = df.select(
    'id',
    F.udf(rand_date)().alias('as_of'),
    F.rand().alias('a'),
    F.rand().alias('b'),
    F.rand().alias('c')
).orderBy(F.asc('as_of'))
df.write.json(output_path)

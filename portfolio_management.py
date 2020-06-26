##### from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import pyspark
import os
from IPython.display import display, HTML

hdfs_namenode = 'hdfs://localhost:9000'
asof_date = '05-29-2020'
knowledge_time = '05-31-2020_17_18_UTC'

def get_data(context, file):
    file = '{}/user/root/{}/{}/{}'.format(hdfs_namenode, asof_date, knowledge_time, file)
    return context.read.parquet(file)
    
    
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

positions = get_data(sqlContext, 'positions_eod.parquet')
print('Positions Row Size ' + '{:,}'.format(positions.count()))
positions.show()

instruments = get_data(sqlContext, 'instruments_eod.parquet')
print('Instruments Row Size ' + '{:,}'.format(instruments.count()))
instruments.select("ProductId", "AssetClass", "Sector", "Industry", "Symbol", "Issuer").show()

books = get_data(sqlContext, 'books_eod.parquet')
print('Books Row Size ' + '{:,}'.format(books.count()))
books.show()

counterparties = get_data(sqlContext, 'counterparties_eod.parquet')
print('counterparties Row Size ' + '{:,}'.format(counterparties.count()))
counterparties.show()

sc.stop()

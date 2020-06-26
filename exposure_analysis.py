from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
import pyspark
import os
from IPython.display import display, HTML
import matplotlib.pyplot as plt

hdfs_namenode = 'hdfs://PSNYD-KAFKA-01:9000'
asof_date = '05-29-2020'
knowledge_time = '05-31-2020_17_18_UTC'

def get_data(context, file):
    file = '{}/user/root/{}/{}/{}'.format(hdfs_namenode, asof_date, knowledge_time, file)
    return context.read.parquet(file)
    
    
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

positions = get_data(sqlContext, 'positions_eod.parquet')
print('Positions Row Size ' + '{:,}'.format(positions.count()))

instruments = get_data(sqlContext, 'instruments_eod.parquet')
print('Instruments Row Size ' + '{:,}'.format(instruments.count()))

nd = positions.join(instruments, positions.ProductId == instruments.ProductId).\
               select('PositionId', 'Symbol', 'Issuer', 'ProductType', 'AssetClass', 'Currency', 'Exposure')

gbp_positions = nd.where(nd.Currency == 'GBP')
gbp_positions.show()


asset_exposures = nd.groupBy("AssetClass").sum("Exposure").toPandas()
display(asset_exposures)
asset_exposures.plot(kind='bar',x='AssetClass',y='sum(Exposure)')
plt.show()

currency_exposures = nd.groupBy("Currency").sum("Exposure").toPandas()
currency_exposures = currency_exposures.sort_values('sum(Exposure)',ascending = False).head(5)
display(currency_exposures)
currency_exposures.plot(kind='scatter',x='Currency',y='sum(Exposure)',color='red')
plt.show()


nd.limit(1000).toPandas()[['Exposure']].plot(kind='hist',rwidth=0.8)
plt.show()


sc.stop()

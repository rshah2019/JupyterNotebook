from pyspark.context import SparkContext
from pyspark.sql import SQLContext
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

credit_risk = get_data(sqlContext, 'credit_risk_eod.parquet')
print('Credit Risk Row Size ' + '{:,}'.format(credit_risk.count()))
credit_risk.select("PositionId", "Credit1Pct", "Credit2Pct", "Credit3Pct", "Credit4Pct", "Credit5Pct").show()

vol_risk = get_data(sqlContext, 'volatility_risk_eod.parquet')
print('Volatility Risk Row Size ' + '{:,}'.format(vol_risk.count()))
vol_risk.select("PositionId", "Volatility1Pct", "Volatility2Pct", "Volatility3Pct", "Volatility4Pct").show()

rate_risk = get_data(sqlContext, 'rate_risk_eod.parquet')
print('Interest Rate risk Row Size ' + '{:,}'.format(rate_risk.count()))
rate_risk.select("PositionId", "InterestRate1Pct", "InterestRate2Pct", "InterestRate3Pct").show()

sc.stop()

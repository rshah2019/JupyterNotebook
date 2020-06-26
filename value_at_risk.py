from pyspark.context import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
import pyspark
import os
from IPython.display import display, HTML
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

hdfs_namenode = 'hdfs://localhost:9000'
asof_date = '05-29-2020'
knowledge_time = '05-31-2020_17_18_UTC'

def get_data(context, file):
    file = '{}/user/root/{}/{}/{}'.format(hdfs_namenode, asof_date, knowledge_time, file)
    return context.read.parquet(file)


def var(r, level=5):
    """
    VaR
    """
    if isinstance(r, pd.DataFrame):
        return r.aggregate(var, level=level)
    elif isinstance(r, pd.Series):
        return -np.nanpercentile(r, level)
    else:
        raise TypeError("Expected r to be Series or DataFrame")


def plot_d(var_array):
    # Build plot
    plt.xlabel("Tenor (Dates)")
    plt.ylabel("Max portfolio loss 95th Percentile")
    plt.title("Max portfolio loss (VaR) over time")
    #fig, ax = plt.subplots()
    #ax.ticklabel_format(useOffset=False)
    plt.plot(var_array, "r")

    
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)

portfolio_simulation = get_data(sqlContext, 'portfolio_simulation_eod.parquet')
print('Portfolio Simulations Gains/Losses Row Size ' + '{:,}'.format(portfolio_simulation.count()))
portfolio_simulation.show()
pandas = portfolio_simulation.toPandas()

print("Producing 90th precentile")
var = var(pandas, 10)

var = var.drop(['Path'])

display(var)
plot_d(var)

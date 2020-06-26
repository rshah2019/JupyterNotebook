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

def get_expected_exposures(counterparty_name):
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)

    position_simulation = get_data(sqlContext, 'position_simulation_eod.parquet')
    print('Position Simulations Row Size ' + '{:,}'.format(position_simulation.count()))

    positions = get_data(sqlContext, 'positions_eod.parquet')
    print('Positions Row Size ' + '{:,}'.format(positions.count()))

    cps = get_data(sqlContext, 'counterparties_eod.parquet')
    print('Counterparty Row Size ' + '{:,}'.format(cps.count()))

    all_paths = position_simulation.where(position_simulation.PositionId == 474).toPandas()
    display(all_paths)


    counterparty_exposures = positions.join(cps, positions.CounterpartyId == cps.PartyId)\
                  .join(position_simulation, positions.PositionId == position_simulation.PositionId)\
                  .where(cps.CounterpartyFullName == counterparty_name)\
                  .toPandas()

    counterparty_exposures = counterparty_exposures.loc[:,~counterparty_exposures.columns.duplicated()]
    display(counterparty_exposures)
    counterparty_ee = counterparty_exposures.groupby(['PositionId']).mean().reset_index()
    del counterparty_ee['Path']
    counterparty_ee = counterparty_ee[["PositionId", "1M", "3M", "6M", "1Y"]]
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    for c in [c for c in counterparty_ee.columns if counterparty_ee[c].dtype in numerics]:
        counterparty_ee[c] = counterparty_ee[c].abs()
    display(counterparty_ee)
    counterparty_ee_sum = pd.DataFrame(counterparty_ee.sum(), columns=[counterparty_name + ' EE'])
    display(counterparty_ee_sum)
    return counterparty_ee_sum

bank_america_ee = get_expected_exposures("Bank Of America")
citibank_ee = get_expected_exposures("Citibank")

    
ax = bank_america_ee.plot()
#ax.xlabel("Tenor (Dates)")
#ax.ylabel("Expected Exposure of Counterparty")
#ax.title("Comparing Expected Exposures of Multiple Counterparties")
citibank_ee.plot(ax=ax)

print("done")

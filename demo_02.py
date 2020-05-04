import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Pysparkexample").config("spark.some.config.option", "some-value").getOrCreate()

df = spark.read.csv('data.csv', header='true', inferSchema='true')

df = df.withColumn("Amount", df["Amount"].cast("double"))

cols = df.columns

for i in cols: print(i)


df.count()

df.show()

df.show(5)

df.head()

df.describe().show()

df.createOrReplaceTempView('VermontVendor')

spark.sql(
'''
SELECT `Quarter Ending`, Department, Amount, State FROM VermontVendor
LIMIT 10
'''
).show()

df.select('Quarter Ending','Amount').show()

df.select('Quarter Ending','Amount').filter(df['Amount']>1000).show()

plot_df = spark.sql(
'''SELECT Department, SUM(Amount) as Total FROM VermontVendor 
GROUP BY Department
ORDER BY Total DESC
LIMIT 10'''
).toPandas()

type(plot_df)

ax = plt.subplots(1,1,figsize=(10,6))

plot_df.plot(x = 'Department', y = 'Total', kind = 'barh', color = 'C0', ax = ax, legend = False)

ax.set_xlabel('Department', size = 16)

ax.set_ylabel('Total', size = 16)

plt.savefig('barplot.png')

plt.show()

import numpy as np
import seaborn as sns
plot_df2 = spark.sql(
'''
SELECT Department, SUM(Amount) as Total FROM VermontVendor 
GROUP BY Department
'''
).toPandas()
plt.figure(figsize = (10,6))
sns.distplot(np.log(plot_df2['Total']))
plt.title('Histogram of Log Totals for all Departments in Dataset', size = 16)
plt.ylabel('Density', size = 16)
plt.xlabel('Log Total', size = 16)
plt.savefig('distplot.png')
plt.show()


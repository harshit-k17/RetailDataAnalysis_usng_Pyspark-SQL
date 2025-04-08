# Databricks notebook source
dbutils.widgets.dropdown("time_period",'Weekly',['Weekly', 'Monthly'])

# COMMAND ----------



from datetime import date,timedelta,datetime
from pyspark.sql.functions import * 

time_period= dbutils.widgets.get("time_period")
print(time_period)
today = date.today()

if time_period=='Weekly':
    start_date=today-timedelta(days=today.weekday(),weeks=1)-timedelta(days=1)
    end_date=start_date+timedelta(days=6)

else:
    first=today.replace(day=1)
    end_date=first-timedelta(days=1)
    start_date=first-timedelta(days=end_date.day)

print(start_date,end_date)

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/superstore.csv", header=True, inferSchema=True)
df.show()

# COMMAND ----------

df.createOrReplaceTempView("sample")

# COMMAND ----------

# DBTITLE 1,total no. of customers
# MAGIC %sql select count(distinct customer_id) from sample;

# COMMAND ----------

# MAGIC %sql select count(distinct customer_id) from sample where order_date between '2024-06-01' and '2024-06-30' 

# COMMAND ----------

# DBTITLE 1,total no. of orders
# MAGIC %sql select count(distinct order_id) from sample

# COMMAND ----------

# DBTITLE 1,total sales & profit
# MAGIC %sql select sum(sales), sum(profit) from sample

# COMMAND ----------

# DBTITLE 1,top sales by country
# MAGIC %sql select sum(sales), country from sample
# MAGIC group by 2

# COMMAND ----------

# DBTITLE 1,most profitable region, country
# MAGIC %sql select sum(sales), country, region  from sample
# MAGIC group by 2,3
# MAGIC order by 1 desc

# COMMAND ----------

# DBTITLE 1,top sales category product
# MAGIC %sql select sum(sales), category  from sample
# MAGIC group by 2
# MAGIC order by 1 desc

# COMMAND ----------

# DBTITLE 1,top 10 sub-category
# MAGIC %sql select sum(sales), sub_category  from sample
# MAGIC group by 2
# MAGIC order by 1 desc limit 10

# COMMAND ----------

# DBTITLE 1,most ordered quantity product
# MAGIC %sql select sum(sales), product_name  from sample
# MAGIC group by 2
# MAGIC order by 1 desc

# COMMAND ----------

# DBTITLE 1,top customer based on city
# MAGIC %sql select sum(sales), customer_name, city  from sample
# MAGIC group by 2,3
# MAGIC order by 1 desc

# COMMAND ----------


from pyspark.sql.functions import count,when,current_timestamp,lit
tables=[
    ("customers",f"{catalog}.dq.customers_validation"),
    ("orders",f"{catalog}.dq.orders_validation"),
    ("order_items",f"{catalog}.dq.order_items_validation"),
    ("payments",f"{catalog}.dq.payments_validation")
]
summary=None
for table_name,table_path in tables:
    df=spark.table(table_path)
    result=df.agg(
        count("*").alias("total_records"),
        count(when(col("dq_status")=="PASS",True)).alias("passed_records"),
        count(when(col("dq_status")=="FAIL",True)).alias("failed_records")
    ).withColumn("table_name",lit(table_name))
    summary=result if summary is None else summary.unionByName(result)
summary=(summary
    .withColumn("dq_pass_pct",col("passed_records")/col("total_records")*100)
    .withColumn("dq_fail_pct",col("failed_records")/col("total_records")*100)
    .withColumn("validation_timestamp",current_timestamp()))
summary.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{catalog}.dq.validation_summary")
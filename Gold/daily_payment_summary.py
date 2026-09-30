from pyspark.sql.functions import col,count,sum,when,round,to_date
payments=spark.table(f"{catalog}.silver.payments")
daily_payment_summary=(payments
    .groupBy(to_date("payment_date").alias("date"))
    .agg(
        count("payment_id").alias("total_transactions"),
        sum(when(col("payment_status")=="success",1).otherwise(0)).alias("successful_transactions"),
        sum(when(col("payment_status")=="failed",1).otherwise(0)).alias("failed_transactions"),
        sum(when(col("payment_status")=="pending",1).otherwise(0)).alias("pending_transactions"),
        sum(when(col("payment_status")=="refunded",1).otherwise(0)).alias("refunded_transactions"),
        sum(when(col("payment_status")=="success",col("amount")).otherwise(0)).alias("successful_payment_amount"),
        sum(when(col("payment_status")=="failed",col("amount")).otherwise(0)).alias("failed_payment_amount"),
        sum(when(col("payment_status")=="refunded",col("amount")).otherwise(0)).alias("refunded_amount"),
        sum("transaction_fee").alias("transaction_fees")
    )
    .withColumn("payment_success_rate",round(col("successful_transactions")/col("total_transactions")*100,2))
    .withColumn("payment_failure_rate",round(col("failed_transactions")/col("total_transactions")*100,2)))
daily_payment_summary.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{catalog}.gold.daily_payment_summary")
from pyspark.sql.functions import col,count,sum,when,round
payments=spark.table(f"{catalog}.silver.payments")
gateway_performance=(payments
    .groupBy("payment_gateway")
    .agg(
        count("payment_id").alias("total_transactions"),
        sum(when(col("payment_status")=="success",1).otherwise(0)).alias("successful_transactions"),
        sum(when(col("payment_status")=="failed",1).otherwise(0)).alias("failed_transactions"),
        sum(when(col("payment_status")=="refunded",1).otherwise(0)).alias("refunded_transactions"),
        sum(when(col("payment_status")=="success",col("amount")).otherwise(0)).alias("successful_payment_amount"),
        sum(when(col("payment_status")=="failed",col("amount")).otherwise(0)).alias("failed_payment_amount"),
        sum(when(col("payment_status")=="refunded",col("amount")).otherwise(0)).alias("refunded_amount"),
        sum("transaction_fee").alias("transaction_fees")
    )
    .withColumn("payment_success_rate",round(col("successful_transactions")/col("total_transactions")*100,2))
    .withColumn("payment_failure_rate",round(col("failed_transactions")/col("total_transactions")*100,2))
    .withColumn("fee_rate",round(col("transaction_fees")/col("successful_payment_amount")*100,2)))
gateway_performance.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{catalog}.gold.gateway_performance")
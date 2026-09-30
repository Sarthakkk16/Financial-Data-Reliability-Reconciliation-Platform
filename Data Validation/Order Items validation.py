from pyspark.sql.functions import abs
items=spark.table(f"{catalog}.silver.order_items")
items_validation=(items
    .withColumn("calculated_item_total",col("quantity")*col("unit_price")-col("item_discount"))
    .withColumn("item_total_mismatch",abs(col("item_total")-col("calculated_item_total"))>0.01)
    .withColumn("dq_status",
        when(col("order_item_id").isNull()| (col("order_item_id")=="") ,"FAIL")
        .when(col("order_id").isNull()| (col("order_id")=="") ,"FAIL")
        .when(col("quantity")<=0,"FAIL")
        .when(col("unit_price")<0,"FAIL")
        .when(col("item_discount")<0,"FAIL")
        .otherwise("PASS"))
    .withColumn("dq_reason",
        when(col("order_item_id").isNull()| (col("order_item_id")=="") ,"missing_order_item_id")
        .when(col("order_id").isNull()| (col("order_id")=="") ,"missing_order_id")
        .when(col("quantity")<=0,"invalid_quantity")
        .when(col("unit_price")<0,"negative_unit_price")
        .when(col("item_discount")<0,"negative_discount")
        .otherwise("valid")))
items_validation.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{catalog}.dq.order_items_validation")
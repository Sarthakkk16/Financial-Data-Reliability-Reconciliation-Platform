from pyspark.sql.functions import col,when,lit,concat_ws
catalog="`reconciliation-platform`"
customers=spark.table(f"{catalog}.silver.customers")
customers_validation=(customers
    .withColumn("dq_status",
        when(col("customer_id").isNull()| (col("customer_id")=="") ,"FAIL")
        .when(col("email").isNull()|~col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"),"FAIL")
        .when(col("signup_date").isNull(),"FAIL")
        .otherwise("PASS"))
    .withColumn("dq_reason",
        when(col("customer_id").isNull()| (col("customer_id")==""),"missing_customer_id")
        .when(col("email").isNull(),"missing_email")
        .when(~col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"),"invalid_email")
        .when(col("signup_date").isNull(),"missing_signup_date")
        .otherwise("valid")))
customers_validation.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(f"{catalog}.dq.customers_validation")
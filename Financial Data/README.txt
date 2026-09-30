E-COMMERCE FINANCIAL DATA ENGINEERING DATASET
================================================
Tables:
customers, orders, order_items, payments

batch_01 = historical/initial load
batch_02 = incremental load containing NEW and UPDATED records

Use updated_at for Auto Loader incremental processing and MERGE/CDC logic.

Relationships:
customers.customer_id -> orders.customer_id
orders.order_id -> order_items.order_id
orders.order_id -> payments.order_id

Intentional data-quality problems:
- nulls
- duplicate rows
- inconsistent casing
- leading/trailing spaces
- inconsistent currency values
- inconsistent status/gateway names
- updates to existing records in batch_02
- new records in batch_02
- potentially inconsistent financial values

Recommended architecture:
Cloud Storage -> Auto Loader -> Bronze Delta -> Silver DQ/Cleaning -> MERGE/CDC -> Gold Financial Marts -> Power BI

Suggested Gold KPIs:
GMV, net revenue, payment success rate, refund rate, discount rate, cancellation rate, transaction fees, gateway performance, customer revenue, city/state revenue.

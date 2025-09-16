# Databricks notebook source
# แทนค่าด้วยชื่อ Storage Account  (เช่น demoazureproject)
storage_account_name = "<storage_account_name>"
# แทนค่าด้วย Access Key 
storage_account_key = "<storage_account_key>"

# กำหนดค่า Spark เพื่อให้เชื่อมต่อกับ ADLS Gen2
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", storage_account_key)

print("ADLS Gen2 connection configured.")

# COMMAND ----------

from pyspark.sql.functions import col

# กำหนด Path สำหรับข้อมูลดิบใน ADLS Gen2
storage_account_name = "demoazureproject"
raw_path = f"abfss://raw-data@{storage_account_name}.dfs.core.windows.net/"

# อ่านข้อมูล (Extract)
# ดึงข้อมูลยอดขาย (ไฟล์ CSV)
sales_df = spark.read.csv(f"{raw_path}sales.csv", header=True, inferSchema=True)

# ดึงข้อมูลจากโฟลเดอร์ products_temp (ไฟล์ Parquet จาก ADF)
products_df = spark.read.parquet(f"{raw_path}products_temp/")

# ดึงข้อมูลจากโฟลเดอร์ customers_temp (ไฟล์ Parquet จาก ADF)
customers_df = spark.read.parquet(f"{raw_path}customers_temp/")

# เปลี่ยนชื่อคอลัมน์ให้ตรงกัน
# เปลี่ยนชื่อคอลัมน์ใน sales_df ให้ตรงกับ products_df และ customers_df
sales_df = sales_df.withColumnRenamed("customer_id", "CustomerID")
sales_df = sales_df.withColumnRenamed("product_id", "ProductID")

# เปลี่ยนชื่อคอลัมน์ใน products_df ให้ตรงกับ sales_df และ customers_df
products_df = products_df.withColumnRenamed("product_id", "ProductID")

print("Data extracted and renamed successfully.")

# ทำการ Join dataframe ทั้ง 3 เข้าด้วยกัน
joined_df = sales_df.join(customers_df, "CustomerID", "inner") \
                    .join(products_df, "ProductID", "inner")

# แสดงผลลัพธ์เพื่อตรวจสอบ
joined_df.display()

# COMMAND ----------

# กำหนด Path สำหรับข้อมูลที่ประมวลผลแล้วใน ADLS Gen2
processed_path = f"abfss://processed-data@{storage_account_name}.dfs.core.windows.net/final_sales_data/"

# บันทึกข้อมูลในรูปแบบ Parquet
joined_df.write.format("parquet").mode("overwrite").save(processed_path)

# COMMAND ----------

# print("Sales DataFrame Schema:")
# sales_df.printSchema()

# print("\nProducts DataFrame Schema:")
# products_df.printSchema()
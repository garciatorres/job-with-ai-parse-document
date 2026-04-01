// Databricks notebook source
// DBTITLE 1,Create pdf_path widget
// MAGIC %python
// MAGIC
// MAGIC # Uncomment the lines below to set up the notebook parameter for the PDF path.
// MAGIC dbutils.widgets.text(
// MAGIC     "pdf_path",
// MAGIC     "/Volumes/catalog/schema/documents/document.pdf",
// MAGIC     "PDF Path"
// MAGIC )

// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC # Demo
// MAGIC
// MAGIC ## Parse PDF using `ai_parse_document`
// MAGIC
// MAGIC **How to run:**
// MAGIC
// MAGIC 1. Clone this notebook
// MAGIC 2. Create a cluster >= 16.4
// MAGIC     - For Serverless GC, make sure it is 16.4+ and "Environment version: 2"
// MAGIC 3. Run it!
// MAGIC
// MAGIC **More info:**
// MAGIC - [Preview Doc](https://docs.google.com/document/d/1YkYyVbpKV1Q2dulzU9JqLYkrBcNeVF5x0gNw5GjL2FM/edit?tab=t.0)
// MAGIC - [Wiki](http://go/docparse)
// MAGIC - Questions? Join [#unstructured-to-structured](https://databricks.enterprise.slack.com/archives/C07HZAJURBM) slack channel

// COMMAND ----------

// DBTITLE 1,install lib for display PDF (no need for production)
// MAGIC %sh
// MAGIC apt-get update && apt-get install -y poppler-utils
// MAGIC pip install pdf2image

// COMMAND ----------

// DBTITLE 1,display example PDF (no need for production)
// MAGIC %python
// MAGIC import subprocess
// MAGIC subprocess.check_call(["pip", "install", "-q", "pymupdf"])
// MAGIC
// MAGIC import fitz  # PyMuPDF
// MAGIC from PIL import Image
// MAGIC import matplotlib.pyplot as plt
// MAGIC import io
// MAGIC
// MAGIC pdf_path = dbutils.widgets.get("pdf_path")
// MAGIC if not pdf_path:
// MAGIC     raise ValueError("Please set the 'pdf_path' parameter to a valid PDF file path before running this cell.")
// MAGIC doc = fitz.open(pdf_path)
// MAGIC page = doc[0]
// MAGIC pix = page.get_pixmap(dpi=300)
// MAGIC img = Image.open(io.BytesIO(pix.tobytes("png")))
// MAGIC plt.figure(figsize=(10, 10))
// MAGIC plt.imshow(img)
// MAGIC plt.axis("off")
// MAGIC plt.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Simple Exmple: `variant` output
// MAGIC
// MAGIC The `ai_parse_document` function output is in a `variant` type column.

// COMMAND ----------

// DBTITLE 1,SQL
// MAGIC %sql
// MAGIC SELECT
// MAGIC   path,
// MAGIC   ai_parse_document(content)::STRING AS parsed
// MAGIC FROM
// MAGIC   READ_FILES(:pdf_path, format => 'binaryFile');

// COMMAND ----------

// DBTITLE 1,python
// MAGIC %skip
// MAGIC %python
// MAGIC        
// MAGIC from pyspark.sql.functions import *
// MAGIC
// MAGIC df = spark.read.format("binaryFile") \
// MAGIC   .load(pdf_path) \
// MAGIC   .withColumn(
// MAGIC     "parsed",
// MAGIC     expr("ai_parse_document(content)"))
// MAGIC display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ## 2. Advanced Exmple: access the `variant` output fields
// MAGIC
// MAGIC The `ai_parse_document` function output can be accessed (`document`, `error_status`, `corrupted_date`, `metadata`).

// COMMAND ----------

// DBTITLE 1,sql
// MAGIC %sql
// MAGIC WITH corpus AS (
// MAGIC   SELECT
// MAGIC     path,
// MAGIC     ai_parse_document(content) AS parsed
// MAGIC   FROM
// MAGIC     READ_FILES(:pdf_path, format => 'binaryFile')
// MAGIC )
// MAGIC SELECT
// MAGIC   path,
// MAGIC   parsed:document:pages::STRING AS pages,
// MAGIC   parsed:document:elements::STRING AS elements,
// MAGIC   parsed:corrupted_data::STRING AS corrupted_data,
// MAGIC   parsed:error_status::STRING AS error_status,
// MAGIC   parsed:metadata::STRING AS metadata
// MAGIC FROM corpus;

// COMMAND ----------

// MAGIC %md
// MAGIC ## 3. Advanced Exmple: cast to struct and .*
// MAGIC
// MAGIC The `ai_parse_document` function output can be casted to struct type (`document`, `error_status`, `metadata`).

// COMMAND ----------

// DBTITLE 1,SQL
// MAGIC %sql
// MAGIC WITH parsed_docs AS (
// MAGIC   SELECT
// MAGIC     path,
// MAGIC     CAST(
// MAGIC       ai_parse_document(content) AS STRUCT<
// MAGIC         document STRUCT<
// MAGIC           pages ARRAY<STRUCT<id INT, image_uri STRING>>,
// MAGIC           elements ARRAY<STRUCT<id INT, type STRING, content STRING, bbox ARRAY<STRUCT<coord ARRAY<INT>, page_id STRING>>, description STRING>>
// MAGIC         >,
// MAGIC         error_status ARRAY<STRUCT<error_message STRING, page_id INT>>,
// MAGIC         metadata STRUCT<id STRING, version STRING>
// MAGIC       >
// MAGIC     ) AS parsed
// MAGIC   FROM
// MAGIC     READ_FILES(
// MAGIC       :pdf_path,
// MAGIC       format => 'binaryFile'
// MAGIC     )
// MAGIC )
// MAGIC SELECT
// MAGIC   path,
// MAGIC   parsed.*
// MAGIC FROM
// MAGIC   parsed_docs;

// COMMAND ----------

// DBTITLE 1,python
// MAGIC %skip
// MAGIC %python
// MAGIC        
// MAGIC from pyspark.sql.functions import *
// MAGIC from pyspark.sql.types import *
// MAGIC
// MAGIC schema = StructType([
// MAGIC   StructField("document", StructType([
// MAGIC     StructField("pages", ArrayType(StructType([
// MAGIC       StructField("id", IntegerType()),
// MAGIC       StructField("image_uri", StringType())]))),
// MAGIC     StructField("elements", ArrayType(StructType([
// MAGIC       StructField("id", IntegerType()),
// MAGIC       StructField("type", StringType()),
// MAGIC       StructField("content", StringType()),
// MAGIC       StructField("bounding_box", ArrayType(StructType([
// MAGIC         StructField("coord", ArrayType(IntegerType())),
// MAGIC         StructField("page_id", IntegerType())
// MAGIC       ]))),
// MAGIC       StructField("description", StringType())])))])),
// MAGIC   StructField("error_status", StructType([
// MAGIC     StructField("error_message", StringType()),
// MAGIC     StructField("page_id", IntegerType())])),
// MAGIC   StructField("metadata", StructType([
// MAGIC     StructField("id", StringType()),
// MAGIC     StructField("version", StringType())]))
// MAGIC ])
// MAGIC
// MAGIC df = spark.read.format("binaryFile") \
// MAGIC   .load(pdf_path) \
// MAGIC   .withColumn(
// MAGIC     "parsed",
// MAGIC     expr("ai_parse_document(content)").cast(schema)) \
// MAGIC   .select(
// MAGIC     "path",
// MAGIC     "parsed.*")
// MAGIC display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC from pyspark.sql.functions import *
// MAGIC
// MAGIC # Add your document types below, one per line, inside the array.
// MAGIC # Example: 'invoice', 'receipt', 'contract', etc.
// MAGIC document_types = [
// MAGIC     'invoice',
// MAGIC     'receipt',
// MAGIC     'contract',
// MAGIC     'report',
// MAGIC     'form',
// MAGIC     'letter',
// MAGIC     'Notice of Sale',
// MAGIC     'Real Estate Evaluation'
// MAGIC     # Add more document types here
// MAGIC ]
// MAGIC
// MAGIC df = spark.read.format("binaryFile") \
// MAGIC   .load(pdf_path) \
// MAGIC   .withColumn("parsed", expr("ai_parse_document(content)")) \
// MAGIC   .withColumn(
// MAGIC       "doc_type",
// MAGIC       expr(f"ai_classify(CAST(parsed:document AS STRING), ARRAY({', '.join([repr(dt) for dt in document_types])}))")
// MAGIC   )
// MAGIC
// MAGIC display(df.select("path", "doc_type"))
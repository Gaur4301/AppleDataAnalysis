from pyspark.sql.window import Window
from pyspark.sql import functions as f
class transformer:
    def __init__(self):
        pass
    def transform(self,inputDF):
        pass

class FirstTransformer(transformer):
    def transform(self,inputDF):
        """
        Customer who have bought Airpods after buying the Iphone
        """
        transactionInputDF=inputDF.get("transactionInputDF")
        customerInputDF=inputDF.get("customerInputDF")
        print ("transactionInputDF in Transform")
        transactionInputDF.show()
        window=Window.partitionBy("customer_id").orderBy("transaction_date")
        transactionInputDF=transactionInputDF.withColumn("next_product_name",f.lead(f.col("product_name")).over(window))
        transactionInputDF=transactionInputDF.filter((f.col("product_name")=="iPhone") & (f.col("next_product_name")=="AirPods"))
        # transactionInputDF.show()
        joinDF=transactionInputDF.alias("tdf").join(customerInputDF.alias("cdf"),f.col("tdf.customer_id")==f.col("cdf.customer_id")).select("cdf.customer_id","customer_name","location")
        joinDF.show()
        return joinDF

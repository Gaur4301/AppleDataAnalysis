"""%run "./reader_factory"""
class extractor:
    """Abstract class"""
    def __init(self):
        pass
    def extract(self):
        pass
class AirpodsAfterIphoneExtractor(extractor):
    def extract(self):
        """Implementing the steps for reading or extarcting the file"""
        transactionInputDF=get_data_source(
            data_file_type="csv",
            file_path="dbfs:/FileStore/tables/Transaction_Updated.csv"
        ).get_data_frame()

        transactionInputDF.orderBy("customer_id","transaction_date").show()
        #Customer who have bought Airpods after buying the Iphone
        customerInputDF=get_data_source(
            data_file_type="delta",
            file_path="customer_updated_csv"
        ).get_data_frame()
        customerInputDF.show()
        inputDFs={
            "transactionInputDF": transactionInputDF,
            "customerInputDF":customerInputDF
        }
        return inputDFs 
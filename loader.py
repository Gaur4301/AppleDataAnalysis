"""%run "./loader_factory"""
class loader:
    def __init__(self,transformedDF):
        self.transformedDF=transformedDF
    def load(self):
        pass
class AirpodsAfterIphoneloader(loader):
    def load(self):
        get_sink_source(
            sink_type="dbfs",
            df=self.transformedDF,
            path="dbfs:/FileStore/AppleDatasetAnalysis/Output/AirpodsAfterIphone",
            method="overwrite"
            )   
class OnlyAirpodsAndIphone(loader):
    def load(self):
        self.params={
            "partitionByColumns":["location"]
        }
        get_sink_source(
            sink_type="dbfs_with_partition",
            df=self.transformedDF,
            path="dbfs:/FileStore/AppleDatasetAnalysis/Output/OnlyAirpodsAndIphone",
            method="overwrite",
            params=self.params
            )   
        get_sink_source(
            sink_type="delta",
            df=self.transformedDF,
            path="default.OnlyAirpodsAndIphone",
            method="overwrite",
            params=self.params
            )           

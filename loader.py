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

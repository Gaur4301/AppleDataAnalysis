class DataSource:
    """
    Abstaract Class
    """
    def __init__(self,path):
        self.path=path

    def get_data_frame(self):
        """
        Abstract method,Fucntion will be define in subclasses
        """
        raise ValueError("Not Implemented")   

class CSVDataSource(DataSource):
    def get_data_frame(self):
        return (
            spark.\
            read.\
            format("csv").\
            option("header","true").\
            load(self.path)    
                        )
class ParquetDataSource(DataSource):
    def get_data_frame(self):
        return (
            spark.\
            read.\
            format("parquet").\
            load(self.path)    
                        )  

class DeltaDataSource(DataSource):
    def get_data_frame(self):

        return (
            spark.\
            read.\
            table(self.path)   
                        )     

def get_data_source(data_file_type,file_path):
    if data_file_type=="csv":
        return CSVDataSource(file_path)   
    elif data_file_type=="parquet":
        return ParquetDataSource(file_path)
    elif data_file_type=="delta":
        return DeltaDataSource(file_path)    
    else:
        raise ValueError(f"Not Implemented in data_file_type:{data_file_type}")
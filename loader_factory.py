class DataSink:
    """
    Abstaract Class
    """
    def __init__(self,df,path,method,params):
        self.df=df
        self.path=path
        self.method=method
        self.params=params

    def load_data_frame(self):
        """
        Abstract method,Fucntion will be define in subclasses
        """
        raise ValueError("Not Implemented") 
class LoadDataToDBFS(DataSink):
    def load_data_frame(self):
        self.df.write.mode(self.method).save(self.path)   

class LoadDataToDBFSWithPartition(DataSink):
    
    def load_data_frame(self):
        self.partitonByColumns=params.get("partitonByColumns")
        self.df.write.mode(self.method).partitionBy(*self.partitonByColumns).save(self.path)  
def get_sink_source(sink_type,df,path,method,params=None):
    if sink_type=="dbfs":
        return LoadDataToDBFS(df,path,method,params).load_data_frame()
    elif sink_type=="dbfs_with_partition":
        return LoadDataToDBFSWithPartition(df,path,method,params).load_data_frame()
    else:

        return ValueError(f"Not Implemented for sink_type :{sink_type}")
           
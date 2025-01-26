"""%run './transformer'"""
"""%run './extractor'"""
"""%run './loader'"""
class FirstWorkflow:
    """ETL pipeline  to generate all Customers having Airpods After Iphone
    """
    def __init__(self):
        pass
    def runner(self):
        """Step 1: Extract data from all sources"""
        inputDFs=AirpodsAfterIphoneExtractor().extract()
        """Step 2: Implement the Transformation Logic"""
        FirstTransformDF=FirstTransformer().transform(inputDFs)
        """Step 3:Load all the required data to differnt sink"""
        AirpodsAfterIphoneloader(FirstTransformDF).load()


class SecondWorkflow:
    """ETL pipeline  to generate all Customers having Airpods After Iphone
    """
    def __init__(self):
        pass
    def runner(self):
        """Step 1: Extract data from all sources"""
        inputDFs=AirpodsAfterIphoneExtractor().extract()
        """Step 2: Implement the Transformation Logic"""
        FirstTransformDF=FirstTransformer().transform(inputDFs)
        """Step 3:Load all the required data to differnt sink"""
        AirpodsAfterIphoneloader(FirstTransformDF).load()

class WorkflowRunner:
    def __init__(self,name):
        self.name=name
    def runner(self):
        if self.name=="firstWorkflow":
            return FirstWorkflow().runner()
        elif self.name=="secondWorkflow":
            return  SecondWorkflow().runner()

name="firstWorkflow"     
WorkflowRunner(name).runner()           



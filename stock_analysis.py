from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime
class MRStockAnalysis(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_final)
        ]

    
    def mapper(self,key,line:str):       
            data = line.split(sep=",")
            try:
                yield data[0], (float(data[1]),data[2])
            except:
                pass
    def reducer(self,key,vals):
            
            sorted_stock_date = sorted(vals,key = lambda listing: datetime.strptime(listing[1], '%Y-%m-%d'))
            
            stable_or_growing = True
            actualValue = sorted_stock_date[0][0]
            for values in sorted_stock_date:
                if values[0] < actualValue:
                    stable_or_growing = False
            yield None,{
                "name" : key,
                "most_value_stock" : max(sorted_stock_date,key=lambda listing : listing[0]),
                "least_value_stock" : min(sorted_stock_date,key=lambda listing : listing[0]),
                "stable_or_growing" : stable_or_growing
            } 
    def reducer_final(self,key,vals):
        
        stock_list = list(vals)
        
        
        black_day = -1
        
        """
        for every stock
        get least_value
        get date of least_value
        increment by 1 the dict counter
        return key with max value, that0s the black dauu
        """
        
        dates_dict = {}

        for stock in stock_list:
            # Find the least value and its corresponding date
            least_value_date = stock["least_value_stock"][1]
            
            # Increment the counter for this date
            if least_value_date in dates_dict:
                dates_dict[least_value_date] += 1
            else:
                dates_dict[least_value_date] = 1

        # Find the date with the maximum count
        black_day = max(dates_dict, key=dates_dict.get)

        
        
        yield "Value range of stocks", [(stock["name"], stock["most_value_stock"], stock["least_value_stock"]) for stock in stock_list]
        yield "Stocks growing or stable", [stock["name"] for stock in stock_list if stock["stable_or_growing"]]
        yield "Black Day",black_day

            
if __name__ == "__main__":
    MRStockAnalysis.run()
    
    
# map compmany -> (price,date)
"""
reduce



"""
import multiprocessing
from multiprocessing import Process, Pool
import time
import pandas as pd
from tqdm import tqdm

def serial_computation(data:str = '../../datasets/Combined_Flights_2021.csv'):
    start = time.time()
    df = pd.read_csv(data)    
    print(f"Diverted Flights {df[(df.OriginCityName == 'Nashville, TN') & (df.DestCityName == 'Chicago, IL')].AirTime.mean()}")
    final = time.time()
    print(f'time taken (serial computation) is : {round(final-start,2)} sec(s)')

#perform given operation on chunks of data and return result
useful_columns = ['AirTime','OriginCityName','DestCityName']
def map_task(chunk_data,data:str = '../../datasets/Combined_Flights_2021.csv'):
    df = pd.read_csv(data, nrows=chunk_data[0], skiprows=chunk_data[1], header=None, usecols=[12,34,42])    
    #there is no any missing values in below operational columns
    return df[(df.iloc[:,1] == 'Nashville, TN') & (df.iloc[:,2] == 'Chicago, IL')].iloc[:,0].mean()
    

#combine the result from each process and return aggregated value
def reduce_task(process_wise_res:list):
    
    avg_air_time = 0.0
    counter = 0
    for air_time in tqdm(process_wise_res):
        if air_time is not None:
            avg_air_time += air_time
            counter+=1
    print(f'{avg_air_time/counter}  was the average airtime for flights that were flying from Nashville to Chicago')

def multiprocessing_computation():
    def data_distribution(n_rows:int,n_processes):
        chunks_list = []
        skip_rows = 1        
        chunks_list.append([n_rows-1,skip_rows])
        skip_rows = n_rows

        for _ in range(1,n_processes-1): #as we add first chunk and last chunk manually
            chunks_list.append([n_rows,skip_rows])
            skip_rows += n_rows            
        chunks_list.append([None,skip_rows])
        print('Distributed data : ',chunks_list)
        return chunks_list

    
    '''
    Pool can be used for parallel execution of a function across multiple input values, 
    distributing the input data across processes (data parallelism)
    '''
    print(f'No of cores available : {multiprocessing.cpu_count()}')
    no_of_processes = multiprocessing.cpu_count()
    processes = Pool(processes=no_of_processes)
    start = time.time()
    process_wise_reult = processes.map(map_task,data_distribution(n_rows=1500000,n_processes=no_of_processes))
    print(f'process_wise_result : {process_wise_reult}')
    reduce_task(process_wise_reult) 
    processes.close()
    processes.join() #terminate the process
    final = time.time()
    print(f'time taken (multiprocessing computation) is : {round(final-start,2)} sec(s)')

if __name__ == '__main__':
    # serial_computation()
    multiprocessing_computation()

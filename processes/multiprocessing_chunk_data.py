import multiprocessing
from multiprocessing import Process, Pool
import time
import pandas as pd
from tqdm import tqdm

def serial_computation(data:str = '../datasets/region25.csv'):
    start = time.time()
    df = pd.read_csv(data)
    print(f'No of missing values in year column is : {df.year.isna().sum()}')
    final = time.time()
    print(f'time taken (serial computation) is : {round(final-start,2)} sec(s)')

#perform given operation on chunks of data and return result
def map_task(chunk_data,data:str = '../datasets/region25.csv'):
    df = pd.read_csv(data, nrows=chunk_data[0], skiprows=chunk_data[1], header=None)
    return df.iloc[:,5].isna().sum()
    

#combine the result from each process and return aggregated value
def reduce_task(process_wise_res):
    no_of_missing_value = 0
    for value in tqdm(process_wise_res):
        if value is not None:
            no_of_missing_value += value
    print(f'No of missing values in year column is : {no_of_missing_value}')

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
    processes = Pool(processes=no_of_processes-1)
    start = time.time()
    process_wise_reult = processes.map(map_task,data_distribution(n_rows=500000,n_processes=no_of_processes-1))
    print(f'process_wise_result : {process_wise_reult}')
    reduce_task(process_wise_reult) 
    processes.close()
    processes.join() #terminate the process
    final = time.time()
    print(f'time taken (multiprocessing computation) is : {round(final-start,2)} sec(s)')

if __name__ == '__main__':
    # serial_computation()
    multiprocessing_computation()

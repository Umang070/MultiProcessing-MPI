import time
import multiprocessing
from multiprocessing import Process

countryWiseProvinceList = {"india":["Gujarat","MP","UP","Delhi"], "canada":["QC","Ontario","AB","BC"]}
countryNames = ["India","Canada"]
def print_country_provinces(countryName='India'):
    global countryWiseProvinceList

    print(f'{countryName} province list is : {countryWiseProvinceList[countryName.lower()]}')

def compute_serially():
    global countryNames
    start = time.time()

    for country in countryNames:
        print_country_provinces(country)
    
    finished = time.time()
    print(f'time taken (serial execution): {round(finished - start, 2)} second(s)\n')


def compute_multi_processing():
    global countryNames
    print(f'No of CPU(cores) available : {multiprocessing.cpu_count()}')

    process_objects = []
    start = time.time()

    for country in countryNames:        
        process_obj = Process(target=print_country_provinces, args=(country,)) #instantiate a process object with args
        process_objects.append(process_obj)
        process_obj.start()  #the process will run and return its result
    
    #complete the processes    
    for process_obj in process_objects:
        process_obj.join() #we tell the process to complete otherwise process will remain idle and won’t terminate
    finished = time.time()
    print(f'time taken (parallel execution): {round(finished - start, 2)} second(s)\n')
    

if __name__ == "__main__": #confirms that the code is under main function
    compute_serially()
    compute_multi_processing()
    

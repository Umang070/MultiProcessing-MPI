from multiprocessing import Process, Queue, Lock, current_process, Pool
import time
import queue

def task_implementation(task_to_complete, task_which_are_done):
    while True:
        try:
            received_number = task_to_complete.get_nowait() #throw exception queue.Empty if Queue is empty
        except queue.Empty:
            break
        else:
            square_of_number = received_number * received_number
            print('Task no : ',received_number)
            task_which_are_done.put('Square of '+ str(received_number) + ' is ' + str(square_of_number) + ' done by : '+current_process().name)
            time.sleep(0.5)

def task_distribution_queue_process(number_of_task, total_process):
     #Queue class is already synchronized. no need of Lock class to block multiple process to access the same queue object
    task_to_complete = Queue()
    task_which_are_done = Queue()

    for i in range(number_of_task):
        task_to_complete.put(i)
    
    
    process_objects = []

    #1st method without using Pool
    for _ in range(total_process):
        process_obj = Process(target=task_implementation,args=(task_to_complete, task_which_are_done))
        process_objects.append(process_obj)
        process_obj.start()
    for process_obj in process_objects:
        process_obj.join()

    while not task_which_are_done.empty():
         print(task_which_are_done.get()) 
    return True
    

def main():
    total_task = 10
    total_process = 4

    task_distribution_queue_process(total_task, total_process)

if __name__ == "__main__":
    main()
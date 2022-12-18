import os
import sys
import traceback
import multiprocessing as mp

from config import mongo_collection, price_collection


def main(block_num):
    
    #Worker
    worker = os.getpid()
    start_time = time.time()


    def get_traces():
        return



    def upload_content(block_num, results):
        collection.delete_many({'blockchain': 'wax', 'blockNumber': block_num})
        
        item = results
        if item:
            collection.insert_many(item)
            print(f'[{worker}]: {len(item)} Transactions uploaded from block: {block_num} in {round(time.time() - start_time, 2)} seconds ')
        else:
            print(f'[{worker}]: No transactions found from block: {block_num} in {round(time.time() - start_time, 2)} seconds ')

    return upload_content(block_num, get_traces())


def error_handler(exception):
    print(f'{exception} occurred, terminating pool.')
    print(traceback.format_exception(*sys.exc_info()))
    pool.terminate()


if __name__ == "__main__":
    mp.set_start_method('spawn')
    processes = min(8, mp.cpu_count()-1 or 1)
    max_pool_queue_size = 1000
    mp_worker = os.getpid()

    pool = mp.Pool(processes=processes, maxtasksperchild=8)

    init_collection = mongo_collection()

    while True:
        start_block = init_collection.find({'blockchain': 'wax'}).sort('blockNumber', -1).limit(1).next()['blockNumber'] - 50
        end_block = current_block()

        if start_block > (end_block -50):
            print(f'Starting with block: {start_block}, Ending with block: {end_block}')
            print(f'Which is within 50 of end block, sleeping for 60 seconds while blocks mine')
            time.sleep(60)
            continue

        else:
            print(f'Starting with block: {start_block}, Ending with block: {end_block}')
            for i in range(start_block, end_block):
                result = pool.apply_async(main, (i,), error_callback=error_handler)

                while pool._taskqueue.qsize() > max_pool_queue_size:
                    print(f'[{mp_worker}]: {pool._taskqueue.qsize()} tasks in queue, waiting...')
                    result.wait(timeout=30)
                    print(f'[{mp_worker}]: {pool._taskqueue.qsize()} tasks in queue, continuing...')
                    continue


            pool.close()
            time.sleep(0.1)
            pool.join()
            print('-fini-')


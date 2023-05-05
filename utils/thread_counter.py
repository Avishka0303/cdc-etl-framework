#### dynamically allocate the thread count ####
import multiprocessing


def get_thread_count():
    return multiprocessing.cpu_count() * 4  # make thread count is 4 x virtual cpu count

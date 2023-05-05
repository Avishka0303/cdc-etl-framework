def get_site_thread_map(site_list, thread_count):
    """
    divide and map sites to threads.
    :param site_list:
    :param thread_count:
    :return: dictionary { 0:{opco1,opco2,..},1:{opco3,opco4,..},2:{opco5,opco6,..}}
    """
    min_site_count_per_thread = len(site_list) // thread_count
    site_per_thread_map = {}

    for thread_id in range(thread_count):
        start_index = thread_id * min_site_count_per_thread
        end_index = start_index + min_site_count_per_thread
        if thread_id == thread_count - 1:
            end_index = len(site_list)
        site_per_thread_map[thread_id] = site_list[start_index: end_index]

    return site_per_thread_map

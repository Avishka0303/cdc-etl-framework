def get_column_index_from_csv(column_csv, idx_columns):
    column_list = column_csv.split(",")
    idx_column_list = idx_columns.split(",")

    idx_list = []

    for idx_column in idx_column_list:
        idx_list.append(column_list.index(idx_column))

    return idx_list

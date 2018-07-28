import time
import os
from datetime import datetime
import math
import numpy as np


def print_runtime(start, end):
    """
    Function to print the runtime in second
    :param start: starting point runtime
    :param end: ending point runtime
    :return: None
    """
    runtime_in_second = end - start
    runtime_in_h_m_s = time.strftime("%H:%M:%S", time.gmtime(runtime_in_second))
    print('runtime: ', runtime_in_h_m_s)
    return None

    

def list_all_files_of_directory(path, prefix):
    """
    Return path of every file in a given folder
    :param path: the path of the given folder
    :return file_path_list: a list of file path
    """
    file_path_list = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        file_path_list.extend(filenames)
        break
    file_path_list_clean = []
    for file in file_path_list:
        if prefix in file:
            file_path_list_clean.append(file)
    file_path_list = list(map(lambda x: path + x, file_path_list_clean))
    return file_path_list

def create_nasted_file_path_list(file_path_list_clean, length_of_nested_list):
    num_of_files = len(file_path_list_clean)
    nasted_file_path_list = []
    for index in np.arange(0, num_of_files+1, length_of_nested_list):
        start_index = index
        end_index = index + length_of_nested_list
        file_pack = file_path_list_clean[start_index:end_index]
        nasted_file_path_list.append(file_pack)
    return nasted_file_path_list

def get_file_name(file_path_as_str):
    """
    Extract the name of a file from a given file path
    :param file_path_as_str: file path as string
    :return file_name: the file name in the given path
    """
    file_location_split = file_path_as_str.split("/")
    file_name = file_location_split[-1]
    file_name_split = file_name.split(".")
    file_name = file_name_split[0]
    return file_name


def delete_unnamed_column_pandas(df):
    """
    Delete all unnamed columns in a given DataFrame
    :param df: df with unnamed columns
    :return df: return df without unnamed columns
    """
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    return df


def convert_float_to_datetime(date):
    timestamp = datetime.utcfromtimestamp(date)
    return timestamp

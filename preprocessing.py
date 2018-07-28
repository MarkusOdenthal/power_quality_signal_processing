import dask.dataframe as dd
from dask.distributed import Client
import my_utilities
import os
import webbrowser
import pickle


class DataCleaning(object):
    """
    This class is for data preprocessing for the power analyse
    """

    def __init__(self, working_path, sources_path):
        self.working_path = working_path
        self.sources_path = sources_path
        self.parameter_path = self.working_path + "data/parameter/"
        self.other_path = self.working_path + "data/other/"
        self.hu_path = self.working_path + "data/hu/"
        self.hi_path = self.working_path + "data/hi/"

    def create_file_paths(self):
        """
        Creates all important folders where the preprocessed files are stored
        """
        if not os.path.exists(self.hi_path):
            os.makedirs(self.hi_path)
        if not os.path.exists(self.hu_path):
            os.makedirs(self.hu_path)
        if not os.path.exists(self.other_path):
            os.makedirs(self.other_path)
        if not os.path.exists(self.parameter_path):
            os.makedirs(self.parameter_path)

    @staticmethod
    def extract_different_column_names(all_columns):
        """
        Extract the column names for the different preprocessed DataFrames
        :param all_columns: a hole list of all columns names
        :return: new columns list for different DataFrames
                    - hi_data_columns
                    - hu_data_columns
                    - parameter_data_columns
                    - other_data_columns
        """
        hi_data_columns = []
        hu_data_columns = []
        parameter_data_columns = []
        other_data_columns = []
        for column in all_columns:
            if "hi" in column:
                hi_data_columns.append(column)
            elif "hu" in column:
                hu_data_columns.append(column)
            elif ('N' in column) or ('P' in column) or ('S' in column) or \
                    ('THDI' in column) or ('THDU' in column) or ('freq' in column) or \
                    ('rms_i' in column) or ('rms_u' in column):
                other_data_columns.append(column)
            elif ('date' in column) or ('event_count' in column) or ('trigger_param' in column) or (
                    'trigger_value' in column):
                parameter_data_columns.append(column)
            elif ('ts' in column):
                hi_data_columns.append(column)
                hu_data_columns.append(column)
                other_data_columns.append(column)
            elif ('event_index' in column) or ('phase' in column):
                hi_data_columns.append(column)
                hu_data_columns.append(column)
                parameter_data_columns.append(column)
                other_data_columns.append(column)
            else:
                print("Error column '", column, "' cannot be assigned!", sep="")
        return hi_data_columns, hu_data_columns, parameter_data_columns, other_data_columns

    @staticmethod
    def create_reconstructed_header_names(columns):
        """
        Create reconstructed header for the data DataFrames hi, hu and resort the order.
        :param columns: a hole list of all columns names
        :return:    - rename_dict: dictionary to rename column names in pandas DataFrame
                    - rename_column_order: a list to resort the column order of the DataFrame
        """
        rename_dict = {}
        rename_column_order = []
        for column in columns:
            if ("ts" not in column) and ("event_index" not in column) and ("phase" not in column):
                new_name = int(column[2:])
                rename_dict[column] = new_name
                rename_column_order.append(new_name)
        rename_column_order = sorted(rename_column_order)
        rename_column_order.insert(0, "phase")
        rename_column_order.insert(1, "event_index")
        rename_column_order.insert(2, "ts")
        return rename_dict, rename_column_order

    @staticmethod
    def save_data(data, path):
        """
        Repartition the preprocessed files and saves this.
        :param data: the data as DataFrame
        :param path: path we to save the file
        """
        data.columns = map(str, data.columns)
        new_partitions = data.npartitions // 20
        data = data.repartition(npartitions=new_partitions)
        data.to_csv(path, index=None)

    def save_parameter(self, parameter_data):
        """
        Save the parameter data
        :param parameter_data: the parameter data as DataFrame
        """
        parameter_data = parameter_data.drop_duplicates()
        parameter_data = parameter_data.repartition(npartitions=1)
        parameter_data.to_csv(self.parameter_path, index=None)

    @staticmethod
    def data_cleaning(data_column):
        """
        Refill NaN cells for object, int and float formats
        :param data_column: The column to refill NaN
        :return: The column where all NaN values are filled
        """
        if (data_column.dtype == 'float64') or (data_column.dtype == 'float32'):
            return data_column.fillna(0.0)
        elif (data_column.dtype == 'int64') or (data_column.dtype == 'int32'):
            return data_column.fillna(0)
        else:
            return data_column.fillna('empty')

    def run(self):
        """
        Main function of DataCleaning. With this function you can start the hole cleaning process
        """
        script_dir = os.getcwd()
        os.chdir(self.working_path)
        client = Client()
        os.chdir(script_dir)
        webbrowser.open('http://127.0.0.1:8787/status')
        self.create_file_paths()
        print("#" * 80)
        print(" Load data ... ".center(80, "#"))
        print("#" * 80)
        f_meta_data = open('meta_data.pickle', 'rb')
        meta_data = pickle.load(f_meta_data)  # variables come out in the order you put them in
        f_meta_data.close()
        data = dd.read_csv(self.sources_path, dtype=meta_data)
        print("#" * 80)
        print(" File format converting ... ".center(80, "#"))
        print("#" * 80)
        data_delete_unnamed = my_utilities.delete_unnamed_column_pandas(data)
        meta_data_s = data_delete_unnamed._meta.dtypes
        meta_data = list(zip(meta_data_s.index, meta_data_s))
        data_clean = data_delete_unnamed.apply(self.data_cleaning, axis=1, meta=meta_data)
        data_clean["ts"] = data_clean["ts"].apply(my_utilities.convert_float_to_datetime, meta=('ts', 'datetime64[ns]'))
        data_clean = data_clean.categorize(['date', 'event_count', 'phase', 'trigger_param', 'trigger_value'])
        hi_data_columns, hu_data_columns, \
        parameter_data_columns, other_data_columns = self.extract_different_column_names(data_clean.columns)
        hi_data = data_clean[hi_data_columns]
        hu_data = data_clean[hu_data_columns]
        parameter_data = data_clean[parameter_data_columns]
        other_data = data_clean[other_data_columns]

        # preprocess hi data
        hi_new_column_dict, hi_columns_order = self.create_reconstructed_header_names(hi_data.columns)
        hi_data = hi_data.rename(columns=hi_new_column_dict)
        hi_data = hi_data[hi_columns_order]

        # preprocess hu data
        hu_new_column_dict, hu_columns_order = self.create_reconstructed_header_names(hu_data.columns)
        hu_data = hu_data.rename(columns=hu_new_column_dict)
        hu_data = hu_data[hu_columns_order]

        # save data
        print("#" * 80)
        print('Process parameter data ...'.center(80, "#"))
        print("#" * 80)
        self.save_parameter(parameter_data)
        print("#" * 80)
        print('Process hi data ...'.center(80, "#"))
        print("#" * 80)
        self.save_data(hi_data, self.hi_path)
        print("#" * 80)
        print('Process hu data ...'.center(80, "#"))
        print("#" * 80)
        self.save_data(hu_data, self.hu_path)
        print("#" * 80)
        print('Process other data ...'.center(80, "#"))
        print("#" * 80)
        self.save_data(other_data, self.other_path)


if __name__ == '__main__':
    working_path = "/Volumes/fo/"
    sources_path = "/Volumes/fo/PQI_raw/*"
    data_cleaning = DataCleaning(working_path, sources_path)
    data_cleaning.run()
    print("Done!")

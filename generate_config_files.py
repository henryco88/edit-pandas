'''
This script reads data acquisition excel file, then build regex file name and after that creates the configuration files in order to deploy in production or uat.
In particular, the script distributes the drop files equally among the various machines based on the size of each file.

Parameters in file parameters_config_files.ini:
path_input_file_name -> path and file name excel
tot_machine -> total number of machines to drop
limit_size_machine -> max limit size that each machine can process (GB)
size_file_chunk -> max limit size of the file to be ingested for each configuration file (GB)
number_file_chunk -> max limit files number to be ingested for each configuration file
user_name_ssh -> user name ssh to insert in each configuration file
is_drop -> set True if you have to drop, set False if you have to ingest files in uat mode
release -> name release
drop_prefix -> name drop
machine_prefix -> name machine prefix
'''
from dataclasses import dataclass
import configparser
import argparse
import pandas as pd
from config_file_functions import create_path_root, columns_check_rename, create_path_clean, create_regexfile, \
    calculate_size_machine, distribuite_file_chunk


@dataclass
class MachineDetail:
    name: str
    size: list[float]
    percentage: float


def main() -> None:
    print("----- START -------")

    # get parameter file ini from command line
    parser = argparse.ArgumentParser()
    parser.add_argument("file_ini", help="Insert path of the file 'parameters_config_files.ini'", type=str)
    args = parser.parse_args()
    path_file_ini = args.file_ini
    print("The path of the file .ini is " + path_file_ini)

    # get parameters from file parameters_config_files.ini
    config = configparser.ConfigParser()
    config.read(path_file_ini)
    parameters = config["parameters"]

    path_input_file_name = parameters['path_input_file_name']
    tot_machine = parameters.getint('tot_machine')
    limit_size_machine = parameters.getint('limit_size_machine')
    size_file_chunk = parameters.getint('size_file_chunk')
    number_file_chunk = parameters.getint('number_file_chunk')
    user_name_ssh = parameters['user_name_ssh']
    is_drop = parameters.getboolean('is_drop')
    release = parameters['release']
    drop_prefix = parameters['drop_prefix']
    machine_prefix = parameters['machine_prefix']

    # create folders in config/
    path_uat_drop = create_path_root(is_drop, release, drop_prefix)

    # read file excel
    dataframe = pd.read_excel(path_input_file_name, decimal=",")
    print("File excel input: " + path_input_file_name)

    # check column with columns script
    dataframe = columns_check_rename(is_drop, dataframe)

    # create new column "REGEXFILES"
    for i in range(0, len(dataframe)):
        # path clean: not final path with / or // or /./
        dataframe = create_path_clean(i, dataframe)
        # Create string regex from column "FILENAME"
        dataframe = create_regexfile(i, dataframe)
        # create new column "PATH + REGEXFILE". Value column is a concatenation of the columns "PATH_CLEAN" and "REGEXFILE"
        dataframe.at[i, 'PATH_CLEAN + REGEXFILE'] = dataframe.loc[i, 'PATH_CLEAN'] + '/' + dataframe.loc[i, 'REGEXFILE']

    if is_drop:
        dataframe, tot_size_files, size_for_machine = calculate_size_machine(dataframe, tot_machine,
                                                                             limit_size_machine)
        ######### dynamic list ###########
        machine_list = [MachineDetail(machine_prefix + str(i).zfill(2), [], 0) for i in range(1, tot_machine + 1)]
        tot_size = tot_size_files
        n_machine = 0
        limit_size_machine = limit_size_machine * 1000  # Convert in MB

        for index in range(0, len(dataframe)):
            row = dataframe.iloc[index]['FILESIZE_MB']
            if (row <= size_for_machine) and (round(sum(machine_list[n_machine].size) + row, 4) <= size_for_machine) or \
                    (row >= size_for_machine) and (row <= limit_size_machine) and (
                    round(sum(machine_list[n_machine].size), 4) <= limit_size_machine) and (
                    round(sum(machine_list[n_machine].size), 4) <= size_for_machine):
                dataframe.at[index, 'MACHINE_PROD'] = machine_list[n_machine].name
                machine_list[n_machine].size.append(row)
            elif row > limit_size_machine:
                raise Exception("Warning, the file " + dataframe.iloc[index]['FILENAME'] + " with size " + str(
                    dataframe.iloc[index]['FILESIZE_MB']) + " MB, is bigger of limit size machine (" + str(
                    limit_size_machine) + " MB)")

            if index + 1 <= len(dataframe) - 1:
                row_1 = dataframe.iloc[index + 1]['FILESIZE_MB']
            else:
                row_1 = 0

            if (round(sum(machine_list[n_machine].size) + row_1, 4) >= size_for_machine) and (tot_machine > 1):
                tot_size = tot_size - sum(machine_list[n_machine].size)
                n_machine = n_machine + 1  # list start from zero
                tot_machine = tot_machine - 1
                size_for_machine = round((tot_size / tot_machine), 4)

        if dataframe['MACHINE_PROD'].isnull().values.any():
            raise Exception("Null values in the column MACHINE_PROD")

        for machine in machine_list:
            machine.percentage = round((sum(machine.size) / tot_size_files) * 100, 2)
            print("Size used for " + machine.name + ": " + str(round(sum(machine.size), 2)) + " MB - " + \
                  str(round(sum(machine.size) / 1000, 2)) + " GB" + ", so " + str(
                machine.percentage) + " %")

        cols = ['MACHINE_PROD', 'FILESIZE_MB', 'SOURCE', 'FILE_NAME_CONFIG', 'PATH', 'PATH_CLEAN', 'FILENAME', 'REGEXFILE', 'PATH_CLEAN + REGEXFILE']
    else:
        dataframe['MACHINE_PROD'] = ''
        dataframe['FILESIZE_MB'] = 0
        print("Not column 'file size' in dataframe")
        cols = ['SOURCE', 'FILE_NAME_CONFIG', 'PATH', 'PATH_CLEAN', 'FILENAME', 'REGEXFILE','PATH_CLEAN + REGEXFILE']

    dataframe = distribuite_file_chunk(is_drop, dataframe, release, drop_prefix, size_file_chunk, number_file_chunk,
                                       path_uat_drop, user_name_ssh)

    # Create a file in excel with this info: SOURCE, PATH, FILENAME, REGEXFILE, PATH + REGEXFILE, FILESIZE_MB, MACHINE_PROD, FILE_NAME_CONFIG
    path_input_file_name = path_input_file_name.replace(".xlsx", "")
    dataframe = dataframe[cols]
    dataframe.sort_values(by=['SOURCE', 'FILE_NAME_CONFIG'], ascending=[True, True], ignore_index=True, inplace=True)
    dataframe.to_excel(path_input_file_name + '_COMPLETED.xlsx', index=False)
    print("Files created successfully")
    print("----- FINISH -------")


if __name__ == "__main__":
    main()

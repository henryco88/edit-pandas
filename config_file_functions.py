'''
The modules:
- creates the path where the files config will be saved;
- checks and cleans columns;
- creates file confinguration name;
'''

import os
import shutil
import pandas as pd

def create_path_root(is_drop: bool, release: str, drop_prefix: str) -> str:
    '''
    Create path where to write the files config.
    For example:
    a) "uat" mode is config_files/uat/
    b) "drop" mode is config_files/r6_drop_1_1
    '''
    if release =='' or drop_prefix == '' or is_drop == '':
        raise NameError("The parameters 'is_drop','release' or 'drop_prefix' are empty")
    else:
        if is_drop:
            folder_uat_drop = release + "-" + drop_prefix
            print("Configuration mode: drop ")
        else:
            folder_uat_drop = "uat"
            print("Configuration mode: uat ")

        # Create path where to write file config
        path_root = os.path.join('config-files', folder_uat_drop)
        # delete folder if exists
        if os.path.isdir(path_root):
            shutil.rmtree(path_root)
            print("Path exist. It will delete: " + path_root)

        os.makedirs(path_root, exist_ok=True)
        print("Path where the config files will be created: " + path_root)


    return path_root


def columns_check_rename(is_drop: bool, dataframe: pd.DataFrame) -> pd.DataFrame:
    '''
    The columns name edit in upper case and delete space for check and for standardization.
    Return a dataframe with the names of columns renamed.
    '''
    print("Columns in file excel: " + ", ".join(dataframe.columns))
    dataframe = dataframe.rename(str.upper, axis='columns')  # columns change in upper to check columns
    dataframe = dataframe.rename(columns=lambda x: x.replace(" ", ""))
    # rename columns dataframe
    for col in dataframe.columns:
        if 'SOURCE' in col or 'SERVER' in col:
            dataframe.rename(columns={col: 'SOURCE'}, inplace=True)

        if 'PATH' in col:
            dataframe.rename(columns={col: 'PATH'}, inplace=True)

        if ('NAME' in col and 'FILE' in col) or (col == 'FILE'):
            dataframe.rename(columns={col: 'FILENAME'}, inplace=True)

        if ('FILE' in col and 'MB' in col) or (('FILE' in col and 'SIZE' in col) and not any(c in col for c in('BLOCKS','KB','MB','GB','FREQUENCY')) ):
            dataframe.rename(columns={col: 'FILESIZE_MB'}, inplace=True)

    print("Columns renamed: " + " , ".join(dataframe.columns))
    if is_drop:
        list_column_check = ["SOURCE", "PATH", "FILENAME", "FILESIZE_MB"]
    else:
        list_column_check = ["SOURCE", "PATH", "FILENAME"]

    if ("FILESIZE_MB" in dataframe.columns and is_drop is False) or ("FILESIZE_MB" not in dataframe.columns and is_drop is True):
        raise NameError("Mismatch parameters between 'path_input_file_name' and 'is_drop'" )

    # check if columns dataframe is equal with the columns used in script (list_column_check)
    if set(list_column_check).issubset(dataframe.columns):
        print("All columns are available in the dataframe")
    else:
        raise NameError("Columns dataframe not match with 'list_column_check'")

    dataframe = dataframe[list_column_check]

    dataframe["SOURCE"] = dataframe["SOURCE"].str.lower()
    # strip all values
    dataframe = dataframe.applymap(lambda x: x.strip() if type(x) == str else x)

    return dataframe

def create_path_clean(i: int, dataframe: pd.DataFrame) -> pd.DataFrame:
    '''
    Take the column 'PATH' and delete the char / or //  or /./ at end string
    Return a dataframe with a new column 'PATH_CLEAN'
    '''
    if (dataframe.at[i, 'PATH'][-1:] == "/") and not(dataframe.at[i, 'PATH'][-3:] == "/./") and not(dataframe.at[i, 'PATH'][-2:] == "//"):
        dataframe.at[i, 'PATH_CLEAN'] = dataframe.at[i, 'PATH'][:-1]
    elif dataframe.at[i, 'PATH'][-2:] == "//":
        dataframe.at[i, 'PATH_CLEAN'] = dataframe.at[i, 'PATH'][:-2]
    elif dataframe.at[i, 'PATH'][-3:] == "/./":
        dataframe.at[i, 'PATH_CLEAN'] = dataframe.at[i, 'PATH'][:-3]
    else:
        dataframe.at[i, 'PATH_CLEAN'] = dataframe.at[i, 'PATH']

    return dataframe


def create_regexfile(i: int, dataframe: pd.DataFrame) -> pd.DataFrame:
    '''
    Take the column 'FILENAME' and create regex files
    Return a dataframe with a new column 'REGEXFILE'
    '''
    dataframe.at[i, 'REGEXFILE'] = dataframe.loc[i, 'FILENAME'] \
        .replace('.', r"\\.") \
        .replace('*', '.*') \
        .replace('YYYYMMDDhhmmss', r'\\d{14}') \
        .replace('YYYYMMDD', r'\\d{8}') \
        .replace('?', '.')
    return dataframe

def calculate_size_machine(dataframe: pd.DataFrame, tot_machine:int, limit_size_machine: int) -> tuple[pd.DataFrame, float, float]:
    '''
    Convert column FILESIZE_MB from string to float (es: 35 KB -> 0.035 )
    Return:
    1) dataframe: a dataframe sorted for FILESIZE_MB
    2) tot_size_files: total size of files to be ingested
    3) size_for_machine: max size for each machine
    '''
    # Clean column file size if is string
    if dataframe['FILESIZE_MB'].dtype == 'O':
        for i in range(0, len(dataframe)):
            row = dataframe.at[i, 'FILESIZE_MB']
            if 'MB' in row:
                dataframe.at[i, 'FILESIZE_MB'] = float(row.replace('MB','').replace(' ',''))
            if 'GB' in row:
                row_number = float(row.replace('GB','').replace(' ',''))
                dataframe.at[i, 'FILESIZE_MB'] = row_number * 1000
            if 'KB' in row:
                row_number = float(row.replace('KB','').replace(' ',''))
                dataframe.at[i, 'FILESIZE_MB'] = row_number / 1000

    # Calculate max size for machine
    tot_size_files = dataframe['FILESIZE_MB'].sum(axis=0)
    if tot_machine == 0:
        raise NameError(f"The parameters tot_machine is {tot_machine}. It can't 0")
    try:
        size_for_machine = round((tot_size_files / tot_machine), 4)
    except Exception as error:
        print("Error: " + str(error))
        raise
    print("Total size: " + str(round(tot_size_files, 2)) + " MB - " + str(round(tot_size_files/1000, 2)) + " GB")
    print("Max size for machine: " + str(round(size_for_machine, 2)) + " MB - " + str(round(size_for_machine/1000, 2)) + " GB")
    print("Limit size one file for machine: " + str(limit_size_machine) + " GB")

    dataframe.sort_values(by=['FILESIZE_MB'], ascending=False, ignore_index=True, inplace=True)

    return dataframe, tot_size_files, size_for_machine



def create_file_name(is_drop: bool, release: str, drop_prefix: str, chunk: int, source: str) -> str:
    '''
    Create the name of config file.
    Variable "chunk" is number file that is created.
    '''
    if is_drop:
        file_name_config = release + "-" + drop_prefix + "-" + source + "-chunk-" + str(chunk)
    else:
        file_name_config = source + "-chunk-" + str(chunk)

    return file_name_config


def create_path_config_file(is_drop: bool, parent_path: str, machine_prod: str, file_name_config: str) -> str:
    '''
    Take file_name, return a concatenation of path and file_name.
    For example:
    a) "uat" mode is config_files/uat/pla-w02hdped06-chunk_1
    b) "drop" mode is config_files/r6-drop-1-1/pla_w02dna01/r6-drop-1-1-pla-w02hdped06-chunk-1
    '''
    if is_drop:
        path_drop_machine = os.path.join(parent_path, machine_prod)
        os.makedirs(path_drop_machine, exist_ok=True)
        path_config = os.path.join(path_drop_machine, file_name_config)
    else:
        path_config = os.path.join(parent_path, file_name_config)

    return path_config


def create_file(path_config_file: str, list_regex_to_string: str, source: str, user_name_ssh: str) -> None:
    '''
    Create file
    '''
    with open(path_config_file, "w", encoding='utf-8') as file:
        file.write("[remote_source]\n")
        file.write("watch_dir = []\n")
        file.write("watch_files = []\n")
        file.write("watch_files_regex = [\"" + list_regex_to_string + "\"]\n")
        file.write("hostname = " + source + "\n")
        file.write("username = " + user_name_ssh + "\n")
        file.write("privatekeyfile = /home/users/" + user_name_ssh + "/.ssh/id_rsa \n")
        file.write("source_name = abinitio")

def distribuite_file_chunk(is_drop: bool, dataframe: pd.DataFrame, release: str, drop_prefix: str, size_file_chunk: int, number_file_chunk: int, path_root: str, user_name_ssh: str) -> pd.DataFrame:
    '''
    Take a dataframe sorted and create a new column with the file name config.
    The file name config is distribuite for each machine based on source, number files and size machine.
    '''
    dataframe.sort_values(by=['SOURCE', 'MACHINE_PROD', 'FILESIZE_MB'], ascending=[True, True, False],ignore_index=True, inplace=True)

    list_files_regex = []
    list_size = []

    chunk = 1
    for index in range(0,len(dataframe)):
        list_files_regex.append(dataframe.iloc[index]['PATH_CLEAN + REGEXFILE'])
        list_size.append(dataframe.iloc[index]['FILESIZE_MB'])
        source = dataframe.iloc[index]['SOURCE']
        machine_prod = dataframe.iloc[index]['MACHINE_PROD']

        file_name_config = create_file_name(is_drop, release, drop_prefix, chunk, source)

        dataframe.at[index, 'FILE_NAME_CONFIG'] = file_name_config

        # condition for chunk
        if (index == len(dataframe) - 1) or \
                (sum(list_size) >= size_file_chunk) or \
                ((sum(list_size) + dataframe.iloc[index + 1]['FILESIZE_MB']) >= size_file_chunk) or \
                (len(list_files_regex) >= number_file_chunk) or \
                (source != dataframe.iloc[index + 1]['SOURCE']) or \
                (machine_prod != dataframe.iloc[index + 1]['MACHINE_PROD']):
            list_regex_to_string = '","'.join(list_files_regex)

            path_config_file = create_path_config_file(is_drop, path_root, machine_prod, file_name_config)

            create_file(path_config_file, list_regex_to_string, source, user_name_ssh)

            list_files_regex = []
            list_size = []

            if (index < len(dataframe) - 1) and (source == dataframe.iloc[index + 1]['SOURCE']):
                chunk += 1
            else:
                chunk = 1

    return dataframe

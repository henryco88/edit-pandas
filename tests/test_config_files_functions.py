from config_file_functions import *
import pytest
from pytest import approx
import pandas as pd
import datatest as dt
import os

''' Dataframe Test'''


def df_drop():
    path_input_file_name = os.path.join('tests', 'data', 'input', 'Release 6 Drop 1 File Watcher Configuration.xlsx')
    return pd.read_excel(path_input_file_name, decimal=",")


def df_drop_2():
    path_input_file_name = os.path.join('tests', 'data', 'input', 'drop 7-1-2.xlsx')
    return pd.read_excel(path_input_file_name, decimal=",")


def df_drop_wrong():
    path_input_file_name = os.path.join('tests', 'data', 'input',
                                        'Release 6 Drop 1 File Watcher Configuration_WRONG.xlsx')
    dataframe = pd.read_excel(path_input_file_name, decimal=",")
    return dataframe


def df_uat():
    path_input_file_name = os.path.join('tests', 'data', 'input', '64 Hadoop files to be configured.xlsx')
    return pd.read_excel(path_input_file_name, decimal=",")


def df_uat_wrong():
    path_input_file_name = os.path.join('tests', 'data', 'input', '64 Hadoop files to be configured_WRONG.xlsx')
    return pd.read_excel(path_input_file_name, decimal=",")


def df_test_calculate_size():
    data = {
        'SOURCE': ['pla-w02hdped01', 'pla-w02hdped06', 'pla-w02hdped08'],
        'PATH': ['/grid/0/data/scm/forecast/forecastdataset/', '/grid/0/data/idh/dae/retail/pos_promo/inbound/./',
                 '/grid/0/data/idh/dae/pharmacy/ScriptAlignment/inbound'],
        'PATH_CLEAN': ['/grid/0/data/scm/forecast/forecastdataset', '/grid/0/data/idh/dae/retail/pos_promo/inbound',
                       '/grid/0/data/idh/dae/pharmacy/ScriptAlignment/inbound'],
        'FILENAME': ['sma_dim_mnth_spcplano_data_*.dat', '*.fct_day_str_upc.dat_*', 'ACAPEXT005_ms_*.dat'],
        'REGEXFILE': ['sma_dim_mnth_spcplano_data_*.dat$', '*.fct_day_str_upc.dat_*$', 'ACAPEXT005_ms_*.dat'],
        'PATH_CLEAN + REGEXFILE': ['/grid/0/data/idh/dae/Core-HR/sma_dim_mnth_spcplano_data_*.dat',
                                   '/grid/0/data/scm/sales_rdi_source/*.fct_day_str_upc.dat_*',
                                   '/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT005_ms_*.dat'],
        'FILESIZE_MB': [4340.82, 1264.66, 598.23]
    }

    return pd.DataFrame(data)


def df_test_calculate_size_2():
    data = {
        'SOURCE': ['pla-w02hdped01', 'pla-w02hdped06', 'pla-w02hdped08'],
        'PATH': ['/grid/0/data/scm/forecast/forecastdataset/', '/grid/0/data/idh/dae/retail/pos_promo/inbound/./',
                 '/grid/0/data/idh/dae/pharmacy/ScriptAlignment/inbound'],
        'PATH_CLEAN': ['/grid/0/data/scm/forecast/forecastdataset', '/grid/0/data/idh/dae/retail/pos_promo/inbound',
                       '/grid/0/data/idh/dae/pharmacy/ScriptAlignment/inbound'],
        'FILENAME': ['sma_dim_mnth_spcplano_data_*.dat', '*.fct_day_str_upc.dat_*', 'ACAPEXT005_ms_*.dat'],
        'REGEXFILE': ['sma_dim_mnth_spcplano_data_*.dat$', '*.fct_day_str_upc.dat_*$', 'ACAPEXT005_ms_*.dat'],
        'PATH_CLEAN + REGEXFILE': ['/grid/0/data/idh/dae/Core-HR/sma_dim_mnth_spcplano_data_*.dat',
                                   '/grid/0/data/scm/sales_rdi_source/*.fct_day_str_upc.dat_*',
                                   '/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT005_ms_*.dat'],
        'FILESIZE_MB': ['300 MB', '1.5 GB', '60 KB']
    }

    return pd.DataFrame(data)


def df_test_drop():
    data = {
        'SOURCE': ['pla-w02hdped01', 'pla-w02hdped06', 'pla-w02hdped08'],
        'PATH': ['/grid/0/data/scm/forecast/forecastdataset/', '/grid/0/data/scm/forecast/forecastdataset//',
                 '/grid/0/data/scm/forecast/forecastdataset/./'],
        'PATH_CLEAN': ['/grid/0/data/scm/forecast/forecastdataset', '/grid/0/data/scm/forecast/forecastdataset',
                       '/grid/0/data/scm/forecast/forecastdataset'],
        'FILENAME': ['sma_dim_mnth_spcplano_data_*.dat', '*.fct_day_str_upc.dat_*', 'ACAPEXT005_ms_*.dat'],
        'REGEXFILE': ['sma_dim_mnth_spcplano_data_*.dat$', '*.fct_day_str_upc.dat_*$', 'ACAPEXT005_ms_*.dat'],
        'PATH_CLEAN + REGEXFILE': ['/grid/0/data/idh/dae/Core-HR/sma_dim_mnth_spcplano_data_*.dat',
                                   '/grid/0/data/scm/sales_rdi_source/*.fct_day_str_upc.dat_*',
                                   '/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT005_ms_*.dat'],
        'FILESIZE_MB': [4.34, 1.26, 0.59],
        'MACHINE_PROD': ['pla-w02dna01', 'pla-w02dna02', 'pla-w02dna02']
    }

    return pd.DataFrame(data)


def df_test_uat():
    data = {
        'SOURCE': ['pla-w02hdped01', 'pla-w02hdped06', 'pla-w02hdped08'],
        'PATH': ['/grid/0/data/scm/forecast/forecastdataset', '/grid/0/data/idh/dae/retail/pos_promo/inbound',
                 '/grid/0/data/idh/dae/pharmacy/ScriptAlignment/inbound'],
        'PATH_CLEAN': ['/grid/0/data/scm/forecast/forecastdataset', '/grid/0/data/idh/dae/retail/pos_promo/inbound',
                       '/grid/0/data/idh/dae/pharmacy/ScriptAlignment/inbound'],
        'FILENAME': ['sma_dim_mnth_spcplano_data_*.dat', '*.fct_day_str_upc.dat_*', 'ACAPEXT005_ms_*.dat'],
        'REGEXFILE': ['sma_dim_mnth_spcplano_data_*.dat$', '*.fct_day_str_upc.dat_*$', 'ACAPEXT005_ms_*.dat$'],
        'PATH_CLEAN + REGEXFILE': ['/grid/0/data/idh/dae/Core-HR/sma_dim_mnth_spcplano_data_*.dat$',
                                   '/grid/0/data/scm/sales_rdi_source/*.fct_day_str_upc.dat_*$',
                                   '/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT005_ms_*.dat$'],
        'FILESIZE_MB': [0, 0, 0],
        'MACHINE_PROD': ['', '', '']
    }

    return pd.DataFrame(data)


'''Test create_path_root()'''


def test_create_path_root_is_drop_true():
    assert create_path_root(True, 'r6', 'drop-1') == os.path.join('config-files', 'r6-drop-1')


def test_create_path_root_is_drop_false():
    assert create_path_root(False, 'r6', 'drop-1') == os.path.join('config-files', 'uat')


def test_create_path_root_parameters_empty():
    with pytest.raises(NameError) as excinfo:
        create_path_root(True, 'r6', '')
    assert "The parameters 'is_drop','release' or 'drop_fix' are empty" in str(excinfo.value)
    with pytest.raises(NameError) as excinfo:
        create_path_root(True, '', 'drop-1')
    assert "The parameters 'is_drop','release' or 'drop_fix' are empty" in str(excinfo.value)
    with pytest.raises(NameError) as excinfo:
        create_path_root(True, '', '')
    assert "The parameters 'is_drop','release' or 'drop_fix' are empty" in str(excinfo.value)

    with pytest.raises(NameError) as excinfo:
        create_path_root(False, 'r6', '')
    assert "The parameters 'is_drop','release' or 'drop_fix' are empty" in str(excinfo.value)
    with pytest.raises(NameError) as excinfo:
        create_path_root(False, '', 'drop-1')
    assert "The parameters 'is_drop','release' or 'drop_fix' are empty" in str(excinfo.value)
    with pytest.raises(NameError) as excinfo:
        create_path_root(False, '', '')
    assert "The parameters 'is_drop','release' or 'drop_fix' are empty" in str(excinfo.value)


'''Test columns_check_rename()'''


def test_columns_check_rename_is_drop_true():
    dt.validate(
        columns_check_rename(True, df_drop()).columns,
        {'SOURCE', 'PATH', 'FILENAME', 'FILESIZE_MB'},
    )


def test_columns_check_rename_is_drop_true_2():
    dt.validate(
        columns_check_rename(True, df_drop_2()).columns,
        {'SOURCE', 'PATH', 'FILENAME', 'FILESIZE_MB'},
    )


def test_columns_check_rename_is_drop_false():
    with pytest.raises(NameError) as excinfo:
        columns_check_rename(False, df_drop()).columns


def test_columns_check_rename_uat_is_drop_false():
    dt.validate(
        columns_check_rename(False, df_uat()).columns,
        {'SOURCE', 'PATH', 'FILENAME'},
    )


def test_columns_check_rename_uat_is_drop_true():
    with pytest.raises(NameError) as excinfo:
        columns_check_rename(True, df_uat()).columns


def test_columns_check_rename_not_check_columns_drop():
    with pytest.raises(NameError) as excinfo:
        columns_check_rename(True, df_drop_wrong())
    assert "Columns dataframe not match with 'list_column_check'" in str(excinfo.value)


def test_columns_check_rename_not_check_columns_uat():
    with pytest.raises(NameError) as excinfo:
        columns_check_rename(False, df_uat_wrong())
    assert "Columns dataframe not match with 'list_column_check'" in str(excinfo.value)


'''Test create_path_clean'''


def test_create_path_clean():
    data = {
        'PATH': ['/grid/0/data/scm/forecast/forecastdataset',
                 '/grid/0/data/scm/forecast/forecastdataset/',
                 '/grid/0/data/scm/forecast/forecastdataset//',
                 '/grid/0/data/scm/forecast/forecastdataset/./']
    }
    dataframe_test = pd.DataFrame(data)

    # case /
    i = 0
    dataframe = create_path_clean(i, dataframe_test)
    assert dataframe.at[i, 'PATH_CLEAN'] == '/grid/0/data/scm/forecast/forecastdataset'
    # case /
    i = 1
    dataframe = create_path_clean(i, dataframe_test)
    assert dataframe.at[i, 'PATH_CLEAN'] == '/grid/0/data/scm/forecast/forecastdataset'
    # case //
    i = 2
    dataframe = create_path_clean(i, dataframe_test)
    assert dataframe.at[i, 'PATH_CLEAN'] == '/grid/0/data/scm/forecast/forecastdataset'
    # case /./
    i = 3
    dataframe = create_path_clean(i, dataframe_test)
    assert dataframe.at[i, 'PATH_CLEAN'] == '/grid/0/data/scm/forecast/forecastdataset'


'''Test create_regexfile'''


def test_create_regexfile():
    data = {
        'FILENAME': ['sma_dim_mnth_spcplano_data_.dat',
                     'fct_day_str_upc_*',
                     'ACAPEXT005_ms_YYYYMMDDhhmmss',
                     'ACAPEXT005_ms_.YYYYMMDD',
                     'OTFR_CR_RPT????']
    }
    dataframe_test = pd.DataFrame(data)

    # case /
    i = 0
    dataframe = create_regexfile(i, dataframe_test)
    assert dataframe.at[i, 'REGEXFILE'] == r'sma_dim_mnth_spcplano_data_\\.dat'
    # case /
    i = 1
    dataframe = create_regexfile(i, dataframe_test)
    assert dataframe.at[i, 'REGEXFILE'] == r'fct_day_str_upc_.*'
    # case //
    i = 2
    dataframe = create_regexfile(i, dataframe_test)
    assert dataframe.at[i, 'REGEXFILE'] == r'ACAPEXT005_ms_\\d{14}'
    # case /./
    i = 3
    dataframe = create_regexfile(i, dataframe_test)
    assert dataframe.at[i, 'REGEXFILE'] == r'ACAPEXT005_ms_\\.\\d{8}'
    i = 4
    dataframe = create_regexfile(i, dataframe_test)
    assert dataframe.at[i, 'REGEXFILE'] == r'OTFR_CR_RPT....'


''' Test test_calculate_size_machine()'''
def test_calculate_size_machine():
    dataframe_check, tot_dimension_gb_check, size_for_machine_check = calculate_size_machine(df_test_calculate_size(),
                                                                                             3, 100)
    dt.validate(
        dataframe_check.columns,
        {'SOURCE', 'PATH', 'FILENAME', 'PATH_CLEAN', 'REGEXFILE', 'PATH_CLEAN + REGEXFILE', 'FILESIZE_MB'}
    )

    dataframe_check_2, tot_dimension_gb_check, size_for_machine_check = calculate_size_machine(
        df_test_calculate_size_2(), 3, 100)
    dt.validate(
        dataframe_check.columns,
        {'SOURCE', 'PATH', 'FILENAME', 'PATH_CLEAN', 'REGEXFILE', 'PATH_CLEAN + REGEXFILE', 'FILESIZE_MB'}
    )

    df_test_calculate_size().sort_values(by=['FILESIZE_MB'], ascending=False, ignore_index=True, inplace=True)
    df_test_calculate_size_2().sort_values(by=['FILESIZE_MB'], ascending=False, ignore_index=True, inplace=True)

    assert isinstance(tot_dimension_gb_check, float)
    assert isinstance(size_for_machine_check, float)


def test_calculate_size_machine_tot_machine_zero():
    with pytest.raises(NameError):
        calculate_size_machine(df_test_calculate_size(), 0, 100)


''' Test create_file_name()'''


def test_create_file_name_is_drop_true():
    assert create_file_name(True, 'r6', 'drop-1', 4, 'pla-w02hdped06') == 'r6-drop-1-pla-w02hdped06-chunk-4'


def test_create_file_name_is_drop_false():
    assert create_file_name(False, 'r6', 'drop-1', 5, 'pla-w02hdped02') == 'pla-w02hdped02-chunk-5'


''' Test create_path_config_file()'''


def test_create_path_config_file_is_drop_true():
    path_root = os.path.join('config-files', 'r6-drop-1')
    path_config_file = create_path_config_file(True, path_root, 'pla-w02dna01', 'r6-drop-1-pla-w02hdped06-chunk-4')

    assert path_config_file == os.path.join(path_root, 'pla-w02dna01', 'r6-drop-1-pla-w02hdped06-chunk-4')
    path_exists = os.path.join(path_root, 'pla-w02dna01')
    assert os.path.exists(path_exists)


def test_create_path_config_file_is_drop_false():
    path_root = os.path.join('config-files', 'uat')
    path_config_file = create_path_config_file(False, path_root, '', 'pla-w02hdped06-chunk-4')

    assert path_config_file == os.path.join(path_root, 'pla-w02hdped06-chunk-4')


''' Test create_file() '''


def test_create_file_drop():
    path_root = os.path.join('config-files', 'r6-drop-1')
    path_config_file = os.path.join(path_root, 'pla-w02dna01', 'r6-drop-1-pla-w02hdped08-chunk-14')
    list_files_regex = ["/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT007_ms_.*\.dat",
                        "/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT013_ms_.*\.dat",
                        "/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT009_ms_.*\.dat",
                        "/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT014_ms_.*\.dat",
                        "/grid/0/data/idh/dae/member_source_lookup/archive/www_member_delta_.*\.dat",
                        "/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT010_ms_.*\.dat",
                        "/grid/0/data/idh/dae/member_source_lookup/archive/WWW_Source_System_Lookup_Extract_.*\.dat"]
    list_regex_to_string = '","'.join(list_files_regex)
    create_file(path_config_file, list_regex_to_string, 'pla-w02hdped08', 'svcDAFIP')

    assert os.path.exists(path_config_file)


def test_create_file_uat():
    path_root = os.path.join('config-files', 'uat')
    path_config_file = os.path.join(path_root, 'pla-w02hdped06-chunk-4')
    list_files_regex = ["/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT007_ms_.*\.dat",
                        "/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT013_ms_.*\.dat",
                        "/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT009_ms_.*\.dat",
                        "/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT014_ms_.*\.dat",
                        "/grid/0/data/idh/dae/member_source_lookup/archive/www_member_delta_.*\.dat",
                        "/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT010_ms_.*\.dat",
                        "/grid/0/data/idh/dae/member_source_lookup/archive/WWW_Source_System_Lookup_Extract_.*\.dat"]
    list_regex_to_string = '","'.join(list_files_regex)
    create_file(path_config_file, list_regex_to_string, 'pla-w02hdped08', 'svcDAFIP')

    assert os.path.exists(path_config_file)


def test_create_file_content():
    path_root = os.path.join('config-files', 'r6-drop-1')
    path_config_file = os.path.join(path_root, 'pla-w02dna01', 'r6-drop-1-pla-w02hdped08-chunk-14')
    list_files_regex = ["/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT007_ms_.*\.dat",
                        "/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT013_ms_.*\.dat",
                        "/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT009_ms_.*\.dat",
                        "/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT014_ms_.*\.dat",
                        "/grid/0/data/idh/dae/member_source_lookup/archive/www_member_delta_.*\.dat",
                        "/grid/0/data/idh/dae/retail/sms_acapdb/inbound/ACAPEXT010_ms_.*\.dat",
                        "/grid/0/data/idh/dae/member_source_lookup/archive/WWW_Source_System_Lookup_Extract_.*\.dat"]
    list_regex_to_string = '","'.join(list_files_regex)
    create_file(path_config_file, list_regex_to_string, 'pla-w02hdped09', 'svcDAFIP')

    with open(path_config_file) as file:
        for item in file:
            # print(item)
            if 'watch_files_regex' in item:
                list_regex_file_check = item[21:-2].split(",")
                assert isinstance(list_regex_file_check, list)
            if 'hostname' in item:
                source_check = item[11:].strip()
                assert source_check == "pla-w02hdped09"
            if 'username' in item:
                username_check = item[11:].strip()
                assert username_check == "svcDAFIP"
            if 'privatekeyfile' in item:
                privatekeyfile_check = item[17:].strip()
                print(privatekeyfile_check)
                assert privatekeyfile_check == "/home/users/svcDAFIP/.ssh/id_rsa"


''' Test test_distribuite_file_chunk()'''


def test_distribuite_file_chunk_is_drop_true():
    path_root = os.path.join('config-files', 'r6-drop-1')

    df_check = distribuite_file_chunk(True, df_test_drop(), 'r6', 'drop-1', 2, 20, path_root, 'svcDAFIP')
    dt.validate(
        df_check.columns,
        {'SOURCE', 'PATH', 'FILENAME', 'PATH_CLEAN', 'REGEXFILE', 'PATH_CLEAN + REGEXFILE', 'FILESIZE_MB',
         'MACHINE_PROD', 'FILE_NAME_CONFIG'}
    )


def test_distribuite_file_chunk_is_drop_false():
    path_root = os.path.join('config-files', 'uat')

    df_check = distribuite_file_chunk(False, df_test_uat(), 'r6', 'drop-1', 2, 20, path_root, 'svcDAFIP')
    dt.validate(
        df_check.columns,
        {'SOURCE', 'PATH', 'FILENAME', 'PATH_CLEAN', 'REGEXFILE', 'PATH_CLEAN + REGEXFILE', 'FILE_NAME_CONFIG'}
    )

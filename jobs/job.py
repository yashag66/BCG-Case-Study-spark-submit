'''
Author: Yash Agarwal
'''

# Command to execute to run Job
# $SPARK_HOME/bin/spark-submit \
# --py-files dependencies.zip \
# --files configs/file_configs.json \
# jobs/job.py


from dependencies.spark import get_spark_session
from dependencies.transform import Transform


def main():
    """
    Start Spark application and get Spark session.(Main Job Definition)
    return: None
    """
    # 
    spark, config = get_spark_session(
        app_name='case_study',
        files=['configs/file_configs.json'])

    # logging the start of the job(using print statement, can be changed to leverage logger)
    print('Job is starting')

    print(config)

    # execute the job
    print('Starting to run extract process')
    data_collection = extract_data(spark, config['INPUT_FILE_PATH'])
    print('Completed running extract process')

    print('Starting to run transform process')    
    data_transformed = transform_data(data_collection)
    print('Completed running transform process')

    print('Starting to run load process')
    load_data(data_transformed, config['OUTPUT_FILE_PATH'])
    print('Completed running load process')

    # logging the sucess of the job
    print('Job is executed')
    spark.stop()
    return None


def extract_data(spark, input_config):
    """Load data from CSV file format.
    :param spark: Spark session object.
    :param input_config: Configs to read data using filepaths
    :return: Spark DataFrame.
    """

    def read_csv_df(path='', header=True, inferSchema=True):
        """Method to read CSV file and return as a spark dataframe
           This function will go through the input once to determine the input schema if
           'inferSchema' is enabled. To avoid going through the entire data once, disable
           
            param path: string for input path.
            param inferSchema: bool, optional
                infers the input schema automatically from data. It requires one extra
                pass over the data. It uses the default value, 'True'.
            param header: bool, optional
                uses the first line as names of columns. It uses the default value, 'True'.
        """
        if path:
            return spark.read.csv(path, header=header, inferSchema=inferSchema).dropDuplicates()
        raise Exception('Please provide file path')


    df_collection = {}

    # Reading Primary Person CSV Data in a spark dataframe
    df_collection['primary_person_df'] = read_csv_df(input_config['PRIMARY_PERSON_DATA_PATH'], \
        header=True, inferSchema=True)

    # Reading Units CSV Data in a spark dataframe
    df_collection['units_df'] = read_csv_df(input_config['UNITS_DATA_PATH'], \
        header=True, inferSchema=True)

    # Reading Charges CSV Data in a spark dataframe
    df_collection['charges_df'] = read_csv_df(input_config['CHARGES_DATA_PATH'], \
        header=True, inferSchema=True)

    # Reading Damages CSV Data in a spark dataframe
    df_collection['damages_df'] = read_csv_df(input_config['DAMAGES_DATA_PATH'], \
        header=True, inferSchema=True)

    return df_collection


def transform_data(df_collection):
    """
    Transform original dataset.
    :param df_collection: Dictionary of Input DataFrame.
    :return: Transformed DataFrame.
    """

    transform_collection = {}
    
    # Analysis 1
    transform_collection['killed_male_df'] = Transform \
                    .analysis_1_count_crashes_killed_male(df_collection['primary_person_df'])

    # Analysis 2
    transform_collection['two_wheelers_crashes_df'] = Transform \
                    .analysis_2_two_wheelers_crashes(df_collection['units_df'])

    # Analysis 3
    transform_collection['accident_female_df'] = Transform \
                    .analysis_3_state_highest_female_accidents(df_collection['primary_person_df'])

    # Analysis 4
    transform_collection['top_5_to_15_veh_make_id'] = Transform \
                    .analysis_4_top_5_to_15_veh_make_id(df_collection['primary_person_df'], df_collection['units_df'])

    # Analysis 5
    transform_collection['top_ethnic_user_grp'] = Transform \
                    .analysis_5_top_ethnic_user_group_each_body_style(df_collection['primary_person_df'], df_collection['units_df'])

    # Analysis 6
    transform_collection['top_5_crashed_car_alcohol'] = Transform \
                    .analysis_6_top_5_zip_codes_crash_alcohol(df_collection['primary_person_df'], df_collection['units_df'])

    # Analysis 7
    transform_collection['distinct_crash_insured_vehicles_damage'] = Transform \
                    .analysis_7_crashes_no_damage_insured_vehicles(df_collection['units_df'], df_collection['damages_df'])

    # Analysis 8
    transform_collection['top_5_vehicle_makers_df'] = Transform \
                    .analysis_8_top_5_vehicle_makers_cd(df_collection['primary_person_df'], df_collection['units_df'], df_collection['charges_df'])

    return transform_collection


def load_data(transform_df_collection, output_config):
    """Collect data locally and write to CSV.
    :param df: DataFrame to print.
    :return: None
    """
    def write_csv_df(df=None, path='', header=True, num_partitions=1):
        """Method to write spark dataframe as CSV file
            param df: spark dataframe which needs to written as file.
            param path: string for output path.
            param header: bool, optional
                writes first line as names of columns. It uses the default value, 'True'.
        """

        if path and df:
            df.coalesce(num_partitions).write.csv(path, mode='overwrite', header=header)
            return
        raise Exception('Please check file path and dataframe')

    write_csv_df(transform_df_collection['killed_male_df'], output_config['ANALYSIS_1'])
    write_csv_df(transform_df_collection['two_wheelers_crashes_df'], output_config['ANALYSIS_2'])
    write_csv_df(transform_df_collection['accident_female_df'], output_config['ANALYSIS_3'])
    write_csv_df(transform_df_collection['top_5_to_15_veh_make_id'], output_config['ANALYSIS_4'])
    write_csv_df(transform_df_collection['top_ethnic_user_grp'], output_config['ANALYSIS_5'])
    write_csv_df(transform_df_collection['top_5_crashed_car_alcohol'], output_config['ANALYSIS_6'])
    write_csv_df(transform_df_collection['distinct_crash_insured_vehicles_damage'], output_config['ANALYSIS_7'])
    write_csv_df(transform_df_collection['top_5_vehicle_makers_df'], output_config['ANALYSIS_8'])



# entry point for Job execution
if __name__ == '__main__':
    main()
# necessary imports
import sys
import json
import pyspark.sql.functions as F
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from utils_logger import create_cloudwatch_log_stream,create_cloudwatch_log
from utils_connection import get_secrets
from utils_transformation import transform_df, registrant_transform_df, flatten_df
from utils_misc import get_independent_object_data, get_webinar_dependent_object_data, get_session_dependent_object_data, get_dependent_object_data, get_column_mapping
from utils_loading import upload_json_file, load_data_to_redshift


# assigning variables for internal use
args_lst = ['bucket_name', 'secret_id', 'redshift_host', 'redshift_database', 'catalog_connection', 'cloudwatch_log_group', 'api_params']
args = getResolvedOptions(sys.argv, args_lst)

bucket_name = args['bucket_name']
secret_id = args['secret_id']
redshift_host = args['redshift_host']
redshift_database = args['redshift_database']
catalog_connection = args['catalog_connection']
cloudwatch_log_group = args['cloudwatch_log_group']

# assigning bu name and source name
bu_name = 'abc'
source_name = 'xyz'

# creating cloudwatch log stream
cloudwatch_log_stream = '{}_{}_{}'.format(bu_name, source_name, datetime.now()).replace('-', '_').replace(' ','_').replace(':', '').replace('.', '')
create_cloudwatch_log_stream(cloudwatch_log_group, cloudwatch_log_stream)

# creating spark session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# initial logging
msg = 'cloudwatch_info_log : {}_{} : job started at {}'.format(bu_name, source_name, str(datetime.now()))
print(msg)
create_cloudwatch_log(cloudwatch_log_group, cloudwatch_log_stream, msg)

# getting secrets
secrets = get_secrets(secret_id)

# generating access_token and organizer_key for each account
# api_params = get_authorization_params(secrets, bucket_name, bu_name, source_name, create_cloudwatch_log, cloudwatch_log_group, cloudwatch_log_stream)
api_params = json.loads(args['api_params'])
print(api_params)

# list of objects
object_names = ['webinars', 'organizer_sessions', 'registrants', 'registrant_fields', 'session_performance', 'session_polls', 'session_questions',
                'session_surveys', 'session_attendees', 'attendees', 'attendee_poll_answers', 'attendee_questions', 'attendee_survey_answers', 'registrant']

# API for data fetch 
base_url = 'https://URL NAME'

# performing ETL-1
for object_name in object_names:
    try:
        if object_name in ['webinars', 'organizer_sessions']:
            # get data for independent objects
            get_independent_object_data(object_name, api_params, base_url, bu_name, source_name, create_cloudwatch_log, cloudwatch_log_group, cloudwatch_log_stream)
        
        elif object_name in ['registrants', 'registrant_fields']:
            
            with open('/tmp/webinars.json', 'r', encoding="utf8") as file:
                webinar_json_file = json.load(file)
            # get data for webinar table dependent objects
            get_webinar_dependent_object_data(base_url, api_params, object_name, webinar_json_file, bu_name, source_name, create_cloudwatch_log, cloudwatch_log_group, cloudwatch_log_stream)

        elif object_name in ['session_performance','session_polls', 'session_questions', 'session_surveys', 'session_attendees']:
            
            with open('/tmp/organizer_sessions.json', 'r', encoding="utf8") as file:
                session_json_file = json.load(file)
            # get data for organizer session table dependent objects
            get_session_dependent_object_data(base_url, api_params, object_name, session_json_file, bu_name, source_name, create_cloudwatch_log, cloudwatch_log_group, cloudwatch_log_stream)
        
        else:
            # file to extract primary keys for attendee's related table
            with open('/tmp/session_attendees.json', 'r', encoding="utf8") as file:
                session_attendees_json_file = json.load(file)

            # file to extract primary keys for registrant table 
            with open('/tmp/registrants.json', 'r', encoding="utf8") as file:
                registrants_json_file = json.load(file)
            # get data for remaining dependent objects 
            get_dependent_object_data(base_url, object_name, api_params, registrants_json_file, session_attendees_json_file, bu_name, source_name, create_cloudwatch_log, cloudwatch_log_group, cloudwatch_log_stream)

    except Exception as e:
        msg = 'cloudwatch_error_log : {}_{} : Exception occured inside ETL-1 fetch_data logic for {} object as {}'.format(bu_name, source_name, object_name, e)
        print(msg)
        create_cloudwatch_log(cloudwatch_log_group, cloudwatch_log_stream, msg)


# performing ETL-2
for object_name in object_names:
    try:
        # uploading json file from Glue's temp location to S3
        upload_json_file(object_name, bucket_name, bu_name, source_name, create_cloudwatch_log, cloudwatch_log_group, cloudwatch_log_stream)
        
        # reading json data from S3
        df = spark.read.format("json").option("multiline", "true").load("s3://{}/{}/processed_data/{}/{}.json".format(bucket_name, bu_name, source_name, object_name))
        
        df_count = df.count()
        msg = 'cloudwatch_info_log : {}_{} : Dataframe record count for {} is {}'.format(bu_name, source_name, object_name, df_count)
        print(msg)
        create_cloudwatch_log(cloudwatch_log_group, cloudwatch_log_stream, msg)
        
        if df_count != 0:

            df = df.na.replace(["null"], [None])
            
            if object_name == 'registrant':
                # extracting data for registrant_responses table from registrant dataframe
                registrant_responses_df = df['registrantKey','responses']
                registrant_responses_df = registrant_transform_df(registrant_responses_df)
                registrant_responses_df = registrant_responses_df.na.replace(["null"], [None])
                
                # loading registrant_responses data in redshift
                load_data_to_redshift(registrant_responses_df, DynamicFrame, bu_name, source_name, 'registrant_responses', glueContext, redshift_database, bucket_name, catalog_connection, create_cloudwatch_log, cloudwatch_log_group, cloudwatch_log_stream)
            
            df = flatten_df(df)
            # adding missing columns with None values 
            total_columns = get_column_mapping(object_name)
            for column in total_columns:
                if column not in df.columns:
                    df=df.withColumn(column, F.lit(None).cast('string'))
            
            # transforming dataframe 
            df = transform_df(object_name, df)

            df = df.na.replace(["null"], [None])
            # loading object data into redshift
            load_data_to_redshift(df, DynamicFrame, bu_name, source_name, object_name, glueContext, redshift_database, bucket_name, catalog_connection, create_cloudwatch_log, cloudwatch_log_group, cloudwatch_log_stream)
    
    except Exception as e:
        msg = 'cloudwatch_error_log : {}_{} : Exception occured inside ETL-2 load_data logic for {} object as {}'.format(bu_name, source_name, object_name, e)
        print(msg)
        create_cloudwatch_log(cloudwatch_log_group, cloudwatch_log_stream, msg)

# final logging
msg = 'cloudwatch_info_log : {}_{} : job ended at {}'.format(bu_name, source_name, str(datetime.now()))
print(msg)
create_cloudwatch_log(cloudwatch_log_group, cloudwatch_log_stream, msg)

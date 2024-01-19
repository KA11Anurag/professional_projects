# necessary imports
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from datetime import datetime
from utils_logger import download_log_files, upload_log_files, get_loggers, create_cloudwatch_log_stream, create_cloudwatch_log
from utils_connection import get_secrets
from utils_misc import get_dependent_object_data, get_object_data, flatten_df
from utils_loading import upload_json_file, load_data_to_redshift


# assigning variables for internal use
args_lst = ['bucket_name', 'secret_id', 'redshift_host', 'redshift_database', 'catalog_connection']
args = getResolvedOptions(sys.argv, args_lst)

bucket_name = args['S3 Bucket']
secret_id = args['Secret Id']
redshift_host = args['DataBase Host Name']
redshift_database = args['redshift_database']
catalog_connection = args['catalog_connection']

# assigning bu name and source name
bu_name = 'abc'
source_name = 'xyz'

# create cloudwatch log stream
cloudwatch_log_group = 'CloudWatchLogGroup'
cloudwatch_log_stream = '{}_{}_{}'.format(bu_name, source_name, datetime.now()).replace('-', '_').replace(' ', '_').replace(':', '').replace('.', '')
create_cloudwatch_log_stream(cloudwatch_log_group, cloudwatch_log_stream)

# creating spark session
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

# downloading log files from s3 and assigning loggers
download_log_files(bu_name, source_name, bucket_name)
error_logger, info_logger = get_loggers(bu_name, source_name)

# initial logging
msg = 'cloudwatch_info_log : {}_{} : job started at {}'.format(bu_name, source_name, str(datetime.now()))
print(msg)
create_cloudwatch_log(cloudwatch_log_group, cloudwatch_log_stream, msg)
error_logger.debug("------------------ job started at : {} ------------------".format(str(datetime.now())))
info_logger.debug("------------------ job started at : {} ------------------".format(str(datetime.now())))

# getting secrets
secrets = get_secrets(secret_id)

# setting the request parameters to access Oktopost
username = secrets['UserName']
password = secrets['Password']

# list of objects
object_names = ['click', 'post', 'message', 'user']

# performing ETL-1
for object_name in object_names:
    try:
        
        print('Starting for {}'.format(object_name))
        
        # get dependent objects data(postlog -> post, analytics -> postlog, message -> campaign)
        if object_name in ['message', 'user']:
            get_dependent_object_data(object_name, username, password)
            continue
        
        # get simple object data
        get_object_data(object_name, username, password)
    
    except Exception as e:
        msg = 'cloudwatch_error_log : {}_{} : Exception occured inside ETL-1 logic for {} object as {}'.format(bu_name, source_name, object_name, e)
        print(msg)
        create_cloudwatch_log(cloudwatch_log_group, cloudwatch_log_stream, msg)
        error_logger.error(msg)

# performing ETL-2
for object_name in object_names:
    try:
        
        # uploading json file from Glue's temp location to S3
        upload_json_file(object_name, bucket_name, info_logger, error_logger, bu_name, source_name, create_cloudwatch_log, cloudwatch_log_group, cloudwatch_log_stream)
        
        # reading json data from S3
        df = spark.read.format("json").option("multiline", "true").load("s3://{}/{}/daily_job_data/{}_data/{}.json".format(bucket_name, bu_name, source_name, object_name))
        
        # get dataframe count
        df_count = df.count()
        
        msg = 'cloudwatch_info_log : {}_{} : Record count for {} is {}'.format(bu_name, source_name, object_name, df_count)
        print(msg)
        create_cloudwatch_log(cloudwatch_log_group, cloudwatch_log_stream, msg)
        error_logger.error(msg)
        
        if df_count != 0:
            df = df.na.replace(["null"], [None])
            if object_name in ['user']:
                df = flatten_df(df)
            df = df.na.replace(["null"], [None])
            
            msg = 'cloudwatch_info_log : {}_{} : Data Loading started for {}'.format(bu_name, source_name, object_name)
            print(msg)
            create_cloudwatch_log(cloudwatch_log_group, cloudwatch_log_stream, msg)
            error_logger.error(msg)
            
            load_data_to_redshift(df, DynamicFrame, bu_name, source_name, object_name, glueContext, error_logger, info_logger, redshift_database, bucket_name, catalog_connection, create_cloudwatch_log, cloudwatch_log_group, cloudwatch_log_stream)        
    
    except Exception as e:
        msg = 'cloudwatch_error_log : {}_{} : Exception occured inside ETL-2 logic for {} object as {}'.format(bu_name, source_name, object_name, e)
        print(msg)
        create_cloudwatch_log(cloudwatch_log_group, cloudwatch_log_stream, msg)
        error_logger.error(msg)


# final logging
msg = 'cloudwatch_info_log : {}_{} : job ended at {}'.format(bu_name, source_name, str(datetime.now()))
print(msg)
create_cloudwatch_log(cloudwatch_log_group, cloudwatch_log_stream, msg)
error_logger.debug("------------------ job ended at : {} ------------------".format(str(datetime.now())))
info_logger.debug("------------------ job ended at : {} ------------------".format(str(datetime.now())))

# uploading log files to s3
upload_log_files(bu_name, source_name, bucket_name)
import json
import boto3
from os import environ
from aws_lambda_powertools.utilities import parameters
import pymysql
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all


patch_all()



def create_proxy_connection_token():
    print("Getting Token")
    # get the required parameters to create a token
    username = environ.get('username')
    region = environ.get('region')  # get the region
    hostname = environ.get('rds_endpoint')  # get the rds proxy endpoint
    port = environ.get('port')  # get the database port

    # generate the authentication token -- temporary password
    token = client.generate_db_auth_token(
        DBHostname=hostname,
        Port=port,
        DBUsername=username,
        Region=region
    )

    return token


def db_ops():
    print("Starting RDS Proxy Connection")
    
    username = environ.get('username')
    password = create_proxy_connection_token()

    try:
        # create a connection object
        print("Making RDS Proxy Connection")
        connection = pymysql.connect(
            host=environ.get('rds_endpoint'),
            # getting the rds proxy endpoint from the environment variables
            user=username,
            password=password,
            db=environ.get('database'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            ssl={"use": True}
        )
    except pymysql.MySQLError as e:
        print(e)
        return e
    print("Completed RDS Proxy Connection")
    return connection

client = boto3.client('rds')  # get the rds object
conn = db_ops()
cursor = conn.cursor()

def lambda_handler(event, context):
    
    query = "select curdate() from dual"
    cursor.execute(query)
    results = cursor.fetchmany(1)

    return {
        'statusCode': 200,
        'body': json.dumps(results, default=str)
    }

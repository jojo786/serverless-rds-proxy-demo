import json
import boto3
from os import environ
from aws_lambda_powertools.utilities import parameters
import pymysql
from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all


patch_all()


def get_secret():
    print("Getting Secret")
    secret = parameters.get_secret(environ.get('secret_arn'), transform='json', max_age=60)
    username = secret.get('username')
    password = secret.get('password')

    return username, password











def db_ops():
    print("Starting RDS Connection")

    username, password = get_secret()

    try:
        # create a connection object
        print("Making RDS Connection")
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
        return e

    print("Completed RDS Connection")
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

import boto3
import pandas as pd
import psycopg2
import json

import configparser
config = configparser.ConfigParser()
config.read_file(open('/Users/ghost/Documents/secret/cluster.config'))


KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')

DWH_CLUSTER_TYPE = config.get('DWH', 'DWH_CLUSTER_TYPE')
DWH_NUM_NODES = config.get('DWH', 'DWH_NUM_NODES')
DWH_NODE_TYPE = config.get('DWH', 'DWH_NODE_TYPE')
DWH_CLUSTER_IDENTIFIER = config.get('DWH', 'DWH_CLUSTER_IDENTIFIER')
DWH_DB = config.get('DWH', 'DWH_DB')
DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
DWH_PORT = config.get('DWH', 'DWH_PORT')
DWH_IAM_ROLE_NAME = config.get('DWH', 'DWH_IAM_ROLE_NAME')


# pd.DataFrame({"Param":
#["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
# "Value":
#[DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME ]
# })

# creatae an object for EC2, s3, iam and  redshift
ec2 = boto3.resource('ec2', region_name='us-east-1',
                     aws_access_key_id=KEY, aws_secret_access_key=SECRET)

s3 = boto3.resource('s3', region_name='us-east-1',
                    aws_access_key_id=KEY, aws_secret_access_key=SECRET)

iam = boto3.client('iam', region_name='us-east-1',
                   aws_access_key_id=KEY, aws_secret_access_key=SECRET)

redshift = boto3.client('redshift', region_name='us-east-1',
                        aws_access_key_id=KEY, aws_secret_access_key=SECRET)

# display the amount of files in your s3 bucket.
bucket = s3.Bucket("covid-19-output-elijah")
log_data_file = [filename.key for filename in bucket.objects.filter(Prefix='')]
log_data_file

# dynamically call my arn role with this code
roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

try:
    response = redshift.create_cluster(
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        #identifier & credendials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,
        # Roles (for s3 access)
        IamRoles=[roleArn]
    )
except Exception as e:
    print(e)

redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)[
    'Clusters'][0]

# this code display specific key value of the redshift cluster created above


def prettyRedshift(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ['ClusterIdentifier', 'NodeType', 'ClusterStatus',
                  'MasterUsername', 'DBName', 'Endpoint', 'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


myClusterProps = redshift.describe_clusters(
    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
prettyRedshift(myClusterProps)


DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
DB_NAME = myClusterProps['DBName']
DB_USER = myClusterProps['MasterUsername']

# attach vpc to the Ec2 object we created above
try:
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)

    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
    )
except Exception as e:
    print(e)

# Connect to redshift database
try:
    conn = psycopg2.connect(host=DWH_ENDPOINT, dbname=DB_NAME,
                            user=DB_USER, password="Passw0rd123", port=5439)
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)

conn.set_session(autocommit=True)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error:Could not get cursor to the database")
    print(e)

# Create empty table on my redshift cluster
try:
    cur.execute("""CREATE TABLE "factCovid" (
    "index" INTEGER,
    "fips" REAL,
    "province_state" TEXT,
    "country_region" TEXT,
    "confirmed" REAL,
    "deaths" REAL,
    "recovered" REAL,
    "active" REAL,
    "date" INTEGER,
    "positive" INTEGER,
    "negative" REAL,
    "hospitalized" REAL,
    "hospitalizedcurrently" REAL,
    "hospitalizeddischarged" REAL)
    """)
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute("""CREATE TABLE "dimRegion" (
    "index" INTEGER,
    "fips" REAL,
    "province_state" TEXT,
    "country_region" TEXT,
    "latitude" REAL,
    "longitude" REAL,
    "county" TEXT,
    "state" TEXT)
    """)
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute("""CREATE TABLE "dimHospital" (
    "index" INTEGER,
    "fips" REAL,
    "state_name" TEXT,
    "latitude" REAL,
    "longtitude" REAL,
    "hq_address" TEXT,
    "hospital_name" TEXT,
    "hospital_type" TEXT,
    "hq_city" TEXT,
    "hq_state" TEXT)
    """)
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute("""CREATE TABLE "dimDate" (
    "index" INTEGER,
    "fips" REAL,
    "date" TIMESTAMP,
    "year" INTEGER,
    "month" INTEGER,
    "day_of_week" INTEGER)
    """)
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute(""" CREATE TABLE "us_states" (
    "date" TIMESTAMP,
    "state" TEXT,
    "fips" DOUBLE PRECISION,
    "cases" INTEGER,
    "deaths" INTEGER)
    """)
except psycopg2.Error as e:
    print("Error: Issue creating copying data into table")
    print(e)

try:
    cur.execute("""
    copy dimDate from 's3://covid-19-output-elijah/dimDate.csv'
    credentials 'aws_iam_role=arn:aws:iam::490101006133:role/redshift-s3-access'
    delimiter ','
    region 'us-east-1'

    """)
except psycopg2.Error as e:
    print("Error: Issue creating copying data into table")
    print(e)


try:
    cur.execute("""
    copy us_states from 's3://covid-19-output-elijah/us_states'
    credentials 'aws_iam_role=arn:aws:iam::490101006133:role/redshift-s3-access'
    delimiter ','
    region 'us-east-1'

    """)
except psycopg2.Error as e:
    print("Error: Issue creating copying data into table")
    print(e)


try:
    cur.execute("""
    copy dimHospital from 's3://covid-19-output-elijah/dimHospital'
    credentials 'aws_iam_role=arn:aws:iam::490101006133:role/redshift-s3-access'
    delimiter ','
    region 'us-east-1'

    """)
except psycopg2.Error as e:
    print("Error: Issue creating copying data into table")
    print(e)

try:
    cur.execute("""
    copy dimRegion from 's3://covid-19-output-elijah/dimRegion'
    credentials 'aws_iam_role=arn:aws:iam::490101006133:role/redshift-s3-access'
    delimiter ','
    region 'us-east-1'

    """)
except psycopg2.Error as e:
    print("Error: Issue creating copying data into table")
    print(e)


try:
    cur.execute("""
    copy factCovid from 's3://covid-19-output-elijah/factCovid'
    credentials 'aws_iam_role=arn:aws:iam::490101006133:role/redshift-s3-access'
    delimiter ','
    region 'us-east-1'

    """)
except psycopg2.Error as e:
    print("Error: Issue creating copying data into table")
    print(e)

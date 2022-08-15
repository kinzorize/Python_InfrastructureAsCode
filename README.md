# Python_InfrastructureAsCode

In this project, i built a complete ETL pipeline with python Infrastructure as Code(IaC) using S3, python, pandas, psycopg2, boto3 and redshift

# Tools you will need to execute this project are

- install pnadas
- install python 3.10
- install boto3
- install psycopg2
- And have a admin access of your aws account or role privilege
- you will need iam_role to create s3fullaccess and s3readonlyaccess to allow or copy data from s3 to redshift table

# Note

- You need to add 'aws_iam_role=' variable on your credentials for you to be able to successfully copy the data to your redshift table.
- I created and remove the cluster.config file on this repository because that is where my aws_secret and access key are stored for safety reasons.

1) Open S3, under already pre created s3 bucket glue-test-bucket-{aws account id} create these subfolders
    a) glue-errors
    b) glue-scripts
    c) glue-temp
    Make sure you have orders.csv under raw/orders/
2) Create s3 bucket named mwaa-bucket-{aws account id}. Create these subfolders
    a) dags
    b) requirements
    Add the psycopg2_binary-2.9.9-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl under requirements
    Under dags subdirectory, upload 
        1) dag_1_s3_glue_aurora.py
        2) dag_2_city_revenue_view.py
3) Go to vpc subnets, create a subnet
    name: mwaa-private-subnet-1
    availability zone: us-east-1a
    Add ipv4 subnet block cidr: 172.31.96.0/24

    create another
    name: mwaa-private-subnet-2
    availability zone: us-east-1b
    Add ipv4 subnet block cidr: 172.31.97.0/24

4) Create a security group
    name: mwaa_practice_sg
    create, post creatin add an inbound rule to it as type: all traffic, source: custom and enter mwaa_practice_sg

5) Modify iceberg_aurora_sg security group
    add an inbound rule as
    type: postgresql
    port: 5432
    source: mwaa_practice_sg

6) Create a route table
    name it mwaa-private-route-table
    Post creation, under subnet associations add mwaa-private-subnet-1 and mwaa-private-subnet-2

7) Create vpc endpoints
    1) name: mwaa-s3
       service: com.amazonaws.us-east-1.s3
       type: gateway
       route table: mwaa-private-route-table

    2) name: mwaa-cloudwatch
        service: com.amazonaws.us-east-1.s3
        Endpoint type: Interface
        VPC: Default VPC
        Subnets:
        mwaa-private-subnet-1
        mwaa-private-subnet-2
        Security group: mwaa_practice_sg
        Private DNS: Enabled
        Endpoint policy: Full access initially
    3) name: mwaa-monitoring
        service: com.amazonaws.us-east-1.monitoring
        Endpoint type: Interface
        VPC: Default VPC
        Subnets:
        mwaa-private-subnet-1
        mwaa-private-subnet-2
        Security group: mwaa_practice_sg
        Private DNS: Enabled
        Endpoint policy: Full access initially
    4) name: mwaa-sqs
        service: com.amazonaws.us-east-1.sqs
        Endpoint type: Interface
        VPC: Default VPC
        Subnets:
        mwaa-private-subnet-1
        mwaa-private-subnet-2
        Security group: mwaa_practice_sg
        Private DNS: Enabled
        Endpoint policy: Full access initially
    5) name: mwaa-kms
        service: com.amazonaws.us-east-1.kms
        Endpoint type: Interface
        VPC: Default VPC
        Subnets:
        mwaa-private-subnet-1
        mwaa-private-subnet-2
        Security group: mwaa_practice_sg
        Private DNS: Enabled
        Endpoint policy: Full access initially
    6) name: mwaa-glue
        service: com.amazonaws.us-east-1.glue
        Endpoint type: Interface
        VPC: Default VPC
        Subnets:
        mwaa-private-subnet-1
        mwaa-private-subnet-2
        Security group: mwaa_practice_sg
        Private DNS: Enabled
        Endpoint policy: Full access initially
8) Create glue connection to aurora if not present.
9) Create glue etl script, practice-s3-orders-to-aurora add the glue connection aurora,
copy the script mwaa_glue_etl.py
10) Create iam role
    trusted entity type: custom trust policy
    trust policy
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowMWAAAssumeRole",
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "airflow.amazonaws.com",
          "airflow-env.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    }
  ]
}

add this as permissions inline policy
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "PublishMWAAMetrics",
			"Effect": "Allow",
			"Action": "airflow:PublishMetrics",
			"Resource": "arn:aws:airflow:us-east-1:316422224399:environment/practice-mwaa"
		},
		{
			"Sid": "ReadMWAASourceBucket",
			"Effect": "Allow",
			"Action": [
				"s3:GetObject",
				"s3:GetObjectVersion",
				"s3:GetBucketLocation",
				"s3:GetBucketVersioning",
				"s3:ListBucket",
				"s3:GetBucketPublicAccessBlock"
			],
			"Resource": [
				"arn:aws:s3:::mwaa-bucket-316422224399",
				"arn:aws:s3:::mwaa-bucket-316422224399/*"
			]
		},
		{
			"Sid": "ReadAccountPublicAccessBlock",
			"Effect": "Allow",
			"Action": "s3:GetAccountPublicAccessBlock",
			"Resource": "*"
		},
		{
			"Sid": "WriteMWAACloudWatchLogs",
			"Effect": "Allow",
			"Action": [
				"logs:CreateLogGroup",
				"logs:CreateLogStream",
				"logs:PutLogEvents",
				"logs:GetLogEvents",
				"logs:GetLogRecord",
				"logs:GetLogGroupFields",
				"logs:GetQueryResults",
				"logs:DescribeLogStreams"
			],
			"Resource": "arn:aws:logs:us-east-1:316422224399:log-group:airflow-practice-mwaa-*"
		},
		{
			"Sid": "DescribeCloudWatchLogGroups",
			"Effect": "Allow",
			"Action": "logs:DescribeLogGroups",
			"Resource": "*"
		},
		{
			"Sid": "PublishCloudWatchMetrics",
			"Effect": "Allow",
			"Action": "cloudwatch:PutMetricData",
			"Resource": "*"
		},
		{
			"Sid": "UseMWAASQSQueues",
			"Effect": "Allow",
			"Action": [
				"sqs:ChangeMessageVisibility",
				"sqs:DeleteMessage",
				"sqs:GetQueueAttributes",
				"sqs:GetQueueUrl",
				"sqs:ReceiveMessage",
				"sqs:SendMessage"
			],
			"Resource": "arn:aws:sqs:us-east-1:*:airflow-celery-*"
		},
		{
			"Sid": "UseAWSOwnedKMSKeysForMWAASQS",
			"Effect": "Allow",
			"Action": [
				"kms:Decrypt",
				"kms:DescribeKey",
				"kms:GenerateDataKey*",
				"kms:Encrypt"
			],
			"NotResource": "arn:aws:kms:*:316422224399:key/*",
			"Condition": {
				"StringLike": {
					"kms:ViaService": "sqs.us-east-1.amazonaws.com"
				}
			}
		},
		{
			"Sid": "RunAndMonitorPracticeGlueJob",
			"Effect": "Allow",
			"Action": [
				"glue:GetJob",
				"glue:GetJobRun",
				"glue:GetJobRuns",
				"glue:StartJobRun",
				"glue:BatchStopJobRun"
			],
			"Resource": "arn:aws:glue:us-east-1:316422224399:job/practice-s3-orders-to-aurora"
		},
		{
			"Sid": "ReadGlueJobLogs",
			"Effect": "Allow",
			"Action": "logs:FilterLogEvents",
			"Resource": [
				"arn:aws:logs:us-east-1:316422224399:log-group:/aws-glue/jobs/output:*",
				"arn:aws:logs:us-east-1:316422224399:log-group:/aws-glue/jobs/error:*"
			]
		}
	]
}
Name it as MWAAGlueJobAccess

11) Go to MWAA, Create Environment
    | Setting           | Value                 |
| ----------------- | --------------------- |
| Name              | `practice-mwaa`       |
| Airflow version   | `3.2.1`               |
| S3 bucket         | Dedicated MWAA bucket |
| DAG folder        | `dags`                |
| Requirements file | Leave blank initially |
| Plugins           | Leave blank           |
| Startup script    | Leave blank           |
VPC: Default VPC
Subnet 1: mwaa-private-subnet-1
Subnet 2: mwaa-private-subnet-2
Webserver access: Both public and private network access
Security group: mwaa_practice_sg
| Setting           | Selection                |
| ----------------- | ------------------------ |
| Environment class | mw1.micro |
| Minimum workers   | 1                        |
| Maximum workers   | 2                        |
| Schedulers        | Minimum/default          |
| Webservers        | Minimum/default          |
For logging
Enable at INFO level:

DAG processing
Scheduler
Task
Worker
Webserver

12) Once airflow gets spinned up
    Go to connections under admin
    connection id: aurora_practice
    Connection type: postgres
    host: writer endpoint of the aurora postgres
    login: postgres
    password: postgres
    port 5432
    database practice

13) unpause both the dags from airflow ui and run dag 1
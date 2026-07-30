# AWS AURORA INFRA CREATION
1) Open AWS CONSOLE and go to aurora and rds.
2) Create database with full configuration
3) Set Engine to Aurora (PostgreSQL Compatible)
4) Set template to Dev/Test
5) Choose Provisioned as cluster scalibility type and set it to Burstable classes
    choose db.t3.medium
6) Set engine version to 17.7
7) Name the db cluster identifier as practice
8) Under credentials settings
    1) Choose self managed
    2) Master username: postgres
    3) Master password: <come up with a secured password> or can keep postgres
9) Set cluster storage configuration to Aurora Standard
10) For availability and durability set it to Dont create an replica
11) Set network type to IPv4
12) Set vpc and db subnet to default
13) Set public access to yes
14) Create a new vpc security group and name it as iceberg_aurora_sg
15) Under additional configuration, in initial database name set it to practice
16) For maintenance, set auto minor version upgrade to off.
17) Create Database

Open Security groups (VPC Feature)
Modifying iceberg_aurora_sg
1) Go to security groups, find the one we created
2) Set an inbound rule, go to edit inbound rules and add a new rule
3) Set type to custom tcp and source as anywhere-ipv4, it should display 0.0.0.0/0 and keep
port range 0-65535
4) Hit save rules

Connecting it via PgAdmin4
1) Open pgadmin, register server, name it as enq_tred_v2
2) Copy the endpoint of the writer instance as host and add it in connection details.
3) password: set to postgres

# Post AURORA CREATION
1) Open pgadmin, run the query scripts\orders_init.sql
2) Run above query, to see the distribution of the data.
SELECT
    city,
    EXTRACT(MONTH FROM updated_at) AS month,
    COUNT(*) AS record_count
FROM orders
GROUP BY
    city,
    EXTRACT(MONTH FROM updated_at)
order by 1, 2


# S3 BUCKET CREATION
1) Will reuse the same bucket, glue-test-bucket-{your aws account id} as landing bucket target.
2) Create a new s3 bucket for emr assets, name the bucket emr-demo-bucket-{your aws account id}.
Create 3 subfolders under this newly created bucket,
    a) drivers
    b) logs
    c) scripts
3) Upload the jar file present under assets of this repo to the drivers directory above.
4) upload the script emr_aurora_orders_to_parquet.py from this repo to the bucket s3://emr-demo-bucket-{your aws account id}/scripts.


# EMR Creation
1) Create a security group, name it as emr_serverless_sg.
2) Add the description of the security group as "SG required to be used by EMR Cluster for demo purposes".
3) Make sure the vpc set is the default vpc. Keep everything else default and create.
4) Open the security group iceberg_aurora_sg which was earlier created while creation of aurora rds.
Add an inbound rule to it by clicking edit inbound rules.
    Type: PostgreSQL
    Source: Custom, in the right blank window search emr_serverless_sg and add it.
    Hit Save rules.
5) Go to vpc endpoints, create a endpoint
    Name: s3-gateway-endpoint
    Services: com.amazonaws.us-east-1.s3(Type: Gateway)
    Vpc: Click the default vpc available
    Route tables: Default route table (wont have a name, and under main it will be Yes)
    Policy: Full access
6) Go to iam, create the role which will be used by EMR
    Trusted Entity: Custom Trust Policy
    Under custom trust policy paste, change aws account id
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EMRServerlessTrustPolicy",
      "Effect": "Allow",
      "Principal": {
        "Service": "emr-serverless.amazonaws.com"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "aws:SourceAccount": "{your aws account id}"
        }
      }
    }
  ]
}

7) In add permissions, add create inline policy
{
	"Version": "2012-10-17",
	"Statement": [
		{
			"Sid": "FullAccessToTrainingBucket",
			"Effect": "Allow",
			"Action": "s3:*",
			"Resource": [
				"arn:aws:s3:::glue-test-bucket-{your aws acccount id}",
				"arn:aws:s3:::glue-test-bucket-{your aws acccount id}/*"
			]
		},
		{
  "Sid": "ListEMRJobAssets",
  "Effect": "Allow",
  "Action": "s3:ListBucket",
  "Resource": "arn:aws:s3:::emr-demo-bucket-{your aws acccount id}",
  "Condition": {
    "StringLike": {
      "s3:prefix": [
        "scripts/*",
        "drivers/*"
      ]
    }
  }
},
{
  "Sid": "ReadEMRJobAssets",
  "Effect": "Allow",
  "Action": "s3:GetObject",
  "Resource": [
    "arn:aws:s3:::emr-demo-bucket-{your aws acccount id}/scripts/*",
    "arn:aws:s3:::emr-demo-bucket-{your aws acccount id}/drivers/*"
  ]
}
	]
}

Add the role name as: EMRServerlessOrdersRuntimeRole
Inline policy name as: EMRServerlessTrainingBucketAccess

8) Open EMR Serverless, Go to manage Manage applications in EMR Studio. Click on Create and launch EMR Studio.
9) Create Application, 
    Name: practice-emr-serverless
    Type: Spark
    Release: EMR Spark 8.0
    Enable serverless storage: switched off
    Architecture: x86_64
    Application Setup options: Use Custom Settings
    Enable pre-initialized capacity: switched off
    Application limits, maximum cpu: 8, maximum memory: 32, maximum disk: 100 gb
    Under application behaviour
      Automatically start application on job submission: checked on
      Automatically stop after application is idle for: checked on with 15 minutes
    VPC: Set the default vpc
    Subnets: select us-east-1a and us-east-1b
    Security group: emr_serverless_sg
    Create the application.
10) Post application creation, open the application and go to submit job run
    Name the job: aurora_to_s3
    Runtime role: EMRServerlessOrdersRuntimeRole
    Script location: s3://emr-demo-bucket-{your aws account id}/scripts/emr_aurora_orders_to_parquet.py, you can browse it also.
    Under Spark properties
        Row 1
            Key: spark.jars
            Value: s3://emr-demo-bucket-512357470856/drivers/postgresql-42.7.13.jar
11) Run crawler to crawl the full data, keep the source as curated/orders_kolkata
12) Check in athena to see if the data has landed or not
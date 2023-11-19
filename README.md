# STEDI Human Balance Analytics


## Project Introduction
In this project a data lakehouse solution has to be built for the STEDI team. Data that is generated from the STEDI step trainer sensors and the moble application has to be extracted and curated in to data lake house solution on AWS so that data scientists can use the solution to train the machine learning model.


## Project Steps

### Setting up the AWS environment

AWS Glue and S3 access setup

![image](https://github.com/LavanyaBoopalan/HumanBalanceAnalytics/assets/24756600/93473334-1857-4988-9884-a65b9597caaa)


Using AWS CLI execute the following steps:

Step 1: Creating an AWS S3 Bucket 

```
aws s3 mb s3://stedihb-lake-house
```
    
Step 2: Identify the VPC that needs access to S3:

```
aws ec2 describe-vpcs
```

Step 3: Identify the routing table to configure with VPC Gateway.

```
aws ec2 describe-route-tables
```

Step 4: Create an S3 Gateway Endpoint

``` 
aws ec2 create-vpc-endpoint --vpc-id vpc-07b7111158de6d9fd --service-name com.amazonaws.us-east-1.s3 --route-table-ids rtb-05dc4999ffbc6f757
```

Step 5: create a IAM Service role 

```
aws iam create-role --role-name my-glue-s3-service-role --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}'
```

Step 6: Grant S3 access to IAM Service role 

```
aws iam put-role-policy --role-name my-glue-s3-service-role --policy-name S3Access --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListObjectsInBucket",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::stedihb-lake-house"
            ]
        },
        {
            "Sid": "AllObjectActions",
            "Effect": "Allow",
            "Action": "s3:*Object",
            "Resource": [
                "arn:aws:s3:::stedihb-lake-house/*"
            ]
        }
    ]
}'
```


Step 7: Grant Glue access to the S3 buckets     

```
aws iam put-role-policy --role-name my-glue-s3-service-role --policy-name GlueAccess --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:*",
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListAllMyBuckets",
                "s3:GetBucketAcl",
                "ec2:DescribeVpcEndpoints",
                "ec2:DescribeRouteTables",
                "ec2:CreateNetworkInterface",
                "ec2:DeleteNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DescribeSecurityGroups",
                "ec2:DescribeSubnets",
                "ec2:DescribeVpcAttribute",
                "iam:ListRolePolicies",
                "iam:GetRole",
                "iam:GetRolePolicy",
                "cloudwatch:PutMetricData"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:CreateBucket",
                "s3:PutBucketPublicAccessBlock"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*/*",
                "arn:aws:s3:::*/*aws-glue-*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::crawler-public*",
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:AssociateKmsKey"
            ],
            "Resource": [
                "arn:aws:logs:*:*:/aws-glue/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": [
                        "aws-glue-service-resource"
                    ]
                }
            },
            "Resource": [
                "arn:aws:ec2:*:*:network-interface/*",
                "arn:aws:ec2:*:*:security-group/*",
                "arn:aws:ec2:*:*:instance/*"
            ]
        }
    ]
}'
```

### Building Data Lakehouse Solution

![image](https://github.com/LavanyaBoopalan/HumanBalanceAnalytics/assets/24756600/fb0fa325-71b5-45c6-b4b7-7dd79b5f2a8c)



### Build Data Landing Zone

1) From AWS CLI, Extract the data from the source path to the following AWS S3 bucket :
Bucket: s3://stedihb-lake-house
 
   **customer/landing** <br>
   **steptrainer/landing** <br>
   **accelerometer/landing** <br>
 
2) Manually create a Glue Table for each of the above landing data using Glue Console from JSON data

   **customer_landing.sql**<br>
   **accelerometer_landing.sql**<br>
   **step_trainer_landing.sql**<br>

3) Following Landing zone tables are created :

   **customer_landing**<br>
   **accelerometer_landing**<br>
   **steptrainer_landing**<br>

3) Use AWS Athena to query and verify the source data in the Landing zone.


### Build Data Trusted Zone

1) Following Glue Jobs are created to extract the data from the landing zone , filter and create the trusted zone.

   **customer_landing_to_trusted.py**
        - Drops rows that do not have data in the sharedWithResearchAsOfDate column.
     
   **accelerometer_landing_to_trusted.py**
        - inner joins the customer_trusted data with the accelerometer_landing data by emails.
     
   **step_trainer_landing_to_trusted.py** (Job created after the customer curated data)
        - inner joins the step_trainer_landing data with the customer_curated data by serial numbers.
  

2) Configure the jobs to Create a table in the Data Catalog and, on subsequent runs, update the schema and add new partitions.


3) Following Trusted zone tables are created in the Data Catalog as part of the Glue Job  :

   **customer_trusted**
   **accelerometer_trusted**
   **steptrainer_trusted**

4) Use AWS Athena to query and verify the data in the Trusted zone tables.


### Build Data Curated Zone

1) Following Glue Jobs are created to extract the data from the trusted zone, add transformations and joins as per the analytics requirements and create the curated zone.

   **customer_trusted_to_curated.py** 
       - inner joins the customer_trusted data with the accelerometer_trusted data by emails.
   **step_trainer_trusted.py**
       - inner joins the step_trainer_landing data with the customer_curated data by serial numbers.
   **machine_learning_curated.py** 
       - inner joins the step_trainer_trusted data with the accelerometer_trusted data by sensor reading time and timestamps.
    
2) PII data such as customername, email and phone are anonymized so that it is not subject to GDPR.


2) Configure the jobs to Create a table in the Data Catalog and, on subsequent runs, update the schema and add new partitions.


3) Following Curated zone tables are created in the Data Catalog as part of the Glue Job  :

   **customer_curated**
   **machinelearning_curated**

4) Use AWS Athena to query and verify the data in the Curated zone tables.

# PropTech360ReviewerReadOnly:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListBucketsInConsole",
      "Effect": "Allow",
      "Action": "s3:ListAllMyBuckets",
      "Resource": "*"
    },
    {
      "Sid": "RegionalConsoleDiscovery",
      "Effect": "Allow",
      "Action": [
        "lambda:ListFunctions",
        "athena:ListWorkGroups",
        "athena:ListDataCatalogs",
        "logs:DescribeLogGroups"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "aws:RequestedRegion": "eu-north-1"
        }
      }
    },
    {
      "Sid": "ViewCloudWatchAlarms",
      "Effect": "Allow",
      "Action": [
        "cloudwatch:DescribeAlarms",
        "cloudwatch:DescribeAlarmsForMetric",
        "cloudwatch:DescribeAlarmHistory"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "aws:RequestedRegion": "eu-north-1"
        }
      }
    },
    {
      "Sid": "ViewProjectBucket",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:GetBucketVersioning",
        "s3:GetEncryptionConfiguration",
        "s3:GetBucketPublicAccessBlock",
        "s3:GetBucketNotification",
        "s3:ListBucket",
        "s3:ListBucketVersions"
      ],
      "Resource": "arn:aws:s3:::proptech360-bucket-800557027629"
    },
    {
      "Sid": "ReadProjectFiles",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": [
        "arn:aws:s3:::proptech360-bucket-800557027629/raw/*",
        "arn:aws:s3:::proptech360-bucket-800557027629/curated/*",
        "arn:aws:s3:::proptech360-bucket-800557027629/audit/*",
        "arn:aws:s3:::proptech360-bucket-800557027629/athena-result/*"
      ]
    },
    {
      "Sid": "ViewProjectLambda",
      "Effect": "Allow",
      "Action": [
        "lambda:GetFunction",
        "lambda:GetFunctionConfiguration",
        "lambda:GetFunctionConcurrency",
        "lambda:GetPolicy",
        "lambda:ListTags",
        "lambda:ListAliases",
        "lambda:ListVersionsByFunction",
        "lambda:ListFunctionEventInvokeConfigs",
        "lambda:ListFunctionUrlConfigs"
      ],
      "Resource": [
        "arn:aws:lambda:eu-north-1:800557027629:function:lambda_handler_starter",
        "arn:aws:lambda:eu-north-1:800557027629:function:lambda_handler_starter:*"
      ]
    },
    {
      "Sid": "ReadProjectGlueCatalog",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabases",
        "glue:GetDatabase",
        "glue:GetTables",
        "glue:GetTable",
        "glue:GetTableVersion",
        "glue:GetTableVersions",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:BatchGetPartition",
        "glue:SearchTables"
      ],
      "Resource": [
        "arn:aws:glue:eu-north-1:800557027629:catalog",
        "arn:aws:glue:eu-north-1:800557027629:database/proptech360_capstone_db",
        "arn:aws:glue:eu-north-1:800557027629:table/proptech360_capstone_db/*"
      ]
    },
    {
      "Sid": "ReadAthenaCatalogMetadata",
      "Effect": "Allow",
      "Action": [
        "athena:GetDataCatalog",
        "athena:GetDatabase",
        "athena:GetTableMetadata",
        "athena:ListDatabases",
        "athena:ListTableMetadata"
      ],
      "Resource": "arn:aws:athena:eu-north-1:800557027629:datacatalog/AwsDataCatalog"
    },
    {
      "Sid": "ReadExistingAthenaResults",
      "Effect": "Allow",
      "Action": [
        "athena:GetWorkGroup",
        "athena:ListQueryExecutions",
        "athena:GetQueryExecution",
        "athena:BatchGetQueryExecution",
        "athena:GetQueryResults",
        "athena:GetQueryRuntimeStatistics",
        "athena:ListNamedQueries",
        "athena:GetNamedQuery",
        "athena:BatchGetNamedQuery"
      ],
      "Resource": "arn:aws:athena:eu-north-1:800557027629:workgroup/primary"
    },
    {
      "Sid": "ViewLambdaLogStreams",
      "Effect": "Allow",
      "Action": [
        "logs:DescribeLogStreams",
        "logs:FilterLogEvents"
      ],
      "Resource": "arn:aws:logs:eu-north-1:800557027629:log-group:/aws/lambda/lambda_handler_starter:*"
    },
    {
      "Sid": "ReadLambdaLogEvents",
      "Effect": "Allow",
      "Action": "logs:GetLogEvents",
      "Resource": "arn:aws:logs:eu-north-1:800557027629:log-group:/aws/lambda/lambda_handler_starter:log-stream:*"
    }
  ]
}
```
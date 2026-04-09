# AwsUtil (C#)

A comprehensive utility library for common AWS services - C# port of the [aws-util](https://github.com/Masrik-Dahir/aws-util-python) Python package.

## Overview

AwsUtil provides high-level, opinionated wrappers around the AWS SDK for .NET, covering 100+ AWS services with:

- **Cached client factory** with TTL-based eviction (15-minute default)
- **Structured exception hierarchy** mapping AWS error codes to semantic exception types
- **Placeholder resolution** for SSM Parameter Store and Secrets Manager references
- **Multi-service orchestration** patterns (deployer, data pipelines, security ops, etc.)
- **All async** - every operation is `async Task<T>` by default

## Installation

```bash
dotnet add package AwsUtil
```

Or add to your `.csproj`:

```xml
<PackageReference Include="AwsUtil" Version="2.2.6" />
```

## Quick Start

```csharp
using AwsUtil;
using AwsUtil.Services;

// Placeholder resolution (SSM + Secrets Manager)
var dbHost = (string)Placeholder.Retrieve("${ssm:/myapp/db/host}")!;
var dbPass = (string)Placeholder.Retrieve("${secret:myapp/db-credentials:password}")!;

// S3 operations
await S3Service.UploadFileAsync("my-bucket", "data/file.json", "/tmp/file.json");
var bytes = await S3Service.DownloadBytesAsync("my-bucket", "data/file.json");

// SQS operations
await SqsService.SendMessageAsync("https://sqs.us-east-1.amazonaws.com/123/my-queue", "hello");
var messages = await SqsService.ReceiveMessagesAsync("https://sqs.us-east-1.amazonaws.com/123/my-queue");

// DynamoDB operations
await DynamoDbService.PutItemAsync("my-table", item);
var result = await DynamoDbService.GetItemAsync("my-table", key);

// Secrets Manager
var secret = await SecretsManagerService.GetSecretAsync("myapp/db-credentials:password");

// Parameter Store
var param = await ParameterStoreService.GetParameterAsync("/myapp/config/db-host", withDecryption: true);
```

## Multi-Service Orchestration

```csharp
// Config loading from SSM + Secrets Manager
var config = await ConfigLoader.LoadAppConfigAsync("/myapp/prod/", secretName: "myapp/secrets");

// Notifications across SNS, SES, SQS
var result = await NotifierService.BroadcastAsync(
    "Alert", "Something happened",
    snsTopicArns: new() { "arn:aws:sns:us-east-1:123:alerts" });

// Exception-aware notifications
await NotifierService.NotifyOnExceptionAsync(
    async () => await SomeRiskyOperation(),
    "arn:aws:sns:us-east-1:123:errors");
```

## Exception Hierarchy

All AWS errors are mapped to semantic exception types:

| Exception | AWS Error Codes |
|-----------|----------------|
| `AwsThrottlingException` | Throttling, TooManyRequestsException, ... |
| `AwsNotFoundException` | ResourceNotFoundException, NoSuchKey, ... |
| `AwsPermissionException` | AccessDenied, UnauthorizedOperation, ... |
| `AwsConflictException` | ConflictException, AlreadyExistsException, ... |
| `AwsValidationException` | ValidationException, InvalidParameterValue, ... |
| `AwsTimeoutException` | Operation timeout |
| `AwsServiceException` | Catch-all for other errors |

All inherit from `AwsUtilException`, which inherits from `Exception`.

## Service Coverage

### Core Services
S3, SQS, DynamoDB, Lambda, SNS, SES (v1 & v2), Parameter Store, Secrets Manager, KMS, STS, IAM, EC2

### Compute & Containers
ECS, ECR, EKS, Lambda, Batch, App Runner, Elastic Beanstalk, Lightsail, EMR, EMR Containers, EMR Serverless

### Database & Storage
RDS, DynamoDB, ElastiCache, Neptune, Neptune Graph, Keyspaces, MemoryDB, DocumentDB, Redshift, Redshift Data, Redshift Serverless, EFS, FSx, Storage Gateway, Transfer, Timestream Write/Query, RDS Data

### Networking & CDN
Route 53, CloudFront, ELBv2, VPC Lattice, Auto Scaling

### AI/ML
Bedrock, Bedrock Agent, Bedrock Agent Runtime, SageMaker Runtime, SageMaker Feature Store Runtime, Rekognition, Textract, Comprehend, Translate, Polly, Transcribe, Personalize, Personalize Runtime, Forecast, Forecast Query, Lex Models, Lex Runtime

### Analytics
Athena, Glue, Kinesis, Kinesis Firehose, Kinesis Analytics, MSK, QuickSight, DataBrew

### Security & Compliance
Security Hub, Inspector, Detective, Macie, Access Analyzer, SSO Admin, Cognito, Cognito Identity

### Management & Governance
CloudWatch, CloudTrail, CloudFormation, EventBridge, Step Functions, Organizations, Service Quotas, Config Service, Health

### Developer Tools
CodeBuild, CodeCommit, CodeDeploy, CodePipeline, CodeArtifact, CodeStar Connections

### IoT
IoT Core, IoT Data, IoT Greengrass, IoT SiteWise

### Media & Communication
MediaConvert, IVS, Connect

### Multi-Service Orchestration
Deployer, Data Pipeline, Resource Ops, Security Ops, Lambda Middleware, API Gateway, Event Orchestration, Data Flow ETL, Resilience, Observability, Deployment, Security Compliance, Cost Optimization, Testing & Dev, Config State, Messaging, AI/ML Pipelines, Infra Automation, Cross-Account, Blue/Green, Data Lake, Event Patterns, Container Ops, Cost Governance, Credential Rotation, Database Migration, Disaster Recovery, ML Pipeline, Networking, Security Automation

## Project Structure

```
AwsUtil/
├── AwsUtil.sln
├── src/
│   └── AwsUtil/
│       ├── AwsUtil.csproj
│       ├── ClientFactory.cs           # Cached AWS client factory
│       ├── Placeholder.cs             # SSM/Secrets placeholder resolution
│       ├── ConfigLoader.cs            # App config from SSM + Secrets
│       ├── Exceptions/
│       │   ├── AwsUtilException.cs    # Exception hierarchy
│       │   └── ErrorClassifier.cs     # AWS error code classifier
│       └── Services/
│           ├── S3Service.cs
│           ├── SqsService.cs
│           ├── DynamoDbService.cs
│           ├── LambdaService.cs
│           ├── ... (100+ service files)
│           └── NotifierService.cs
└── tests/
    └── AwsUtil.Tests/
        ├── AwsUtil.Tests.csproj
        ├── ExceptionsTests.cs
        ├── ClientFactoryTests.cs
        └── PlaceholderTests.cs
```

## Requirements

- .NET 10.0+
- AWS credentials configured (environment variables, AWS config file, or IAM role)

## Author

Masrik Dahir - [masrikdahir.com](https://www.masrikdahir.com)

## License

MIT

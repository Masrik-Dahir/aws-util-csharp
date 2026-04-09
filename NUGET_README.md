# AwsUtil

A comprehensive C# utility library wrapping **133 AWS services** with cached clients, structured exceptions, placeholder resolution, and multi-service orchestration — the .NET port of [aws-util](https://github.com/Masrik-Dahir/aws-util-python).

[![CI](https://github.com/Masrik-Dahir/aws-util-csharp/actions/workflows/ci.yml/badge.svg)](https://github.com/Masrik-Dahir/aws-util-csharp/actions/workflows/ci.yml)
[![NuGet Version](https://img.shields.io/nuget/v/AwsUtil)](https://www.nuget.org/packages/AwsUtil)
[![NuGet Downloads](https://img.shields.io/nuget/dt/AwsUtil)](https://www.nuget.org/packages/AwsUtil)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![.NET](https://img.shields.io/badge/.NET-10.0-512BD4)](https://dotnet.microsoft.com/)

---

## Features

- **133 AWS Service Wrappers** — High-level methods for S3, DynamoDB, Lambda, SQS, Bedrock, ECS, RDS, and 126 more
- **Cached Client Factory** — LRU cache with TTL-based eviction (15-min default, max 64 clients) so credential rotations are picked up automatically
- **Dual API Surface** — Every operation has both `async Task<T>` and synchronous overloads
- **Structured Exception Hierarchy** — AWS error codes classified into 6 semantic exception types plus a catch-all
- **Placeholder Resolution** — Inline `${ssm:/path}` and `${secret:name:key}` references resolved from SSM Parameter Store and Secrets Manager
- **Config Loader** — Batch load app config from SSM parameter paths with optional Secrets Manager overlay
- **30+ Multi-Service Orchestrations** — Pre-built patterns for blue/green deploys, data pipelines, security ops, disaster recovery, and more

---

## Quick Start

### Install

```bash
dotnet add package AwsUtil
```

### Use a service

```csharp
using AwsUtil;
using AwsUtil.Services;

// S3 operations
await S3Service.UploadFileAsync("my-bucket", "data/file.json", "/tmp/file.json");
var bytes = await S3Service.DownloadBytesAsync("my-bucket", "data/file.json");
var url   = S3Service.GeneratePresignedUrl("my-bucket", "data/file.json", expiresIn: 3600);

// SQS operations
await SqsService.SendMessageAsync(
    "https://sqs.us-east-1.amazonaws.com/123/my-queue", "hello");
var messages = await SqsService.ReceiveMessagesAsync(
    "https://sqs.us-east-1.amazonaws.com/123/my-queue");

// DynamoDB operations
await DynamoDbService.PutItemAsync("my-table", item);
var result = await DynamoDbService.GetItemAsync("my-table", key);

// Placeholder resolution (SSM + Secrets Manager)
var dbHost = (string)Placeholder.Retrieve("${ssm:/myapp/db/host}")!;
var dbPass = (string)Placeholder.Retrieve("${secret:myapp/db-credentials:password}")!;
```

### Multi-service orchestration

```csharp
// Batch config loading from SSM + Secrets Manager
var config = await ConfigLoader.LoadAppConfigAsync(
    "/myapp/prod/", secretName: "myapp/secrets");
var dbUrl = config.Get("database-url");

// DB credentials from Secrets Manager
var creds = await ConfigLoader.GetDbCredentialsAsync("myapp/db-creds");
```

---

## Exception Handling

All AWS errors are mapped to semantic exception types via `ErrorClassifier`:

| Exception | Example AWS Error Codes |
|-----------|------------------------|
| `AwsThrottlingException` | `Throttling`, `TooManyRequestsException`, `SlowDown` |
| `AwsNotFoundException` | `ResourceNotFoundException`, `NoSuchKey`, `NoSuchBucket` |
| `AwsPermissionException` | `AccessDenied`, `UnauthorizedOperation`, `ExpiredToken` |
| `AwsConflictException` | `ConflictException`, `AlreadyExistsException` |
| `AwsValidationException` | `ValidationException`, `InvalidParameterValue` |
| `AwsTimeoutException` | Operation timeout |
| `AwsServiceException` | Catch-all for unclassified errors |

```csharp
try
{
    await S3Service.DownloadBytesAsync("my-bucket", "missing-key");
}
catch (AwsNotFoundException ex)
{
    Console.WriteLine($"Not found: {ex.ErrorCode}");  // "NoSuchKey"
}
catch (AwsThrottlingException)
{
    // Back off and retry
}
catch (AwsUtilException ex)
{
    Console.WriteLine($"{ex.GetType().Name}: {ex.Message} [{ex.ErrorCode}]");
}
```

---

## Placeholder Resolution

Resolve AWS references embedded in configuration strings:

```csharp
// SSM Parameter Store
var host = (string)Placeholder.Retrieve("${ssm:/myapp/db/host}")!;

// Secrets Manager (full secret)
var secret = (string)Placeholder.Retrieve("${secret:myapp/api-key}")!;

// Secrets Manager (JSON key extraction)
var password = (string)Placeholder.Retrieve("${secret:myapp/db-creds:password}")!;

// Async version
var value = await Placeholder.RetrieveAsync("${ssm:/myapp/config}");

// Clear caches
Placeholder.ClearAllCaches();
```

---

## Service Coverage

**Core:** S3, SQS, DynamoDB, Lambda, SNS, SES (v1 & v2), Parameter Store, Secrets Manager, KMS, STS, IAM, EC2

**Compute & Containers:** ECS, ECR, EKS, Lambda, Batch, App Runner, Elastic Beanstalk, Lightsail, EMR, EMR Containers, EMR Serverless

**Database & Storage:** RDS, DynamoDB, ElastiCache, Neptune, Neptune Graph, Keyspaces, MemoryDB, DocumentDB, Redshift, Redshift Data, Redshift Serverless, EFS, FSx, Storage Gateway, Transfer, Timestream Write/Query, RDS Data

**Networking & CDN:** Route 53, CloudFront, ELBv2, VPC Lattice, Auto Scaling

**AI/ML:** Bedrock, Bedrock Agent, Bedrock Agent Runtime, SageMaker Runtime, SageMaker Feature Store, Rekognition, Textract, Comprehend, Translate, Polly, Transcribe, Personalize, Forecast, Lex

**Analytics:** Athena, Glue, Kinesis, Kinesis Firehose, Kinesis Analytics, MSK, QuickSight, DataBrew

**Security & Compliance:** Security Hub, Inspector, Detective, Macie, Access Analyzer, SSO Admin, Cognito

**Management & Governance:** CloudWatch, CloudTrail, CloudFormation, EventBridge, Step Functions, Organizations, Service Quotas, Config Service, Health

**Developer Tools:** CodeBuild, CodeCommit, CodeDeploy, CodePipeline, CodeArtifact, CodeStar Connections

**IoT:** IoT Core, IoT Data, IoT Greengrass, IoT SiteWise

**Media & Communication:** MediaConvert, IVS, Connect

**Multi-Service Orchestration (30+ patterns):** Deployer, Data Pipeline, Security Ops, Blue/Green, Disaster Recovery, Cost Governance, Credential Rotation, Container Ops, ML Pipeline, and more

---

## Architecture

- **All service classes are static** with static methods — no dependency injection needed
- **Client acquisition** via `ClientFactory.GetClient<T>(region?)` with LRU caching (TTL 15 min, max 64 clients)
- **Error handling** through `ErrorClassifier` which maps AWS error codes to typed exceptions
- **Dual API** with both async and synchronous overloads for every operation

---

## Requirements

- .NET 10.0 or later
- AWS credentials configured via environment variables, shared credentials file, or IAM role

---

## Links

- [GitHub Repository](https://github.com/Masrik-Dahir/aws-util-csharp)
- [Changelog](https://github.com/Masrik-Dahir/aws-util-csharp/blob/master/CHANGELOG.md)
- [License (MIT)](https://github.com/Masrik-Dahir/aws-util-csharp/blob/master/LICENSE)

---

Made with care by [Masrik Dahir](https://www.masrikdahir.com)

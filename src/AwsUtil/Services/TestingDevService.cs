using System.Text;
using System.Text.Json;
using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>A generated test event for a Lambda function.</summary>
public sealed record LambdaTestEvent(
    string EventType,
    string EventSource,
    Dictionary<string, object?> Payload,
    string JsonPayload);

/// <summary>Result of seeding a DynamoDB table with test data.</summary>
public sealed record LocalDynamoDbSeederResult(
    string TableName,
    int ItemsSeeded,
    int ItemsFailed,
    List<string> Errors);

/// <summary>Result of a single integration test run.</summary>
public sealed record IntegrationTestResult(
    string TestName,
    bool Passed,
    double DurationMs,
    string? Error = null,
    Dictionary<string, object?>? Metadata = null);

/// <summary>Aggregated integration test harness result.</summary>
public sealed record IntegrationTestHarnessResult(
    List<IntegrationTestResult> Results,
    int TotalTests,
    int PassedTests,
    int FailedTests,
    double TotalDurationMs);

/// <summary>A mock event source record for testing.</summary>
public sealed record MockEventSourceResult(
    string Source,
    string DetailType,
    Dictionary<string, object?> Detail,
    string JsonBody);

/// <summary>Result of recording Lambda invocations.</summary>
public sealed record LambdaInvokeRecording(
    string FunctionName,
    string InvokedAt,
    int StatusCode,
    object? RequestPayload,
    object? ResponsePayload,
    string? FunctionError);

/// <summary>Result of recording multiple Lambda invocations.</summary>
public sealed record LambdaInvokeRecorderResult(
    List<LambdaInvokeRecording> Recordings,
    int TotalInvocations,
    int SuccessfulInvocations,
    int FailedInvocations);

/// <summary>Result of snapshot testing against stored baseline.</summary>
public sealed record SnapshotTesterResult(
    string SnapshotKey,
    bool Matched,
    string? Diff = null,
    string? BaselineS3Key = null,
    string? ActualS3Key = null);

/// <summary>
/// Developer and testing utilities that orchestrate Lambda, DynamoDB, S3,
/// and EventBridge to support test workflows.
/// </summary>
public static class TestingDevService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    /// <summary>
    /// Generate a realistic test event payload for common Lambda event sources.
    /// </summary>
    public static async Task<LambdaTestEvent> LambdaEventGeneratorAsync(
        string eventType,
        Dictionary<string, object?>? overrides = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var payload = eventType.ToLowerInvariant() switch
            {
                "apigateway" or "api-gateway" => new Dictionary<string, object?>
                {
                    ["httpMethod"] = "GET",
                    ["path"] = "/test",
                    ["headers"] = new Dictionary<string, string>
                    {
                        ["Content-Type"] = "application/json"
                    },
                    ["queryStringParameters"] = new Dictionary<string, string>(),
                    ["body"] = null,
                    ["isBase64Encoded"] = false,
                    ["requestContext"] = new Dictionary<string, object?>
                    {
                        ["accountId"] = "123456789012",
                        ["stage"] = "test"
                    }
                },
                "s3" => new Dictionary<string, object?>
                {
                    ["Records"] = new List<Dictionary<string, object?>>
                    {
                        new()
                        {
                            ["eventSource"] = "aws:s3",
                            ["eventName"] = "ObjectCreated:Put",
                            ["s3"] = new Dictionary<string, object?>
                            {
                                ["bucket"] = new Dictionary<string, string>
                                    { ["name"] = "test-bucket" },
                                ["object"] = new Dictionary<string, object?>
                                    { ["key"] = "test-key", ["size"] = 1024 }
                            }
                        }
                    }
                },
                "sqs" => new Dictionary<string, object?>
                {
                    ["Records"] = new List<Dictionary<string, object?>>
                    {
                        new()
                        {
                            ["messageId"] = Guid.NewGuid().ToString(),
                            ["receiptHandle"] = "test-handle",
                            ["body"] = "{\"test\": true}",
                            ["eventSource"] = "aws:sqs",
                            ["eventSourceARN"] = "arn:aws:sqs:us-east-1:123456789012:test-queue"
                        }
                    }
                },
                "dynamodb" => new Dictionary<string, object?>
                {
                    ["Records"] = new List<Dictionary<string, object?>>
                    {
                        new()
                        {
                            ["eventID"] = Guid.NewGuid().ToString(),
                            ["eventName"] = "INSERT",
                            ["eventSource"] = "aws:dynamodb",
                            ["dynamodb"] = new Dictionary<string, object?>
                            {
                                ["Keys"] = new Dictionary<string, object?>
                                {
                                    ["id"] = new Dictionary<string, string> { ["S"] = "test-id" }
                                },
                                ["NewImage"] = new Dictionary<string, object?>
                                {
                                    ["id"] = new Dictionary<string, string> { ["S"] = "test-id" },
                                    ["data"] = new Dictionary<string, string> { ["S"] = "test-data" }
                                }
                            }
                        }
                    }
                },
                "eventbridge" or "cloudwatch-events" => new Dictionary<string, object?>
                {
                    ["version"] = "0",
                    ["id"] = Guid.NewGuid().ToString(),
                    ["detail-type"] = "Test Event",
                    ["source"] = "aws.test",
                    ["account"] = "123456789012",
                    ["time"] = DateTime.UtcNow.ToString("o"),
                    ["region"] = region?.SystemName ?? "us-east-1",
                    ["detail"] = new Dictionary<string, object?> { ["test"] = true }
                },
                "sns" => new Dictionary<string, object?>
                {
                    ["Records"] = new List<Dictionary<string, object?>>
                    {
                        new()
                        {
                            ["EventSource"] = "aws:sns",
                            ["Sns"] = new Dictionary<string, object?>
                            {
                                ["Type"] = "Notification",
                                ["MessageId"] = Guid.NewGuid().ToString(),
                                ["TopicArn"] = "arn:aws:sns:us-east-1:123456789012:test-topic",
                                ["Subject"] = "Test",
                                ["Message"] = "{\"test\": true}",
                                ["Timestamp"] = DateTime.UtcNow.ToString("o")
                            }
                        }
                    }
                },
                _ => new Dictionary<string, object?>
                {
                    ["source"] = eventType,
                    ["data"] = new Dictionary<string, object?> { ["test"] = true }
                }
            };

            // Apply overrides
            if (overrides != null)
            {
                foreach (var (key, value) in overrides)
                    payload[key] = value;
            }

            var json = JsonSerializer.Serialize(payload, JsonOptions);

            return await Task.FromResult(new LambdaTestEvent(
                EventType: eventType,
                EventSource: $"aws:{eventType.ToLowerInvariant()}",
                Payload: payload,
                JsonPayload: json));
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to generate Lambda test event");
        }
    }

    /// <summary>
    /// Seed a DynamoDB table with test data items.
    /// </summary>
    public static async Task<LocalDynamoDbSeederResult> LocalDynamoDbSeederAsync(
        string tableName,
        List<Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>> items,
        RegionEndpoint? region = null)
    {
        try
        {
            var seeded = 0;
            var failed = 0;
            var errors = new List<string>();

            foreach (var item in items)
            {
                try
                {
                    await DynamoDbService.PutItemAsync(tableName, item, region: region);
                    seeded++;
                }
                catch (Exception ex)
                {
                    failed++;
                    errors.Add($"Item {seeded + failed}: {ex.Message}");
                }
            }

            return new LocalDynamoDbSeederResult(
                TableName: tableName,
                ItemsSeeded: seeded,
                ItemsFailed: failed,
                Errors: errors);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to seed DynamoDB table");
        }
    }

    /// <summary>
    /// Run a set of integration test functions, invoking Lambda functions and
    /// capturing results with timing information.
    /// </summary>
    public static async Task<IntegrationTestHarnessResult> IntegrationTestHarnessAsync(
        List<(string TestName, string FunctionName, string Payload)> tests,
        RegionEndpoint? region = null)
    {
        try
        {
            var results = new List<IntegrationTestResult>();

            foreach (var (testName, functionName, payload) in tests)
            {
                var sw = System.Diagnostics.Stopwatch.StartNew();
                try
                {
                    var invokeResult = await LambdaService.InvokeAsync(
                        functionName,
                        payload: payload,
                        region: region);

                    sw.Stop();
                    results.Add(new IntegrationTestResult(
                        TestName: testName,
                        Passed: invokeResult.Succeeded,
                        DurationMs: sw.Elapsed.TotalMilliseconds,
                        Error: invokeResult.FunctionError,
                        Metadata: new Dictionary<string, object?>
                        {
                            ["statusCode"] = invokeResult.StatusCode,
                            ["functionName"] = functionName
                        }));
                }
                catch (Exception ex)
                {
                    sw.Stop();
                    results.Add(new IntegrationTestResult(
                        TestName: testName,
                        Passed: false,
                        DurationMs: sw.Elapsed.TotalMilliseconds,
                        Error: ex.Message));
                }
            }

            return new IntegrationTestHarnessResult(
                Results: results,
                TotalTests: results.Count,
                PassedTests: results.Count(r => r.Passed),
                FailedTests: results.Count(r => !r.Passed),
                TotalDurationMs: results.Sum(r => r.DurationMs));
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Integration test harness failed");
        }
    }

    /// <summary>
    /// Generate mock EventBridge events for testing event-driven architectures.
    /// </summary>
    public static async Task<List<MockEventSourceResult>> MockEventSourceAsync(
        string source,
        string detailType,
        List<Dictionary<string, object?>> details,
        string? eventBusName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var results = new List<MockEventSourceResult>();

            foreach (var detail in details)
            {
                var json = JsonSerializer.Serialize(detail, JsonOptions);

                if (eventBusName != null)
                {
                    await EventBridgeService.PutEventsAsync(
                        entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                        {
                            new()
                            {
                                Source = source,
                                DetailType = detailType,
                                Detail = json,
                                EventBusName = eventBusName
                            }
                        },
                        region: region);
                }

                results.Add(new MockEventSourceResult(
                    Source: source,
                    DetailType: detailType,
                    Detail: detail,
                    JsonBody: json));
            }

            return results;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to generate mock events");
        }
    }

    /// <summary>
    /// Invoke a Lambda function multiple times with different payloads and record
    /// all request/response pairs for analysis.
    /// </summary>
    public static async Task<LambdaInvokeRecorderResult> LambdaInvokeRecorderAsync(
        string functionName,
        List<string> payloads,
        RegionEndpoint? region = null)
    {
        try
        {
            var recordings = new List<LambdaInvokeRecording>();

            foreach (var payload in payloads)
            {
                var invokedAt = DateTime.UtcNow.ToString("o");
                try
                {
                    var result = await LambdaService.InvokeAsync(
                        functionName,
                        payload: payload,
                        region: region);

                    recordings.Add(new LambdaInvokeRecording(
                        FunctionName: functionName,
                        InvokedAt: invokedAt,
                        StatusCode: result.StatusCode,
                        RequestPayload: payload,
                        ResponsePayload: result.Payload,
                        FunctionError: result.FunctionError));
                }
                catch (Exception ex)
                {
                    recordings.Add(new LambdaInvokeRecording(
                        FunctionName: functionName,
                        InvokedAt: invokedAt,
                        StatusCode: 0,
                        RequestPayload: payload,
                        ResponsePayload: null,
                        FunctionError: ex.Message));
                }
            }

            return new LambdaInvokeRecorderResult(
                Recordings: recordings,
                TotalInvocations: recordings.Count,
                SuccessfulInvocations: recordings.Count(r => r.FunctionError == null),
                FailedInvocations: recordings.Count(r => r.FunctionError != null));
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to record Lambda invocations");
        }
    }

    /// <summary>
    /// Compare a Lambda function's output against a stored baseline snapshot in S3,
    /// reporting any differences.
    /// </summary>
    public static async Task<SnapshotTesterResult> SnapshotTesterAsync(
        string functionName,
        string payload,
        string snapshotBucket,
        string snapshotKey,
        RegionEndpoint? region = null)
    {
        try
        {
            // Invoke the function
            var result = await LambdaService.InvokeAsync(
                functionName,
                payload: payload,
                region: region);

            var actual = JsonSerializer.Serialize(result.Payload, JsonOptions);

            // Try to fetch the baseline snapshot from S3
            string? baseline = null;
            var baselineKey = $"snapshots/{snapshotKey}/baseline.json";
            var actualKey = $"snapshots/{snapshotKey}/actual.json";

            try
            {
                var baselineObj = await S3Service.GetObjectAsync(snapshotBucket, baselineKey, region: region);
                baseline = Encoding.UTF8.GetString(baselineObj.Body);
            }
            catch (AwsNotFoundException)
            {
                // No baseline exists yet; store current as baseline
                await S3Service.PutObjectAsync(
                    snapshotBucket, baselineKey, Encoding.UTF8.GetBytes(actual), region: region);
                return new SnapshotTesterResult(
                    SnapshotKey: snapshotKey,
                    Matched: true,
                    Diff: null,
                    BaselineS3Key: baselineKey,
                    ActualS3Key: null);
            }

            // Store actual result
            await S3Service.PutObjectAsync(
                snapshotBucket, actualKey, Encoding.UTF8.GetBytes(actual), region: region);

            // Compare
            var matched = string.Equals(
                baseline?.Trim(), actual.Trim(), StringComparison.Ordinal);

            return new SnapshotTesterResult(
                SnapshotKey: snapshotKey,
                Matched: matched,
                Diff: matched ? null : $"Baseline and actual differ. See {actualKey}",
                BaselineS3Key: baselineKey,
                ActualS3Key: actualKey);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Snapshot test failed");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="LambdaEventGeneratorAsync"/>.</summary>
    public static LambdaTestEvent LambdaEventGenerator(string eventType, Dictionary<string, object?>? overrides = null, RegionEndpoint? region = null)
        => LambdaEventGeneratorAsync(eventType, overrides, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="LocalDynamoDbSeederAsync"/>.</summary>
    public static LocalDynamoDbSeederResult LocalDynamoDbSeeder(string tableName, List<Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>> items, RegionEndpoint? region = null)
        => LocalDynamoDbSeederAsync(tableName, items, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="IntegrationTestHarnessAsync"/>.</summary>
    public static IntegrationTestHarnessResult IntegrationTestHarness(List<(string TestName, string FunctionName, string Payload)> tests, RegionEndpoint? region = null)
        => IntegrationTestHarnessAsync(tests, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="MockEventSourceAsync"/>.</summary>
    public static List<MockEventSourceResult> MockEventSource(string source, string detailType, List<Dictionary<string, object?>> details, string? eventBusName = null, RegionEndpoint? region = null)
        => MockEventSourceAsync(source, detailType, details, eventBusName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="LambdaInvokeRecorderAsync"/>.</summary>
    public static LambdaInvokeRecorderResult LambdaInvokeRecorder(string functionName, List<string> payloads, RegionEndpoint? region = null)
        => LambdaInvokeRecorderAsync(functionName, payloads, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SnapshotTesterAsync"/>.</summary>
    public static SnapshotTesterResult SnapshotTester(string functionName, string payload, string snapshotBucket, string snapshotKey, RegionEndpoint? region = null)
        => SnapshotTesterAsync(functionName, payload, snapshotBucket, snapshotKey, region).GetAwaiter().GetResult();

}

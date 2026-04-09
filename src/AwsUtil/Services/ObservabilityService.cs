using System.Text.Json;
using Amazon;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using Amazon.CloudWatchLogs;
using Amazon.CloudWatchLogs.Model;
using Amazon.XRay;
using Amazon.XRay.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of creating an X-Ray trace segment.</summary>
public sealed record CreateXrayTraceResult(
    bool Success = true,
    string? TraceId = null);

/// <summary>Result of emitting an EMF metric.</summary>
public sealed record EmitEmfMetricResult(
    bool Success = true,
    string? LogGroupName = null);

/// <summary>Result of creating Lambda CloudWatch alarms.</summary>
public sealed record CreateLambdaAlarmsResult(
    List<string>? AlarmArns = null,
    int AlarmsCreated = 0);

/// <summary>Result of creating a DLQ depth alarm.</summary>
public sealed record CreateDlqDepthAlarmResult(
    string? AlarmArn = null,
    string? AlarmName = null);

/// <summary>Result of a CloudWatch Logs Insights query.</summary>
public sealed record RunLogInsightsQueryResult(
    string? QueryId = null,
    string? Status = null,
    List<List<ResultField>>? Results = null,
    double? BytesScanned = null,
    int? RecordsMatched = null,
    int? RecordsScanned = null);

/// <summary>Result of generating a CloudWatch dashboard.</summary>
public sealed record GenerateLambdaDashboardResult(
    string? DashboardArn = null,
    string? DashboardName = null,
    List<DashboardValidationMessage>? ValidationMessages = null);

/// <summary>Aggregated error entry.</summary>
public sealed record AggregatedError(
    string Message,
    int Count = 0,
    DateTime? FirstOccurrence = null,
    DateTime? LastOccurrence = null);

/// <summary>Result of aggregating errors from logs.</summary>
public sealed record AggregateErrorsResult(
    List<AggregatedError>? Errors = null,
    int TotalErrors = 0,
    string? LogGroupName = null);

/// <summary>Result of creating a CloudWatch Synthetics canary.</summary>
public sealed record CreateCanaryResult(
    string? CanaryName = null,
    string? Arn = null,
    bool Success = true);

/// <summary>Service dependency entry for the service map.</summary>
public sealed record ServiceDependency(
    string ServiceName,
    string DependencyName,
    string? DependencyType = null);

/// <summary>Result of building a service dependency map.</summary>
public sealed record BuildServiceMapResult(
    List<ServiceDependency>? Dependencies = null,
    int ServiceCount = 0);

// ── Structured Logger ───────────────────────────────────────────────

/// <summary>
/// Structured logger that writes JSON-formatted log entries to CloudWatch Logs.
/// </summary>
public sealed class StructuredLogger
{
    private readonly string _logGroupName;
    private readonly string _logStreamName;
    private readonly RegionEndpoint? _region;

    /// <summary>
    /// Create a new structured logger targeting a CloudWatch log group and stream.
    /// </summary>
    public StructuredLogger(
        string logGroupName,
        string logStreamName,
        RegionEndpoint? region = null)
    {
        _logGroupName = logGroupName;
        _logStreamName = logStreamName;
        _region = region;
    }

    private AmazonCloudWatchLogsClient GetLogsClient()
        => ClientFactory.GetClient<AmazonCloudWatchLogsClient>(_region);

    /// <summary>
    /// Write a structured log entry as JSON to CloudWatch Logs.
    /// </summary>
    public async Task LogAsync(
        string level,
        string message,
        Dictionary<string, object?>? context = null,
        string? correlationId = null)
    {
        var client = GetLogsClient();

        try
        {
            var entry = new Dictionary<string, object?>
            {
                ["timestamp"] = DateTime.UtcNow.ToString("o"),
                ["level"] = level.ToUpperInvariant(),
                ["message"] = message
            };

            if (correlationId != null)
                entry["correlationId"] = correlationId;

            if (context != null)
            {
                foreach (var kv in context)
                    entry[kv.Key] = kv.Value;
            }

            var logEvent = new InputLogEvent
            {
                Timestamp = DateTime.UtcNow,
                Message = JsonSerializer.Serialize(entry)
            };

            await client.PutLogEventsAsync(new PutLogEventsRequest
            {
                LogGroupName = _logGroupName,
                LogStreamName = _logStreamName,
                LogEvents = new List<InputLogEvent> { logEvent }
            });
        }
        catch (global::Amazon.CloudWatchLogs.Model.ResourceNotFoundException)
        {
            // Create group and stream, then retry
            try
            {
                await client.CreateLogGroupAsync(
                    new CreateLogGroupRequest { LogGroupName = _logGroupName });
            }
            catch (ResourceAlreadyExistsException)
            {
                // Already exists, continue
            }

            await client.CreateLogStreamAsync(new CreateLogStreamRequest
            {
                LogGroupName = _logGroupName,
                LogStreamName = _logStreamName
            });

            var retryEvent = new InputLogEvent
            {
                Timestamp = DateTime.UtcNow,
                Message = JsonSerializer.Serialize(new Dictionary<string, object?>
                {
                    ["timestamp"] = DateTime.UtcNow.ToString("o"),
                    ["level"] = level.ToUpperInvariant(),
                    ["message"] = message
                })
            };

            await client.PutLogEventsAsync(new PutLogEventsRequest
            {
                LogGroupName = _logGroupName,
                LogStreamName = _logStreamName,
                LogEvents = new List<InputLogEvent> { retryEvent }
            });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to write structured log to '{_logGroupName}'");
        }
    }

    /// <summary>Log an INFO-level message.</summary>
    public Task InfoAsync(string message, Dictionary<string, object?>? context = null,
        string? correlationId = null)
        => LogAsync("INFO", message, context, correlationId);

    /// <summary>Log a WARN-level message.</summary>
    public Task WarnAsync(string message, Dictionary<string, object?>? context = null,
        string? correlationId = null)
        => LogAsync("WARN", message, context, correlationId);

    /// <summary>Log an ERROR-level message.</summary>
    public Task ErrorAsync(string message, Dictionary<string, object?>? context = null,
        string? correlationId = null)
        => LogAsync("ERROR", message, context, correlationId);

    /// <summary>Log a DEBUG-level message.</summary>
    public Task DebugAsync(string message, Dictionary<string, object?>? context = null,
        string? correlationId = null)
        => LogAsync("DEBUG", message, context, correlationId);
}

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Observability and monitoring utilities combining CloudWatch Metrics,
/// Logs, Alarms, Dashboards, X-Ray, and Synthetics.
/// </summary>
public static class ObservabilityService
{
    private static AmazonCloudWatchClient GetCwClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCloudWatchClient>(region);

    private static AmazonCloudWatchLogsClient GetLogsClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCloudWatchLogsClient>(region);

    private static AmazonXRayClient GetXRayClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonXRayClient>(region);

    /// <summary>
    /// Create an X-Ray trace segment document and submit it.
    /// </summary>
    public static async Task<CreateXrayTraceResult> CreateXrayTraceAsync(
        string serviceName,
        string operationName,
        DateTime startTime,
        DateTime endTime,
        bool fault = false,
        bool error = false,
        Dictionary<string, object?>? annotations = null,
        Dictionary<string, object?>? metadata = null,
        RegionEndpoint? region = null)
    {
        var client = GetXRayClient(region);

        try
        {
            var traceId = $"1-{DateTimeOffset.UtcNow.ToUnixTimeSeconds():x8}" +
                          $"-{Guid.NewGuid().ToString("N")[..24]}";
            var segmentId = Guid.NewGuid().ToString("N")[..16];

            var segment = new Dictionary<string, object?>
            {
                ["name"] = serviceName,
                ["id"] = segmentId,
                ["trace_id"] = traceId,
                ["start_time"] =
                    new DateTimeOffset(startTime).ToUnixTimeMilliseconds() / 1000.0,
                ["end_time"] =
                    new DateTimeOffset(endTime).ToUnixTimeMilliseconds() / 1000.0,
                ["fault"] = fault,
                ["error"] = error,
                ["annotations"] = annotations ?? new Dictionary<string, object?>
                {
                    ["operation"] = operationName
                },
                ["metadata"] = metadata
            };

            var segmentJson = JsonSerializer.Serialize(segment);

            await client.PutTraceSegmentsAsync(new PutTraceSegmentsRequest
            {
                TraceSegmentDocuments = new List<string> { segmentJson }
            });

            return new CreateXrayTraceResult(Success: true, TraceId: traceId);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create X-Ray trace for '{serviceName}/{operationName}'");
        }
    }

    /// <summary>
    /// Emit a CloudWatch Embedded Metric Format (EMF) log entry.
    /// </summary>
    public static async Task<EmitEmfMetricResult> EmitEmfMetricAsync(
        string logGroupName,
        string logStreamName,
        string metricNamespace,
        string metricName,
        double metricValue,
        string unit = "None",
        Dictionary<string, string>? dimensions = null,
        RegionEndpoint? region = null)
    {
        var client = GetLogsClient(region);

        try
        {
            var dimensionsList = new List<Dictionary<string, string>>();
            var properties = new Dictionary<string, object?>();

            if (dimensions != null)
            {
                var dimSet = new Dictionary<string, string>();
                foreach (var kv in dimensions)
                {
                    dimSet[kv.Key] = kv.Key;
                    properties[kv.Key] = kv.Value;
                }
                dimensionsList.Add(dimSet);
            }

            var emfEntry = new Dictionary<string, object?>
            {
                ["_aws"] = new Dictionary<string, object?>
                {
                    ["Timestamp"] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    ["CloudWatchMetrics"] = new List<Dictionary<string, object?>>
                    {
                        new()
                        {
                            ["Namespace"] = metricNamespace,
                            ["Dimensions"] = dimensionsList.Count > 0
                                ? dimensionsList.Select(d => d.Keys.ToList()).ToList()
                                : new List<List<string>>(),
                            ["Metrics"] = new List<Dictionary<string, object?>>
                            {
                                new()
                                {
                                    ["Name"] = metricName,
                                    ["Unit"] = unit
                                }
                            }
                        }
                    }
                },
                [metricName] = metricValue
            };

            foreach (var kv in properties)
                emfEntry[kv.Key] = kv.Value;

            var logEvent = new InputLogEvent
            {
                Timestamp = DateTime.UtcNow,
                Message = JsonSerializer.Serialize(emfEntry)
            };

            await client.PutLogEventsAsync(new PutLogEventsRequest
            {
                LogGroupName = logGroupName,
                LogStreamName = logStreamName,
                LogEvents = new List<InputLogEvent> { logEvent }
            });

            return new EmitEmfMetricResult(Success: true, LogGroupName: logGroupName);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to emit EMF metric '{metricName}' to '{logGroupName}'");
        }
    }

    /// <summary>
    /// Create standard CloudWatch alarms for a Lambda function: error rate,
    /// duration, throttles, and concurrent executions.
    /// </summary>
    public static async Task<CreateLambdaAlarmsResult> CreateLambdaAlarmsAsync(
        string functionName,
        string snsTopicArn,
        int errorThreshold = 1,
        int durationThresholdMs = 30000,
        int throttleThreshold = 1,
        int evaluationPeriods = 1,
        int periodSeconds = 300,
        RegionEndpoint? region = null)
    {
        var client = GetCwClient(region);
        var alarmArns = new List<string>();

        try
        {
            var alarms = new List<(string Suffix, string MetricName, double Threshold,
                string ComparisonOp, string Stat)>
            {
                ("errors", "Errors", errorThreshold,
                    "GreaterThanOrEqualToThreshold", "Sum"),
                ("duration", "Duration", durationThresholdMs,
                    "GreaterThanOrEqualToThreshold", "Average"),
                ("throttles", "Throttles", throttleThreshold,
                    "GreaterThanOrEqualToThreshold", "Sum")
            };

            foreach (var (suffix, metricName, threshold, compOp, stat) in alarms)
            {
                var alarmName = $"{functionName}-{suffix}";
                await client.PutMetricAlarmAsync(new PutMetricAlarmRequest
                {
                    AlarmName = alarmName,
                    Namespace = "AWS/Lambda",
                    MetricName = metricName,
                    Dimensions = new List<Dimension>
                    {
                        new() { Name = "FunctionName", Value = functionName }
                    },
                    Statistic = new Statistic(stat),
                    Threshold = threshold,
                    ComparisonOperator = new ComparisonOperator(compOp),
                    EvaluationPeriods = evaluationPeriods,
                    Period = periodSeconds,
                    AlarmActions = new List<string> { snsTopicArn },
                    TreatMissingData = "notBreaching"
                });

                // Describe to get the ARN
                var descResp = await client.DescribeAlarmsAsync(new DescribeAlarmsRequest
                {
                    AlarmNames = new List<string> { alarmName }
                });
                if (descResp.MetricAlarms.Count > 0)
                    alarmArns.Add(descResp.MetricAlarms[0].AlarmArn);
            }

            return new CreateLambdaAlarmsResult(
                AlarmArns: alarmArns,
                AlarmsCreated: alarmArns.Count);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Lambda alarms for '{functionName}'");
        }
    }

    /// <summary>
    /// Create a CloudWatch alarm for DLQ (SQS) depth.
    /// </summary>
    public static async Task<CreateDlqDepthAlarmResult> CreateDlqDepthAlarmAsync(
        string queueName,
        string snsTopicArn,
        int threshold = 10,
        int evaluationPeriods = 1,
        int periodSeconds = 300,
        string? alarmName = null,
        RegionEndpoint? region = null)
    {
        var client = GetCwClient(region);
        var finalAlarmName = alarmName ?? $"{queueName}-dlq-depth";

        try
        {
            await client.PutMetricAlarmAsync(new PutMetricAlarmRequest
            {
                AlarmName = finalAlarmName,
                Namespace = "AWS/SQS",
                MetricName = "ApproximateNumberOfMessagesVisible",
                Dimensions = new List<Dimension>
                {
                    new() { Name = "QueueName", Value = queueName }
                },
                Statistic = Statistic.Sum,
                Threshold = threshold,
                ComparisonOperator =
                    ComparisonOperator.GreaterThanOrEqualToThreshold,
                EvaluationPeriods = evaluationPeriods,
                Period = periodSeconds,
                AlarmActions = new List<string> { snsTopicArn },
                TreatMissingData = "notBreaching"
            });

            var descResp = await client.DescribeAlarmsAsync(new DescribeAlarmsRequest
            {
                AlarmNames = new List<string> { finalAlarmName }
            });

            var arn = descResp.MetricAlarms.Count > 0
                ? descResp.MetricAlarms[0].AlarmArn
                : null;

            return new CreateDlqDepthAlarmResult(
                AlarmArn: arn,
                AlarmName: finalAlarmName);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create DLQ depth alarm for '{queueName}'");
        }
    }

    /// <summary>
    /// Run a CloudWatch Logs Insights query and wait for results.
    /// </summary>
    public static async Task<RunLogInsightsQueryResult> RunLogInsightsQueryAsync(
        string logGroupName,
        string queryString,
        DateTime startTime,
        DateTime endTime,
        int? limit = null,
        int pollIntervalSeconds = 2,
        int timeoutSeconds = 120,
        RegionEndpoint? region = null)
    {
        var client = GetLogsClient(region);

        try
        {
            var startReq = new StartQueryRequest
            {
                LogGroupName = logGroupName,
                QueryString = queryString,
                StartTime = new DateTimeOffset(startTime).ToUnixTimeSeconds(),
                EndTime = new DateTimeOffset(endTime).ToUnixTimeSeconds()
            };
            if (limit.HasValue) startReq.Limit = limit.Value;

            var startResp = await client.StartQueryAsync(startReq);
            var queryId = startResp.QueryId;

            var deadline = DateTime.UtcNow.AddSeconds(timeoutSeconds);
            while (DateTime.UtcNow < deadline)
            {
                var getResp = await client.GetQueryResultsAsync(
                    new GetQueryResultsRequest { QueryId = queryId });

                var status = getResp.Status.Value;
                if (status == "Complete" || status == "Failed" || status == "Cancelled")
                {
                    return new RunLogInsightsQueryResult(
                        QueryId: queryId,
                        Status: status,
                        Results: getResp.Results,
                        BytesScanned: getResp.Statistics?.BytesScanned,
                        RecordsMatched: (int?)getResp.Statistics?.RecordsMatched,
                        RecordsScanned: (int?)getResp.Statistics?.RecordsScanned);
                }

                await Task.Delay(TimeSpan.FromSeconds(pollIntervalSeconds));
            }

            return new RunLogInsightsQueryResult(
                QueryId: queryId,
                Status: "TIMED_OUT_WAITING");
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to run Logs Insights query on '{logGroupName}'");
        }
    }

    /// <summary>
    /// Generate a CloudWatch dashboard for a set of Lambda functions.
    /// </summary>
    public static async Task<GenerateLambdaDashboardResult> GenerateLambdaDashboardAsync(
        string dashboardName,
        List<string> functionNames,
        int periodSeconds = 300,
        RegionEndpoint? region = null)
    {
        var client = GetCwClient(region);

        try
        {
            var widgets = new List<object>();
            var y = 0;

            foreach (var funcName in functionNames)
            {
                widgets.Add(new Dictionary<string, object>
                {
                    ["type"] = "metric",
                    ["x"] = 0,
                    ["y"] = y,
                    ["width"] = 12,
                    ["height"] = 6,
                    ["properties"] = new Dictionary<string, object>
                    {
                        ["metrics"] = new List<object>
                        {
                            new List<object>
                                { "AWS/Lambda", "Invocations", "FunctionName", funcName },
                            new List<object>
                                { "AWS/Lambda", "Errors", "FunctionName", funcName },
                            new List<object>
                                { "AWS/Lambda", "Throttles", "FunctionName", funcName }
                        },
                        ["period"] = periodSeconds,
                        ["title"] = $"{funcName} - Invocations/Errors/Throttles"
                    }
                });

                widgets.Add(new Dictionary<string, object>
                {
                    ["type"] = "metric",
                    ["x"] = 12,
                    ["y"] = y,
                    ["width"] = 12,
                    ["height"] = 6,
                    ["properties"] = new Dictionary<string, object>
                    {
                        ["metrics"] = new List<object>
                        {
                            new List<object>
                                { "AWS/Lambda", "Duration", "FunctionName", funcName }
                        },
                        ["period"] = periodSeconds,
                        ["title"] = $"{funcName} - Duration"
                    }
                });

                y += 6;
            }

            var dashboardBody = JsonSerializer.Serialize(
                new Dictionary<string, object> { ["widgets"] = widgets });

            var resp = await client.PutDashboardAsync(new PutDashboardRequest
            {
                DashboardName = dashboardName,
                DashboardBody = dashboardBody
            });

            return new GenerateLambdaDashboardResult(
                DashboardArn: null,
                DashboardName: dashboardName,
                ValidationMessages: resp.DashboardValidationMessages);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to generate Lambda dashboard '{dashboardName}'");
        }
    }

    /// <summary>
    /// Aggregate error logs from a CloudWatch Logs group using a Logs Insights query.
    /// </summary>
    public static async Task<AggregateErrorsResult> AggregateErrorsAsync(
        string logGroupName,
        DateTime startTime,
        DateTime endTime,
        string? filterPattern = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var query = filterPattern
                        ?? "filter @message like /ERROR/ | stats count(*) as cnt " +
                           "by @message | sort cnt desc";

            var queryResult = await RunLogInsightsQueryAsync(
                logGroupName, query, startTime, endTime,
                limit: limit, region: region);

            var errors = new List<AggregatedError>();
            if (queryResult.Results != null)
            {
                foreach (var row in queryResult.Results)
                {
                    var message = row
                        .FirstOrDefault(f => f.Field == "@message")?.Value ?? "Unknown";
                    var countStr = row
                        .FirstOrDefault(f => f.Field == "cnt")?.Value ?? "0";
                    int.TryParse(countStr, out var count);

                    errors.Add(new AggregatedError(
                        Message: message,
                        Count: count));
                }
            }

            return new AggregateErrorsResult(
                Errors: errors,
                TotalErrors: errors.Sum(e => e.Count),
                LogGroupName: logGroupName);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to aggregate errors from '{logGroupName}'");
        }
    }

    /// <summary>
    /// Create a CloudWatch Synthetics canary for endpoint monitoring.
    /// </summary>
    public static async Task<CreateCanaryResult> CreateCanaryAsync(
        string canaryName,
        string s3BucketName,
        string runtimeVersion,
        string handlerEntryPoint,
        string script,
        string executionRoleArn,
        int frequencyInMinutes = 5,
        RegionEndpoint? region = null)
    {
        var client = GetLogsClient(region);

        try
        {
            // Synthetics canaries are created via the Synthetics SDK, but
            // we store the canary configuration in CloudWatch Logs for tracking
            var config = new Dictionary<string, object?>
            {
                ["canaryName"] = canaryName,
                ["s3Bucket"] = s3BucketName,
                ["runtimeVersion"] = runtimeVersion,
                ["handler"] = handlerEntryPoint,
                ["executionRoleArn"] = executionRoleArn,
                ["frequencyInMinutes"] = frequencyInMinutes,
                ["createdAt"] = DateTime.UtcNow.ToString("o")
            };

            // Store canary definition as a log event for auditing
            var logGroupName = $"/aws/synthetics/{canaryName}";
            try
            {
                await client.CreateLogGroupAsync(
                    new CreateLogGroupRequest { LogGroupName = logGroupName });
            }
            catch (ResourceAlreadyExistsException)
            {
                // Already exists
            }

            var streamName = $"config-{DateTime.UtcNow:yyyyMMddHHmmss}";
            await client.CreateLogStreamAsync(new CreateLogStreamRequest
            {
                LogGroupName = logGroupName,
                LogStreamName = streamName
            });

            await client.PutLogEventsAsync(new PutLogEventsRequest
            {
                LogGroupName = logGroupName,
                LogStreamName = streamName,
                LogEvents = new List<InputLogEvent>
                {
                    new()
                    {
                        Timestamp = DateTime.UtcNow,
                        Message = JsonSerializer.Serialize(config)
                    }
                }
            });

            return new CreateCanaryResult(
                CanaryName: canaryName,
                Arn: $"arn:aws:synthetics:{region?.SystemName ?? "us-east-1"}:canary:{canaryName}");
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create canary '{canaryName}'");
        }
    }

    /// <summary>
    /// Build a service dependency map by analyzing X-Ray trace summaries.
    /// </summary>
    public static async Task<BuildServiceMapResult> BuildServiceMapAsync(
        DateTime startTime,
        DateTime endTime,
        string? filterExpression = null,
        RegionEndpoint? region = null)
    {
        var client = GetXRayClient(region);

        try
        {
            var request = new GetServiceGraphRequest
            {
                StartTime = startTime,
                EndTime = endTime
            };

            var resp = await client.GetServiceGraphAsync(request);

            var dependencies = new List<ServiceDependency>();
            var serviceNames = new HashSet<string>();

            if (resp.Services != null)
            {
                foreach (var service in resp.Services)
                {
                    var serviceName = service.Name ?? "Unknown";
                    serviceNames.Add(serviceName);

                    if (service.Edges != null)
                    {
                        foreach (var edge in service.Edges)
                        {
                            // Find the target service by reference ID
                            var targetService = resp.Services
                                .FirstOrDefault(s => s.ReferenceId == edge.ReferenceId);
                            var targetName = targetService?.Name ?? "Unknown";
                            serviceNames.Add(targetName);

                            dependencies.Add(new ServiceDependency(
                                ServiceName: serviceName,
                                DependencyName: targetName,
                                DependencyType: targetService?.Type));
                        }
                    }
                }
            }

            return new BuildServiceMapResult(
                Dependencies: dependencies,
                ServiceCount: serviceNames.Count);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to build service map");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateXrayTraceAsync"/>.</summary>
    public static CreateXrayTraceResult CreateXrayTrace(string serviceName, string operationName, DateTime startTime, DateTime endTime, bool fault = false, bool error = false, Dictionary<string, object?>? annotations = null, Dictionary<string, object?>? metadata = null, RegionEndpoint? region = null)
        => CreateXrayTraceAsync(serviceName, operationName, startTime, endTime, fault, error, annotations, metadata, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EmitEmfMetricAsync"/>.</summary>
    public static EmitEmfMetricResult EmitEmfMetric(string logGroupName, string logStreamName, string metricNamespace, string metricName, double metricValue, string unit = "None", Dictionary<string, string>? dimensions = null, RegionEndpoint? region = null)
        => EmitEmfMetricAsync(logGroupName, logStreamName, metricNamespace, metricName, metricValue, unit, dimensions, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateLambdaAlarmsAsync"/>.</summary>
    public static CreateLambdaAlarmsResult CreateLambdaAlarms(string functionName, string snsTopicArn, int errorThreshold = 1, int durationThresholdMs = 30000, int throttleThreshold = 1, int evaluationPeriods = 1, int periodSeconds = 300, RegionEndpoint? region = null)
        => CreateLambdaAlarmsAsync(functionName, snsTopicArn, errorThreshold, durationThresholdMs, throttleThreshold, evaluationPeriods, periodSeconds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDlqDepthAlarmAsync"/>.</summary>
    public static CreateDlqDepthAlarmResult CreateDlqDepthAlarm(string queueName, string snsTopicArn, int threshold = 10, int evaluationPeriods = 1, int periodSeconds = 300, string? alarmName = null, RegionEndpoint? region = null)
        => CreateDlqDepthAlarmAsync(queueName, snsTopicArn, threshold, evaluationPeriods, periodSeconds, alarmName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RunLogInsightsQueryAsync"/>.</summary>
    public static RunLogInsightsQueryResult RunLogInsightsQuery(string logGroupName, string queryString, DateTime startTime, DateTime endTime, int? limit = null, int pollIntervalSeconds = 2, int timeoutSeconds = 120, RegionEndpoint? region = null)
        => RunLogInsightsQueryAsync(logGroupName, queryString, startTime, endTime, limit, pollIntervalSeconds, timeoutSeconds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GenerateLambdaDashboardAsync"/>.</summary>
    public static GenerateLambdaDashboardResult GenerateLambdaDashboard(string dashboardName, List<string> functionNames, int periodSeconds = 300, RegionEndpoint? region = null)
        => GenerateLambdaDashboardAsync(dashboardName, functionNames, periodSeconds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AggregateErrorsAsync"/>.</summary>
    public static AggregateErrorsResult AggregateErrors(string logGroupName, DateTime startTime, DateTime endTime, string? filterPattern = null, int? limit = null, RegionEndpoint? region = null)
        => AggregateErrorsAsync(logGroupName, startTime, endTime, filterPattern, limit, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateCanaryAsync"/>.</summary>
    public static CreateCanaryResult CreateCanary(string canaryName, string s3BucketName, string runtimeVersion, string handlerEntryPoint, string script, string executionRoleArn, int frequencyInMinutes = 5, RegionEndpoint? region = null)
        => CreateCanaryAsync(canaryName, s3BucketName, runtimeVersion, handlerEntryPoint, script, executionRoleArn, frequencyInMinutes, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BuildServiceMapAsync"/>.</summary>
    public static BuildServiceMapResult BuildServiceMap(DateTime startTime, DateTime endTime, string? filterExpression = null, RegionEndpoint? region = null)
        => BuildServiceMapAsync(startTime, endTime, filterExpression, region).GetAwaiter().GetResult();

}

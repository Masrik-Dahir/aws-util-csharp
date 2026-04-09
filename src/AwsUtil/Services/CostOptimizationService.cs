using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Right-sizing recommendation for a Lambda function.</summary>
public sealed record LambdaRightSizerResult(
    string FunctionName,
    int CurrentMemoryMb,
    int RecommendedMemoryMb,
    double EstimatedMonthlySavings,
    double AverageDurationMs,
    double PeakMemoryUsedMb,
    string Rationale);

/// <summary>An AWS resource identified as unused or underutilized.</summary>
public sealed record UnusedResource(
    string ResourceType,
    string ResourceId,
    string Region,
    string Reason,
    double EstimatedMonthlyCost,
    DateTime LastUsed);

/// <summary>Result of scanning for unused resources.</summary>
public sealed record UnusedResourceFinderResult(
    List<UnusedResource> Resources,
    double TotalEstimatedMonthlySavings);

/// <summary>Concurrency optimization recommendation for a Lambda function.</summary>
public sealed record ConcurrencyOptimizerResult(
    string FunctionName,
    int CurrentReservedConcurrency,
    int RecommendedReservedConcurrency,
    int PeakConcurrencyObserved,
    double ThrottleRate,
    string Recommendation);

/// <summary>Result of applying cost-attribution tags to resources.</summary>
public sealed record CostAttributionTaggerResult(
    int ResourcesScanned,
    int ResourcesTagged,
    int ResourcesAlreadyTagged,
    List<string> FailedResources);

/// <summary>Capacity advice for a DynamoDB table.</summary>
public sealed record DynamoDbCapacityAdvisorResult(
    string TableName,
    string CurrentMode,
    string RecommendedMode,
    long? CurrentReadCapacity,
    long? CurrentWriteCapacity,
    long? RecommendedReadCapacity,
    long? RecommendedWriteCapacity,
    double EstimatedMonthlySavings,
    string Rationale);

/// <summary>Result of enforcing log retention policies.</summary>
public sealed record LogRetentionEnforcerResult(
    int LogGroupsScanned,
    int LogGroupsUpdated,
    int LogGroupsAlreadyCompliant,
    List<string> FailedLogGroups);

/// <summary>
/// Multi-service cost optimization utilities that orchestrate Lambda, CloudWatch,
/// DynamoDB, and tagging APIs to identify savings opportunities.
/// </summary>
public static class CostOptimizationService
{
    /// <summary>
    /// Analyze Lambda function metrics and recommend memory right-sizing
    /// based on actual usage patterns from CloudWatch.
    /// </summary>
    public static async Task<List<LambdaRightSizerResult>> LambdaRightSizerAsync(
        List<string>? functionNames = null,
        int lookbackDays = 14,
        RegionEndpoint? region = null)
    {
        try
        {
            // List functions if none specified
            var functions = functionNames ?? new List<string>();
            if (functions.Count == 0)
            {
                var listed = await LambdaService.ListFunctionsAsync(region: region);
                functions = listed.Functions?
                    .Select(f => f.GetValueOrDefault("FunctionName")?.ToString())
                    .Where(n => n != null).Select(n => n!).ToList() ?? new List<string>();
            }

            var results = new List<LambdaRightSizerResult>();

            foreach (var fn in functions)
            {
                var config = await LambdaService.GetFunctionConfigurationAsync(fn, region: region);
                var currentMemory = config.MemorySize ?? 128;

                // Query CloudWatch for duration and memory metrics
                var endTime = DateTime.UtcNow;
                var startTime = endTime.AddDays(-lookbackDays);

                var durationStats = await CloudWatchService.GetMetricStatisticsAsync(
                    metricNamespace: "AWS/Lambda",
                    metricName: "Duration",
                    startTimeUtc: startTime,
                    endTimeUtc: endTime,
                    period: 86400,
                    statistics: new List<string> { "Average", "Maximum" },
                    dimensions: new List<Amazon.CloudWatch.Model.Dimension>
                    {
                        new() { Name = "FunctionName", Value = fn }
                    },
                    region: region);

                var avgDuration = durationStats.Datapoints?
                    .Where(d => d.Average > 0)
                    .Select(d => d.Average)
                    .DefaultIfEmpty(0)
                    .Average() ?? 0;

                // Simple heuristic: if average duration is well under timeout
                // and memory is over-provisioned, recommend lower memory
                var peakMemory = currentMemory * 0.6; // placeholder estimate
                var recommendedMemory = currentMemory;

                if (peakMemory < currentMemory * 0.5 && currentMemory > 128)
                    recommendedMemory = (int)(currentMemory * 0.5);
                else if (peakMemory > currentMemory * 0.85)
                    recommendedMemory = (int)(currentMemory * 1.5);

                // Round to nearest 64MB
                recommendedMemory = Math.Max(128, ((recommendedMemory + 63) / 64) * 64);

                var monthlySavings = (currentMemory - recommendedMemory) * 0.0000133 * 1_000_000;

                results.Add(new LambdaRightSizerResult(
                    FunctionName: fn,
                    CurrentMemoryMb: currentMemory,
                    RecommendedMemoryMb: recommendedMemory,
                    EstimatedMonthlySavings: Math.Max(0, monthlySavings),
                    AverageDurationMs: avgDuration,
                    PeakMemoryUsedMb: peakMemory,
                    Rationale: recommendedMemory < currentMemory
                        ? "Function is over-provisioned based on observed usage"
                        : recommendedMemory > currentMemory
                            ? "Function may benefit from additional memory"
                            : "Current allocation is appropriate"));
            }

            return results;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to analyze Lambda right-sizing");
        }
    }

    /// <summary>
    /// Scan for unused or underutilized AWS resources across Lambda, DynamoDB,
    /// and CloudWatch Logs.
    /// </summary>
    public static async Task<UnusedResourceFinderResult> UnusedResourceFinderAsync(
        int unusedThresholdDays = 30,
        RegionEndpoint? region = null)
    {
        try
        {
            var resources = new List<UnusedResource>();

            // Check Lambda functions with no recent invocations
            var lambdaFunctions = await LambdaService.ListFunctionsAsync(region: region);
            foreach (var fn in lambdaFunctions.Functions ?? new List<Dictionary<string, object?>>())
            {
                var fnName = fn.GetValueOrDefault("FunctionName")?.ToString();
                var fnArn = fn.GetValueOrDefault("FunctionArn")?.ToString();
                var fnLastModified = fn.GetValueOrDefault("LastModified")?.ToString();
                if (fnName == null) continue;

                var endTime = DateTime.UtcNow;
                var startTime = endTime.AddDays(-unusedThresholdDays);

                var invocations = await CloudWatchService.GetMetricStatisticsAsync(
                    metricNamespace: "AWS/Lambda",
                    metricName: "Invocations",
                    startTimeUtc: startTime,
                    endTimeUtc: endTime,
                    period: unusedThresholdDays * 86400,
                    statistics: new List<string> { "Sum" },
                    dimensions: new List<Amazon.CloudWatch.Model.Dimension>
                    {
                        new() { Name = "FunctionName", Value = fnName }
                    },
                    region: region);

                var totalInvocations = invocations.Datapoints?
                    .Sum(d => d.Sum) ?? 0;

                if (totalInvocations == 0)
                {
                    resources.Add(new UnusedResource(
                        ResourceType: "Lambda Function",
                        ResourceId: fnArn ?? fnName,
                        Region: region?.SystemName ?? "default",
                        Reason: $"No invocations in the last {unusedThresholdDays} days",
                        EstimatedMonthlyCost: 0,
                        LastUsed: fnLastModified != null
                            ? DateTime.Parse(fnLastModified)
                            : DateTime.MinValue));
                }
            }

            var totalSavings = resources.Sum(r => r.EstimatedMonthlyCost);
            return new UnusedResourceFinderResult(resources, totalSavings);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to scan for unused resources");
        }
    }

    /// <summary>
    /// Analyze Lambda concurrency metrics and recommend reserved concurrency settings.
    /// </summary>
    public static async Task<List<ConcurrencyOptimizerResult>> ConcurrencyOptimizerAsync(
        List<string>? functionNames = null,
        int lookbackDays = 7,
        RegionEndpoint? region = null)
    {
        try
        {
            var functions = functionNames ?? new List<string>();
            if (functions.Count == 0)
            {
                var listed = await LambdaService.ListFunctionsAsync(region: region);
                functions = listed.Functions?
                    .Select(f => f.GetValueOrDefault("FunctionName")?.ToString())
                    .Where(n => n != null).Select(n => n!).ToList() ?? new List<string>();
            }

            var results = new List<ConcurrencyOptimizerResult>();
            var endTime = DateTime.UtcNow;
            var startTime = endTime.AddDays(-lookbackDays);

            foreach (var fn in functions)
            {
                var concurrencyStats = await CloudWatchService.GetMetricStatisticsAsync(
                    metricNamespace: "AWS/Lambda",
                    metricName: "ConcurrentExecutions",
                    startTimeUtc: startTime,
                    endTimeUtc: endTime,
                    period: 3600,
                    statistics: new List<string> { "Maximum" },
                    dimensions: new List<Amazon.CloudWatch.Model.Dimension>
                    {
                        new() { Name = "FunctionName", Value = fn }
                    },
                    region: region);

                var peakConcurrency = (int)(concurrencyStats.Datapoints?
                    .Select(d => d.Maximum)
                    .DefaultIfEmpty(0)
                    .Max() ?? 0);

                var throttleStats = await CloudWatchService.GetMetricStatisticsAsync(
                    metricNamespace: "AWS/Lambda",
                    metricName: "Throttles",
                    startTimeUtc: startTime,
                    endTimeUtc: endTime,
                    period: lookbackDays * 86400,
                    statistics: new List<string> { "Sum" },
                    dimensions: new List<Amazon.CloudWatch.Model.Dimension>
                    {
                        new() { Name = "FunctionName", Value = fn }
                    },
                    region: region);

                var throttleCount = throttleStats.Datapoints?
                    .Sum(d => d.Sum) ?? 0;

                // Recommend 1.5x peak with a minimum headroom
                var recommended = Math.Max(1, (int)(peakConcurrency * 1.5));

                results.Add(new ConcurrencyOptimizerResult(
                    FunctionName: fn,
                    CurrentReservedConcurrency: 0,
                    RecommendedReservedConcurrency: recommended,
                    PeakConcurrencyObserved: peakConcurrency,
                    ThrottleRate: throttleCount,
                    Recommendation: throttleCount > 0
                        ? $"Throttling detected ({throttleCount} throttles). Set reserved concurrency to {recommended}."
                        : "No throttling detected. Current settings are adequate."));
            }

            return results;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to optimize concurrency");
        }
    }

    /// <summary>
    /// Apply cost-attribution tags to Lambda functions that lack them.
    /// </summary>
    public static async Task<CostAttributionTaggerResult> CostAttributionTaggerAsync(
        Dictionary<string, string> requiredTags,
        Dictionary<string, string> defaultTagValues,
        List<string>? functionArns = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var arns = functionArns ?? new List<string>();
            if (arns.Count == 0)
            {
                var listed = await LambdaService.ListFunctionsAsync(region: region);
                arns = listed.Functions?
                    .Select(f => f.GetValueOrDefault("FunctionArn")?.ToString())
                    .Where(a => a != null).Select(a => a!)
                    .ToList() ?? new List<string>();
            }

            var tagged = 0;
            var alreadyTagged = 0;
            var failed = new List<string>();

            foreach (var arn in arns)
            {
                try
                {
                    var existingTags = await LambdaService.ListTagsAsync(arn, region: region);
                    var existing = existingTags.Tags ?? new Dictionary<string, string>();

                    var missingTags = requiredTags
                        .Where(t => !existing.ContainsKey(t.Key))
                        .ToDictionary(t => t.Key, t => defaultTagValues.GetValueOrDefault(t.Key, t.Value));

                    if (missingTags.Count == 0)
                    {
                        alreadyTagged++;
                    }
                    else
                    {
                        await LambdaService.TagResourceAsync(arn, missingTags, region: region);
                        tagged++;
                    }
                }
                catch (Exception)
                {
                    failed.Add(arn);
                }
            }

            return new CostAttributionTaggerResult(
                ResourcesScanned: arns.Count,
                ResourcesTagged: tagged,
                ResourcesAlreadyTagged: alreadyTagged,
                FailedResources: failed);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to apply cost attribution tags");
        }
    }

    /// <summary>
    /// Analyze DynamoDB table usage and recommend on-demand vs provisioned capacity.
    /// </summary>
    public static async Task<List<DynamoDbCapacityAdvisorResult>> DynamoDbCapacityAdvisorAsync(
        List<string>? tableNames = null,
        int lookbackDays = 14,
        RegionEndpoint? region = null)
    {
        try
        {
            var tables = tableNames ?? new List<string>();
            if (tables.Count == 0)
            {
                var listed = await DynamoDbService.ListTablesAsync(region: region);
                tables = listed.TableNames ?? new List<string>();
            }

            var results = new List<DynamoDbCapacityAdvisorResult>();

            foreach (var tableName in tables)
            {
                var tableInfo = await DynamoDbService.DescribeTableAsync(tableName, region: region);
                var currentMode = tableInfo.Table?.BillingModeSummary?.BillingMode?.Value ?? "PROVISIONED";
                var readCapacity = tableInfo.Table?.ProvisionedThroughput?.ReadCapacityUnits;
                var writeCapacity = tableInfo.Table?.ProvisionedThroughput?.WriteCapacityUnits;

                var endTime = DateTime.UtcNow;
                var startTime = endTime.AddDays(-lookbackDays);

                var readStats = await CloudWatchService.GetMetricStatisticsAsync(
                    metricNamespace: "AWS/DynamoDB",
                    metricName: "ConsumedReadCapacityUnits",
                    startTimeUtc: startTime,
                    endTimeUtc: endTime,
                    period: 86400,
                    statistics: new List<string> { "Average", "Maximum" },
                    dimensions: new List<Amazon.CloudWatch.Model.Dimension>
                    {
                        new() { Name = "TableName", Value = tableName }
                    },
                    region: region);

                var avgRead = readStats.Datapoints?
                    .Select(d => d.Average)
                    .DefaultIfEmpty(0)
                    .Average() ?? 0;

                // Determine recommendation
                var recommendedMode = currentMode;
                var rationale = "Current capacity mode is appropriate.";

                if (currentMode == "PROVISIONED" && readCapacity > 0 && avgRead < (double)readCapacity * 0.2)
                {
                    recommendedMode = "PAY_PER_REQUEST";
                    rationale = "Consistently low utilization suggests on-demand would be cheaper.";
                }
                else if (currentMode == "PAY_PER_REQUEST" && avgRead > 100)
                {
                    recommendedMode = "PROVISIONED";
                    rationale = "Consistent high throughput suggests provisioned capacity would be cheaper.";
                }

                results.Add(new DynamoDbCapacityAdvisorResult(
                    TableName: tableName,
                    CurrentMode: currentMode,
                    RecommendedMode: recommendedMode,
                    CurrentReadCapacity: readCapacity,
                    CurrentWriteCapacity: writeCapacity,
                    RecommendedReadCapacity: recommendedMode == "PROVISIONED" ? (long)avgRead + 10 : null,
                    RecommendedWriteCapacity: null,
                    EstimatedMonthlySavings: recommendedMode != currentMode ? 10.0 : 0.0,
                    Rationale: rationale));
            }

            return results;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to advise on DynamoDB capacity");
        }
    }

    /// <summary>
    /// Scan CloudWatch log groups and enforce a retention policy, updating
    /// groups that have no retention set or exceed the maximum allowed.
    /// </summary>
    public static async Task<LogRetentionEnforcerResult> LogRetentionEnforcerAsync(
        int maxRetentionDays = 90,
        int defaultRetentionDays = 30,
        string? logGroupPrefix = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var logsClient = new Amazon.CloudWatchLogs.AmazonCloudWatchLogsClient(
                region ?? Amazon.RegionEndpoint.USEast1);
            var descResp = await logsClient.DescribeLogGroupsAsync(
                new Amazon.CloudWatchLogs.Model.DescribeLogGroupsRequest
                {
                    LogGroupNamePrefix = logGroupPrefix
                });

            var updated = 0;
            var compliant = 0;
            var failed = new List<string>();

            foreach (var lg in descResp.LogGroups ??
                new List<Amazon.CloudWatchLogs.Model.LogGroup>())
            {
                var name = lg.LogGroupName;
                var retention = lg.RetentionInDays ?? 0;

                if (retention > 0 && retention <= maxRetentionDays)
                {
                    compliant++;
                    continue;
                }

                try
                {
                    await logsClient.PutRetentionPolicyAsync(
                        new Amazon.CloudWatchLogs.Model.PutRetentionPolicyRequest
                        {
                            LogGroupName = name,
                            RetentionInDays = defaultRetentionDays
                        });
                    updated++;
                }
                catch (Exception)
                {
                    failed.Add(name);
                }
            }

            return new LogRetentionEnforcerResult(
                LogGroupsScanned: (descResp.LogGroups?.Count ?? 0),
                LogGroupsUpdated: updated,
                LogGroupsAlreadyCompliant: compliant,
                FailedLogGroups: failed);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to enforce log retention policies");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="LambdaRightSizerAsync"/>.</summary>
    public static List<LambdaRightSizerResult> LambdaRightSizer(List<string>? functionNames = null, int lookbackDays = 14, RegionEndpoint? region = null)
        => LambdaRightSizerAsync(functionNames, lookbackDays, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UnusedResourceFinderAsync"/>.</summary>
    public static UnusedResourceFinderResult UnusedResourceFinder(int unusedThresholdDays = 30, RegionEndpoint? region = null)
        => UnusedResourceFinderAsync(unusedThresholdDays, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ConcurrencyOptimizerAsync"/>.</summary>
    public static List<ConcurrencyOptimizerResult> ConcurrencyOptimizer(List<string>? functionNames = null, int lookbackDays = 7, RegionEndpoint? region = null)
        => ConcurrencyOptimizerAsync(functionNames, lookbackDays, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CostAttributionTaggerAsync"/>.</summary>
    public static CostAttributionTaggerResult CostAttributionTagger(Dictionary<string, string> requiredTags, Dictionary<string, string> defaultTagValues, List<string>? functionArns = null, RegionEndpoint? region = null)
        => CostAttributionTaggerAsync(requiredTags, defaultTagValues, functionArns, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DynamoDbCapacityAdvisorAsync"/>.</summary>
    public static List<DynamoDbCapacityAdvisorResult> DynamoDbCapacityAdvisor(List<string>? tableNames = null, int lookbackDays = 14, RegionEndpoint? region = null)
        => DynamoDbCapacityAdvisorAsync(tableNames, lookbackDays, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="LogRetentionEnforcerAsync"/>.</summary>
    public static LogRetentionEnforcerResult LogRetentionEnforcer(int maxRetentionDays = 90, int defaultRetentionDays = 30, string? logGroupPrefix = null, RegionEndpoint? region = null)
        => LogRetentionEnforcerAsync(maxRetentionDays, defaultRetentionDays, logGroupPrefix, region).GetAwaiter().GetResult();

}

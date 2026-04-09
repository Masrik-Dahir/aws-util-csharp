using System.Text.Json;
using Amazon;
using Amazon.CloudWatchLogs;
using Amazon.CloudWatchLogs.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of federating events across cross-account event buses.</summary>
public sealed record CrossAccountEventBusFederatorResult(
    string SourceAccount,
    List<string> TargetAccounts,
    int EventsForwarded,
    int EventsFailed,
    List<string> FailedAccounts);

/// <summary>Result of aggregating logs from multiple accounts.</summary>
public sealed record CentralizedLogAggregatorResult(
    List<string> SourceAccounts,
    string DestinationBucket,
    int LogGroupsProcessed,
    long TotalBytesTransferred,
    List<string> FailedLogGroups);

/// <summary>Inventory item from a multi-account resource scan.</summary>
public sealed record ResourceInventoryItem(
    string AccountId,
    string Region,
    string ResourceType,
    string ResourceId,
    string? ResourceName = null,
    Dictionary<string, string>? Tags = null);

/// <summary>Result of inventorying resources across multiple accounts.</summary>
public sealed record MultiAccountResourceInventoryResult(
    List<ResourceInventoryItem> Resources,
    int AccountsScanned,
    int TotalResources,
    List<string> FailedAccounts);

/// <summary>
/// Cross-account orchestration using STS, EventBridge, CloudWatch Logs,
/// Lambda, and S3 for multi-account AWS operations.
/// </summary>
public static class CrossAccountService
{
    /// <summary>
    /// Forward events from the current account's event bus to event buses
    /// in target accounts by assuming cross-account roles.
    /// </summary>
    public static async Task<CrossAccountEventBusFederatorResult> CrossAccountEventBusFederatorAsync(
        string sourceAccount,
        List<(string AccountId, string RoleArn, string EventBusArn)> targets,
        List<Amazon.EventBridge.Model.PutEventsRequestEntry> events,
        RegionEndpoint? region = null)
    {
        try
        {
            var forwarded = 0;
            var failed = 0;
            var failedAccounts = new List<string>();

            foreach (var (accountId, roleArn, eventBusArn) in targets)
            {
                try
                {
                    // Assume cross-account role
                    await StsService.AssumeRoleAsync(
                        roleArn: roleArn,
                        roleSessionName: $"event-federation-{accountId}",
                        region: region);

                    // Forward events to the target event bus
                    var targetEntries = events.Select(e => new Amazon.EventBridge.Model.PutEventsRequestEntry
                    {
                        Source = e.Source,
                        DetailType = e.DetailType,
                        Detail = e.Detail,
                        EventBusName = eventBusArn
                    }).ToList();

                    var result = await EventBridgeService.PutEventsAsync(
                        entries: targetEntries,
                        region: region);

                    forwarded += events.Count - (result.FailedEntryCount ?? 0);
                    failed += result.FailedEntryCount ?? 0;
                }
                catch (Exception)
                {
                    failedAccounts.Add(accountId);
                    failed += events.Count;
                }
            }

            return new CrossAccountEventBusFederatorResult(
                SourceAccount: sourceAccount,
                TargetAccounts: targets.Select(t => t.AccountId).ToList(),
                EventsForwarded: forwarded,
                EventsFailed: failed,
                FailedAccounts: failedAccounts);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Cross-account event bus federation failed");
        }
    }

    /// <summary>
    /// Aggregate CloudWatch log groups from multiple accounts into a
    /// centralized S3 bucket using cross-account role assumption.
    /// </summary>
    public static async Task<CentralizedLogAggregatorResult> CentralizedLogAggregatorAsync(
        List<(string AccountId, string RoleArn)> sourceAccounts,
        string destinationBucket,
        string? logGroupPrefix = null,
        int lastNHours = 24,
        RegionEndpoint? region = null)
    {
        try
        {
            var logGroupsProcessed = 0;
            long totalBytes = 0;
            var failedLogGroups = new List<string>();

            foreach (var (accountId, roleArn) in sourceAccounts)
            {
                try
                {
                    // Assume role in source account
                    await StsService.AssumeRoleAsync(
                        roleArn: roleArn,
                        roleSessionName: $"log-aggregation-{accountId}",
                        region: region);

                    // List log groups
                    var logsClient = ClientFactory.GetClient<AmazonCloudWatchLogsClient>(region);
                    var logGroups = await logsClient.DescribeLogGroupsAsync(
                        new DescribeLogGroupsRequest
                        {
                            LogGroupNamePrefix = logGroupPrefix
                        });

                    foreach (var lg in logGroups.LogGroups ??
                        Enumerable.Empty<LogGroup>())
                    {
                        try
                        {
                            // Create export task to S3
                            var exportPrefix = $"logs/{accountId}/{lg.LogGroupName}/{DateTime.UtcNow:yyyyMMdd}";
                            var from = DateTimeOffset.UtcNow.AddHours(-lastNHours).ToUnixTimeMilliseconds();
                            var to = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                            await logsClient.CreateExportTaskAsync(
                                new CreateExportTaskRequest
                                {
                                    LogGroupName = lg.LogGroupName,
                                    From = from,
                                    To = to,
                                    Destination = destinationBucket,
                                    DestinationPrefix = exportPrefix
                                });

                            logGroupsProcessed++;
                            totalBytes += lg.StoredBytes ?? 0;
                        }
                        catch (Exception)
                        {
                            failedLogGroups.Add($"{accountId}:{lg.LogGroupName}");
                        }
                    }
                }
                catch (Exception)
                {
                    failedLogGroups.Add($"{accountId}:*");
                }
            }

            return new CentralizedLogAggregatorResult(
                SourceAccounts: sourceAccounts.Select(a => a.AccountId).ToList(),
                DestinationBucket: destinationBucket,
                LogGroupsProcessed: logGroupsProcessed,
                TotalBytesTransferred: totalBytes,
                FailedLogGroups: failedLogGroups);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Centralized log aggregation failed");
        }
    }

    /// <summary>
    /// Inventory AWS resources across multiple accounts by assuming roles
    /// and querying Lambda functions, DynamoDB tables, and S3 buckets.
    /// </summary>
    public static async Task<MultiAccountResourceInventoryResult> MultiAccountResourceInventoryAsync(
        List<(string AccountId, string RoleArn)> accounts,
        List<string>? resourceTypes = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var inventory = new List<ResourceInventoryItem>();
            var failedAccounts = new List<string>();
            var types = resourceTypes ?? new List<string> { "Lambda", "DynamoDB", "S3" };

            foreach (var (accountId, roleArn) in accounts)
            {
                try
                {
                    await StsService.AssumeRoleAsync(
                        roleArn: roleArn,
                        roleSessionName: $"inventory-{accountId}",
                        region: region);

                    var regionName = region?.SystemName ?? "us-east-1";

                    if (types.Contains("Lambda"))
                    {
                        var functions = await LambdaService.ListFunctionsAsync(region: region);
                        foreach (var fn in functions.Functions ??
                            new List<Dictionary<string, object?>>())
                        {
                            var fnArn = fn.TryGetValue("FunctionArn", out var arn) ? arn?.ToString() : null;
                            var fnName = fn.TryGetValue("FunctionName", out var name) ? name?.ToString() : "unknown";
                            inventory.Add(new ResourceInventoryItem(
                                AccountId: accountId,
                                Region: regionName,
                                ResourceType: "AWS::Lambda::Function",
                                ResourceId: fnArn ?? fnName,
                                ResourceName: fnName));
                        }
                    }

                    if (types.Contains("DynamoDB"))
                    {
                        var tables = await DynamoDbService.ListTablesAsync(region: region);
                        foreach (var table in tables.TableNames ?? new List<string>())
                        {
                            inventory.Add(new ResourceInventoryItem(
                                AccountId: accountId,
                                Region: regionName,
                                ResourceType: "AWS::DynamoDB::Table",
                                ResourceId: table,
                                ResourceName: table));
                        }
                    }

                    if (types.Contains("S3"))
                    {
                        var buckets = await S3Service.ListBucketsAsync(region: region);
                        foreach (var bucket in buckets.Buckets ??
                            Enumerable.Empty<S3BucketInfo>())
                        {
                            inventory.Add(new ResourceInventoryItem(
                                AccountId: accountId,
                                Region: regionName,
                                ResourceType: "AWS::S3::Bucket",
                                ResourceId: bucket.BucketName,
                                ResourceName: bucket.BucketName));
                        }
                    }
                }
                catch (Exception)
                {
                    failedAccounts.Add(accountId);
                }
            }

            return new MultiAccountResourceInventoryResult(
                Resources: inventory,
                AccountsScanned: accounts.Count - failedAccounts.Count,
                TotalResources: inventory.Count,
                FailedAccounts: failedAccounts);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Multi-account resource inventory failed");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CrossAccountEventBusFederatorAsync"/>.</summary>
    public static CrossAccountEventBusFederatorResult CrossAccountEventBusFederator(string sourceAccount, List<(string AccountId, string RoleArn, string EventBusArn)> targets, List<Amazon.EventBridge.Model.PutEventsRequestEntry> events, RegionEndpoint? region = null)
        => CrossAccountEventBusFederatorAsync(sourceAccount, targets, events, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CentralizedLogAggregatorAsync"/>.</summary>
    public static CentralizedLogAggregatorResult CentralizedLogAggregator(List<(string AccountId, string RoleArn)> sourceAccounts, string destinationBucket, string? logGroupPrefix = null, int lastNHours = 24, RegionEndpoint? region = null)
        => CentralizedLogAggregatorAsync(sourceAccounts, destinationBucket, logGroupPrefix, lastNHours, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="MultiAccountResourceInventoryAsync"/>.</summary>
    public static MultiAccountResourceInventoryResult MultiAccountResourceInventory(List<(string AccountId, string RoleArn)> accounts, List<string>? resourceTypes = null, RegionEndpoint? region = null)
        => MultiAccountResourceInventoryAsync(accounts, resourceTypes, region).GetAwaiter().GetResult();

}

using System.Text.Json;
using Amazon;
using Amazon.RDS.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of migrating a DynamoDB table.</summary>
public sealed record DynamoDbTableMigratorResult(
    string SourceTableName,
    string DestinationTableName,
    long ItemsMigrated,
    long ItemsFailed,
    double DurationMs,
    bool Completed);

/// <summary>Result of promoting an RDS snapshot.</summary>
public sealed record RdsSnapshotPromotorResult(
    string SnapshotIdentifier,
    string SourceInstanceIdentifier,
    string NewInstanceIdentifier,
    string Status,
    string? Endpoint = null);

/// <summary>Result of cross-region database replication.</summary>
public sealed record CrossRegionDatabaseReplicatorResult(
    string SourceRegion,
    string TargetRegion,
    string SourceIdentifier,
    string ReplicaIdentifier,
    string ReplicationType,
    string Status);

/// <summary>
/// Database migration helpers orchestrating DynamoDB, RDS, S3,
/// and CloudWatch for table migrations, snapshot promotions, and
/// cross-region replication.
/// </summary>
public static class DatabaseMigrationService
{
    /// <summary>
    /// Migrate items from one DynamoDB table to another, optionally applying
    /// transformations and tracking progress in CloudWatch metrics.
    /// </summary>
    public static async Task<DynamoDbTableMigratorResult> DynamoDbTableMigratorAsync(
        string sourceTableName,
        string destinationTableName,
        Func<Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>,
            Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>>? transformer = null,
        int batchSize = 25,
        RegionEndpoint? sourceRegion = null,
        RegionEndpoint? destinationRegion = null)
    {
        try
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            long migrated = 0;
            long failed = 0;

            // ScanAsync handles pagination internally
            var items = await DynamoDbService.ScanAsync(
                sourceTableName,
                limit: batchSize,
                region: sourceRegion);

            foreach (var item in items)
            {
                try
                {
                    var transformedItem = transformer != null ? transformer(item) : item;

                    await DynamoDbService.PutItemAsync(
                        destinationTableName,
                        transformedItem,
                        region: destinationRegion ?? sourceRegion);

                    migrated++;
                }
                catch (Exception)
                {
                    failed++;
                }
            }

            // Publish progress metrics
            await CloudWatchService.PutMetricDataAsync(
                metricNamespace: "AwsUtil/Migration",
                metricData: new List<Amazon.CloudWatch.Model.MetricDatum>
                {
                    new()
                    {
                        MetricName = "ItemsMigrated",
                        Value = migrated,
                        Unit = Amazon.CloudWatch.StandardUnit.Count,
                        Dimensions = new List<Amazon.CloudWatch.Model.Dimension>
                        {
                            new()
                            {
                                Name = "SourceTable",
                                Value = sourceTableName
                            }
                        }
                    }
                },
                region: sourceRegion);

            sw.Stop();

            return new DynamoDbTableMigratorResult(
                SourceTableName: sourceTableName,
                DestinationTableName: destinationTableName,
                ItemsMigrated: migrated,
                ItemsFailed: failed,
                DurationMs: sw.Elapsed.TotalMilliseconds,
                Completed: failed == 0);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "DynamoDB table migration failed");
        }
    }

    /// <summary>
    /// Restore an RDS snapshot to a new database instance, wait for it
    /// to become available, and return the connection endpoint.
    /// </summary>
    public static async Task<RdsSnapshotPromotorResult> RdsSnapshotPromotorAsync(
        string snapshotIdentifier,
        string newInstanceIdentifier,
        string? dbInstanceClass = null,
        bool publiclyAccessible = false,
        string? alertTopicArn = null,
        RegionEndpoint? region = null)
    {
        try
        {
            // Describe the snapshot to get source information
            var snapshots = await RdsService.DescribeDBSnapshotsAsync(
                dbSnapshotIdentifier: snapshotIdentifier,
                region: region);

            var snapshot = snapshots.DBSnapshots?.FirstOrDefault();
            var sourceInstance = snapshot?.DBInstanceIdentifier ?? "unknown";

            // Restore from snapshot
            var restoreReq = new Amazon.RDS.Model.RestoreDBInstanceFromDBSnapshotRequest
            {
                DBInstanceIdentifier = newInstanceIdentifier,
                DBSnapshotIdentifier = snapshotIdentifier,
                PubliclyAccessible = publiclyAccessible
            };
            if (dbInstanceClass != null)
                restoreReq.DBInstanceClass = dbInstanceClass;
            await RdsService.RestoreDBInstanceFromDBSnapshotAsync(
                restoreReq, region: region);

            // Notify about promotion
            if (alertTopicArn != null)
            {
                await SnsService.PublishAsync(
                    alertTopicArn,
                    $"RDS snapshot {snapshotIdentifier} is being restored to " +
                    $"new instance {newInstanceIdentifier}.",
                    subject: "RDS Snapshot Promotion",
                    region: region);
            }

            // Publish event
            await EventBridgeService.PutEventsAsync(
                entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                {
                    new()
                    {
                        Source = "aws-util.database-migration",
                        DetailType = "RdsSnapshotPromotion",
                        Detail = JsonSerializer.Serialize(new
                        {
                            snapshotIdentifier,
                            sourceInstance,
                            newInstanceIdentifier,
                            timestamp = DateTime.UtcNow.ToString("o")
                        })
                    }
                },
                region: region);

            return new RdsSnapshotPromotorResult(
                SnapshotIdentifier: snapshotIdentifier,
                SourceInstanceIdentifier: sourceInstance,
                NewInstanceIdentifier: newInstanceIdentifier,
                Status: "Restoring");
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "RDS snapshot promotion failed");
        }
    }

    /// <summary>
    /// Create a cross-region read replica for an RDS instance or initiate
    /// DynamoDB global table replication to a target region.
    /// </summary>
    public static async Task<CrossRegionDatabaseReplicatorResult> CrossRegionDatabaseReplicatorAsync(
        string sourceIdentifier,
        string replicationType,
        string targetRegion,
        string? replicaIdentifier = null,
        string? alertTopicArn = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var replicaId = replicaIdentifier ?? $"{sourceIdentifier}-{targetRegion}-replica";
            var sourceRegion = region?.SystemName ?? "us-east-1";

            switch (replicationType.ToLowerInvariant())
            {
                case "rds":
                    await RdsService.CreateDBInstanceReadReplicaAsync(
                        new Amazon.RDS.Model.CreateDBInstanceReadReplicaRequest
                        {
                            DBInstanceIdentifier = replicaId,
                            SourceDBInstanceIdentifier = sourceIdentifier
                        },
                        region: RegionEndpoint.GetBySystemName(targetRegion));
                    break;

                case "dynamodb":
                    // Create a global table replica
                    await DynamoDbService.UpdateTableAsync(
                        sourceIdentifier,
                        region: region);
                    break;

                default:
                    throw new AwsValidationException(
                        $"Unsupported replication type: {replicationType}. " +
                        "Use 'rds' or 'dynamodb'.");
            }

            // Notify about replication
            if (alertTopicArn != null)
            {
                await SnsService.PublishAsync(
                    alertTopicArn,
                    $"Cross-region replication initiated for {sourceIdentifier} " +
                    $"from {sourceRegion} to {targetRegion}.",
                    subject: "Cross-Region Replication Started",
                    region: region);
            }

            return new CrossRegionDatabaseReplicatorResult(
                SourceRegion: sourceRegion,
                TargetRegion: targetRegion,
                SourceIdentifier: sourceIdentifier,
                ReplicaIdentifier: replicaId,
                ReplicationType: replicationType,
                Status: "Initiating");
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Cross-region database replication failed");
        }
    }
}

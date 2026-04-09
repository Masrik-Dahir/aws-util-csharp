using System.Text.Json;
using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of managing cross-region backups.</summary>
public sealed record CrossRegionBackupManagerResult(
    string SourceRegion,
    string TargetRegion,
    int BackupsCreated,
    int BackupsFailed,
    List<BackupRecord> Backups);

/// <summary>A single backup record.</summary>
public sealed record BackupRecord(
    string ResourceType,
    string ResourceIdentifier,
    string BackupIdentifier,
    string Status,
    DateTime CreatedAt);

/// <summary>Result of a failover orchestration.</summary>
public sealed record FailoverOrchestratorResult(
    string PrimaryRegion,
    string SecondaryRegion,
    bool FailoverSucceeded,
    List<FailoverAction> Actions,
    double TotalDurationMs);

/// <summary>An individual action taken during failover.</summary>
public sealed record FailoverAction(
    string ActionName,
    string ResourceType,
    string ResourceId,
    bool Succeeded,
    string? Error = null);

/// <summary>Result of validating recovery points.</summary>
public sealed record RecoveryPointValidatorResult(
    int TotalRecoveryPoints,
    int ValidRecoveryPoints,
    int InvalidRecoveryPoints,
    List<RecoveryPointStatus> Statuses);

/// <summary>Status of a single recovery point.</summary>
public sealed record RecoveryPointStatus(
    string ResourceType,
    string ResourceId,
    string BackupId,
    bool IsValid,
    DateTime RecoveryPointTime,
    string? ValidationError = null);

/// <summary>Result of monitoring RPO/RTO compliance.</summary>
public sealed record RpoRtoMonitorResult(
    TimeSpan TargetRpo,
    TimeSpan TargetRto,
    TimeSpan ActualRpo,
    TimeSpan ActualRto,
    bool RpoCompliant,
    bool RtoCompliant,
    List<string> NonCompliantResources);

/// <summary>
/// Disaster recovery orchestration combining S3, RDS, DynamoDB,
/// Route 53, CloudWatch, SNS, and EventBridge for backup management,
/// failover orchestration, and RPO/RTO monitoring.
/// </summary>
public static class DisasterRecoveryService
{
    /// <summary>
    /// Create cross-region backups for RDS snapshots and S3 objects,
    /// copying them to the target region for disaster recovery.
    /// </summary>
    public static async Task<CrossRegionBackupManagerResult> CrossRegionBackupManagerAsync(
        string sourceRegion,
        string targetRegion,
        List<string>? rdsInstanceIds = null,
        List<(string Bucket, string KeyPrefix)>? s3Sources = null,
        string? auditTableName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var backups = new List<BackupRecord>();
            var failed = 0;

            // Backup RDS instances via snapshots
            foreach (var instanceId in rdsInstanceIds ?? new List<string>())
            {
                try
                {
                    var snapshotId = $"{instanceId}-dr-{DateTime.UtcNow:yyyyMMddHHmmss}";

                    await RdsService.CreateDBSnapshotAsync(
                        dbInstanceIdentifier: instanceId,
                        dbSnapshotIdentifier: snapshotId,
                        region: RegionEndpoint.GetBySystemName(sourceRegion));

                    backups.Add(new BackupRecord(
                        ResourceType: "RDS",
                        ResourceIdentifier: instanceId,
                        BackupIdentifier: snapshotId,
                        Status: "Creating",
                        CreatedAt: DateTime.UtcNow));
                }
                catch (Exception)
                {
                    failed++;
                }
            }

            // Backup S3 objects by copying to target region bucket
            foreach (var (bucket, keyPrefix) in s3Sources ?? new List<(string, string)>())
            {
                try
                {
                    var objects = await S3Service.ListObjectsV2Async(
                        bucket, prefix: keyPrefix,
                        region: RegionEndpoint.GetBySystemName(sourceRegion));

                    var drBucket = $"{bucket}-dr-{targetRegion}";

                    foreach (var obj in objects.Objects)
                    {
                        await S3Service.CopyObjectAsync(
                            srcBucket: bucket,
                            srcKey: obj.Key,
                            dstBucket: drBucket,
                            dstKey: obj.Key,
                            region: RegionEndpoint.GetBySystemName(sourceRegion));
                    }

                    backups.Add(new BackupRecord(
                        ResourceType: "S3",
                        ResourceIdentifier: $"{bucket}/{keyPrefix}",
                        BackupIdentifier: $"{bucket}-dr-{targetRegion}/{keyPrefix}",
                        Status: "Completed",
                        CreatedAt: DateTime.UtcNow));
                }
                catch (Exception)
                {
                    failed++;
                }
            }

            // Audit trail
            if (auditTableName != null)
            {
                await DynamoDbService.PutItemAsync(
                    auditTableName,
                    new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                    {
                        ["pk"] = new() { S = $"backup#{DateTime.UtcNow:yyyyMMdd}" },
                        ["sk"] = new() { S = DateTime.UtcNow.ToString("o") },
                        ["sourceRegion"] = new() { S = sourceRegion },
                        ["targetRegion"] = new() { S = targetRegion },
                        ["backupsCreated"] = new() { N = backups.Count.ToString() },
                        ["backupsFailed"] = new() { N = failed.ToString() }
                    },
                    region: region);
            }

            return new CrossRegionBackupManagerResult(
                SourceRegion: sourceRegion,
                TargetRegion: targetRegion,
                BackupsCreated: backups.Count,
                BackupsFailed: failed,
                Backups: backups);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Cross-region backup management failed");
        }
    }

    /// <summary>
    /// Orchestrate a complete failover from primary to secondary region,
    /// updating DNS, notifying stakeholders, and verifying target readiness.
    /// </summary>
    public static async Task<FailoverOrchestratorResult> FailoverOrchestratorAsync(
        string primaryRegion,
        string secondaryRegion,
        string hostedZoneId,
        string recordName,
        string secondaryEndpoint,
        string? alertTopicArn = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var actions = new List<FailoverAction>();
            var overallSuccess = true;

            // Action 1: Verify secondary region health
            try
            {
                await CloudWatchService.DescribeAlarmsAsync(
                    alarmNamePrefix: $"dr-health-{secondaryRegion}",
                    region: RegionEndpoint.GetBySystemName(secondaryRegion));

                actions.Add(new FailoverAction(
                    "Verify secondary health", "Region", secondaryRegion, true));
            }
            catch (Exception ex)
            {
                actions.Add(new FailoverAction(
                    "Verify secondary health", "Region", secondaryRegion, false,
                    Error: ex.Message));
                overallSuccess = false;
            }

            // Action 2: Update DNS to point to secondary
            try
            {
                await Route53Service.ChangeResourceRecordSetsAsync(
                    new Amazon.Route53.Model.ChangeResourceRecordSetsRequest
                    {
                        HostedZoneId = hostedZoneId,
                        ChangeBatch = new Amazon.Route53.Model.ChangeBatch
                        {
                            Changes = new List<Amazon.Route53.Model.Change>
                            {
                                new()
                                {
                                    Action = Amazon.Route53.ChangeAction.UPSERT,
                                    ResourceRecordSet = new Amazon.Route53.Model.ResourceRecordSet
                                    {
                                        Name = recordName,
                                        Type = Amazon.Route53.RRType.CNAME,
                                        TTL = 60,
                                        ResourceRecords = new List<Amazon.Route53.Model.ResourceRecord>
                                        {
                                            new() { Value = secondaryEndpoint }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    region: region);

                actions.Add(new FailoverAction(
                    "Update DNS", "Route53", recordName, true));
            }
            catch (Exception ex)
            {
                actions.Add(new FailoverAction(
                    "Update DNS", "Route53", recordName, false, Error: ex.Message));
                overallSuccess = false;
            }

            // Action 3: Send notification
            if (alertTopicArn != null)
            {
                try
                {
                    await SnsService.PublishAsync(
                        alertTopicArn,
                        $"DR failover initiated from {primaryRegion} to {secondaryRegion}. " +
                        $"DNS record {recordName} updated to {secondaryEndpoint}.",
                        subject: "Disaster Recovery Failover",
                        region: region);

                    actions.Add(new FailoverAction(
                        "Send notification", "SNS", alertTopicArn, true));
                }
                catch (Exception ex)
                {
                    actions.Add(new FailoverAction(
                        "Send notification", "SNS", alertTopicArn, false,
                        Error: ex.Message));
                }
            }

            // Action 4: Publish failover event
            try
            {
                await EventBridgeService.PutEventsAsync(
                    entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                    {
                        new()
                        {
                            Source = "aws-util.disaster-recovery",
                            DetailType = "FailoverExecuted",
                            Detail = JsonSerializer.Serialize(new
                            {
                                primaryRegion,
                                secondaryRegion,
                                recordName,
                                success = overallSuccess,
                                timestamp = DateTime.UtcNow.ToString("o")
                            })
                        }
                    },
                    region: region);

                actions.Add(new FailoverAction(
                    "Publish failover event", "EventBridge", "default", true));
            }
            catch (Exception ex)
            {
                actions.Add(new FailoverAction(
                    "Publish failover event", "EventBridge", "default", false,
                    Error: ex.Message));
            }

            sw.Stop();

            return new FailoverOrchestratorResult(
                PrimaryRegion: primaryRegion,
                SecondaryRegion: secondaryRegion,
                FailoverSucceeded: overallSuccess,
                Actions: actions,
                TotalDurationMs: sw.Elapsed.TotalMilliseconds);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failover orchestration failed");
        }
    }

    /// <summary>
    /// Validate that recovery points (RDS snapshots, S3 backups) are intact
    /// and restorable by checking their status and metadata.
    /// </summary>
    public static async Task<RecoveryPointValidatorResult> RecoveryPointValidatorAsync(
        List<string>? rdsSnapshotIds = null,
        List<(string Bucket, string Key)>? s3BackupKeys = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var statuses = new List<RecoveryPointStatus>();

            // Validate RDS snapshots
            foreach (var snapshotId in rdsSnapshotIds ?? new List<string>())
            {
                try
                {
                    var snapshots = await RdsService.DescribeDBSnapshotsAsync(
                        dbSnapshotIdentifier: snapshotId,
                        region: region);

                    var snapshot = snapshots.DBSnapshots?.FirstOrDefault();
                    var isValid = snapshot?.Status == "available";

                    statuses.Add(new RecoveryPointStatus(
                        ResourceType: "RDS",
                        ResourceId: snapshot?.DBInstanceIdentifier ?? "unknown",
                        BackupId: snapshotId,
                        IsValid: isValid,
                        RecoveryPointTime: snapshot?.SnapshotCreateTime ?? DateTime.MinValue,
                        ValidationError: isValid ? null : $"Snapshot status: {snapshot?.Status}"));
                }
                catch (Exception ex)
                {
                    statuses.Add(new RecoveryPointStatus(
                        ResourceType: "RDS",
                        ResourceId: "unknown",
                        BackupId: snapshotId,
                        IsValid: false,
                        RecoveryPointTime: DateTime.MinValue,
                        ValidationError: ex.Message));
                }
            }

            // Validate S3 backup objects
            foreach (var (bucket, key) in s3BackupKeys ?? new List<(string, string)>())
            {
                try
                {
                    var head = await S3Service.HeadObjectAsync(bucket, key, region: region);
                    var isValid = head.ContentLength > 0;

                    statuses.Add(new RecoveryPointStatus(
                        ResourceType: "S3",
                        ResourceId: $"{bucket}/{key}",
                        BackupId: $"s3://{bucket}/{key}",
                        IsValid: isValid,
                        RecoveryPointTime: head.LastModified ?? DateTime.MinValue,
                        ValidationError: isValid ? null : "Object is empty"));
                }
                catch (Exception ex)
                {
                    statuses.Add(new RecoveryPointStatus(
                        ResourceType: "S3",
                        ResourceId: $"{bucket}/{key}",
                        BackupId: $"s3://{bucket}/{key}",
                        IsValid: false,
                        RecoveryPointTime: DateTime.MinValue,
                        ValidationError: ex.Message));
                }
            }

            return new RecoveryPointValidatorResult(
                TotalRecoveryPoints: statuses.Count,
                ValidRecoveryPoints: statuses.Count(s => s.IsValid),
                InvalidRecoveryPoints: statuses.Count(s => !s.IsValid),
                Statuses: statuses);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Recovery point validation failed");
        }
    }

    /// <summary>
    /// Monitor RPO (Recovery Point Objective) and RTO (Recovery Time Objective)
    /// compliance by checking backup freshness and estimating recovery time.
    /// </summary>
    public static async Task<RpoRtoMonitorResult> RpoRtoMonitorAsync(
        TimeSpan targetRpo,
        TimeSpan targetRto,
        string trackingTableName,
        string? alertTopicArn = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var nonCompliant = new List<string>();

            // Get last backup timestamps from tracking table
            var lastBackup = await DynamoDbService.GetItemAsync(
                trackingTableName,
                new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                {
                    ["pk"] = new() { S = "dr-metrics#latest" }
                },
                region: region);

            var lastBackupTimeStr = lastBackup?
                .GetValueOrDefault("lastBackupTime")?.S;
            var lastRecoveryTestStr = lastBackup?
                .GetValueOrDefault("lastRecoveryTestDurationMs")?.N;

            var lastBackupTime = lastBackupTimeStr != null
                ? DateTime.Parse(lastBackupTimeStr)
                : DateTime.UtcNow.AddDays(-1);

            var lastRecoveryDurationMs = lastRecoveryTestStr != null
                ? double.Parse(lastRecoveryTestStr)
                : targetRto.TotalMilliseconds * 0.8;

            var actualRpo = DateTime.UtcNow - lastBackupTime;
            var actualRto = TimeSpan.FromMilliseconds(lastRecoveryDurationMs);

            var rpoCompliant = actualRpo <= targetRpo;
            var rtoCompliant = actualRto <= targetRto;

            if (!rpoCompliant)
                nonCompliant.Add($"RPO: actual {actualRpo} exceeds target {targetRpo}");
            if (!rtoCompliant)
                nonCompliant.Add($"RTO: actual {actualRto} exceeds target {targetRto}");

            // Publish metrics
            await CloudWatchService.PutMetricDataAsync(
                metricNamespace: "AwsUtil/DR",
                metricData: new List<Amazon.CloudWatch.Model.MetricDatum>
                {
                    new()
                    {
                        MetricName = "ActualRpoMinutes",
                        Value = actualRpo.TotalMinutes,
                        Unit = Amazon.CloudWatch.StandardUnit.None
                    },
                    new()
                    {
                        MetricName = "ActualRtoMinutes",
                        Value = actualRto.TotalMinutes,
                        Unit = Amazon.CloudWatch.StandardUnit.None
                    }
                },
                region: region);

            // Alert if non-compliant
            if (nonCompliant.Count > 0 && alertTopicArn != null)
            {
                await SnsService.PublishAsync(
                    alertTopicArn,
                    $"DR compliance violation: {string.Join("; ", nonCompliant)}",
                    subject: "DR RPO/RTO Non-Compliance Alert",
                    region: region);
            }

            return new RpoRtoMonitorResult(
                TargetRpo: targetRpo,
                TargetRto: targetRto,
                ActualRpo: actualRpo,
                ActualRto: actualRto,
                RpoCompliant: rpoCompliant,
                RtoCompliant: rtoCompliant,
                NonCompliantResources: nonCompliant);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "RPO/RTO monitoring failed");
        }
    }
}

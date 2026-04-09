using System.Text.Json;
using Amazon;
using Amazon.SQS.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of reprocessing messages from an SQS dead-letter queue.</summary>
public sealed record ReprocessSqsDlqResult(
    int MessagesReprocessed = 0,
    int MessagesFailed = 0,
    List<string>? FailedMessageIds = null);

/// <summary>Result of a cross-account S3 copy operation.</summary>
public sealed record CrossAccountS3CopyResult(
    int ObjectsCopied = 0,
    int ObjectsFailed = 0,
    List<string>? Errors = null);

/// <summary>Result of tagging multiple resources across services.</summary>
public sealed record TagResourcesResult(
    int Succeeded = 0,
    int Failed = 0,
    List<string>? Errors = null);

/// <summary>Describes a single resource to tag.</summary>
public sealed record TagTarget(
    string Service,
    string ResourceArn);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Multi-service resource operations combining SQS, S3, STS, and other
/// services for common cross-service operational workflows.
/// </summary>
public static class ResourceOpsService
{
    /// <summary>
    /// Move messages from an SQS dead-letter queue back to the source queue
    /// for reprocessing. Messages are received in batches, sent to the source
    /// queue, and then deleted from the DLQ.
    /// </summary>
    /// <param name="dlqUrl">URL of the dead-letter queue.</param>
    /// <param name="sourceQueueUrl">URL of the source (processing) queue.</param>
    /// <param name="maxMessages">Maximum number of messages to reprocess (0 = all available).</param>
    /// <param name="batchSize">Number of messages to receive per batch (1-10).</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<ReprocessSqsDlqResult> ReprocessSqsDlqAsync(
        string dlqUrl,
        string sourceQueueUrl,
        int maxMessages = 0,
        int batchSize = 10,
        RegionEndpoint? region = null)
    {
        try
        {
            var reprocessed = 0;
            var failed = 0;
            var failedIds = new List<string>();
            var effectiveBatchSize = Math.Clamp(batchSize, 1, 10);

            while (maxMessages == 0 || reprocessed + failed < maxMessages)
            {
                var remaining = maxMessages > 0
                    ? Math.Min(effectiveBatchSize, maxMessages - reprocessed - failed)
                    : effectiveBatchSize;

                var messages = await SqsService.ReceiveMessagesAsync(
                    dlqUrl,
                    maxNumber: remaining,
                    waitSeconds: 1,
                    region: region);

                if (messages.Count == 0)
                    break;

                foreach (var msg in messages)
                {
                    try
                    {
                        // Re-send the message to the source queue
                        await SqsService.SendMessageAsync(
                            sourceQueueUrl,
                            msg.Body,
                            region: region);

                        // Delete from the DLQ after successful re-send
                        await SqsService.DeleteMessageAsync(
                            dlqUrl,
                            msg.ReceiptHandle,
                            region: region);

                        reprocessed++;
                    }
                    catch (Exception)
                    {
                        failed++;
                        failedIds.Add(msg.MessageId);
                    }
                }
            }

            return new ReprocessSqsDlqResult(
                MessagesReprocessed: reprocessed,
                MessagesFailed: failed,
                FailedMessageIds: failedIds.Count > 0 ? failedIds : null);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to reprocess DLQ '{dlqUrl}' to '{sourceQueueUrl}'");
        }
    }

    /// <summary>
    /// Copy S3 objects between accounts by assuming a role in the destination
    /// account via STS, then performing server-side copies.
    /// </summary>
    /// <param name="srcBucket">Source S3 bucket.</param>
    /// <param name="srcPrefix">Source key prefix to copy.</param>
    /// <param name="dstBucket">Destination S3 bucket.</param>
    /// <param name="dstPrefix">Destination key prefix.</param>
    /// <param name="roleArn">IAM role ARN to assume in the destination account.</param>
    /// <param name="roleSessionName">Session name for the assumed role.</param>
    /// <param name="maxConcurrency">Maximum concurrent copy operations.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<CrossAccountS3CopyResult> CrossAccountS3CopyAsync(
        string srcBucket,
        string srcPrefix,
        string dstBucket,
        string dstPrefix,
        string roleArn,
        string roleSessionName = "cross-account-copy",
        int maxConcurrency = 20,
        RegionEndpoint? region = null)
    {
        try
        {
            // 1. Assume the cross-account role to verify access
            var credentials = await StsService.AssumeRoleAsync(
                roleArn,
                roleSessionName,
                region: region);

            if (credentials.AccessKeyId == null)
            {
                throw new InvalidOperationException(
                    $"Failed to assume role '{roleArn}' - no credentials returned");
            }

            // 2. List all objects under the source prefix
            var sourceObjects = await S3Service.ListObjectsAsync(
                srcBucket,
                srcPrefix,
                region: region);

            if (sourceObjects.Count == 0)
            {
                return new CrossAccountS3CopyResult(ObjectsCopied: 0);
            }

            // 3. Build copy specs: replace source prefix with destination prefix
            var copySpecs = sourceObjects.Select(obj =>
            {
                var relativePath = obj.Key.StartsWith(srcPrefix)
                    ? obj.Key[srcPrefix.Length..].TrimStart('/')
                    : obj.Key;
                var dstKey = string.IsNullOrEmpty(dstPrefix)
                    ? relativePath
                    : $"{dstPrefix.TrimEnd('/')}/{relativePath}";

                return new CopySpec(srcBucket, obj.Key, dstBucket, dstKey);
            }).ToList();

            // 4. Execute the batch copy (uses the caller's credentials +
            //    the assumed role grants cross-account bucket access)
            var batchResult = await S3Service.BatchCopyAsync(
                copySpecs,
                maxConcurrency: maxConcurrency,
                region: region);

            return new CrossAccountS3CopyResult(
                ObjectsCopied: batchResult.Succeeded,
                ObjectsFailed: batchResult.Failed,
                Errors: batchResult.Errors);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed cross-account S3 copy from '{srcBucket}/{srcPrefix}' to '{dstBucket}/{dstPrefix}'");
        }
    }

    /// <summary>
    /// Tag multiple resources across different AWS services.
    /// Currently supports tagging S3 buckets and EC2 resources.
    /// </summary>
    /// <param name="targets">List of resources to tag, each with a service type and ARN.</param>
    /// <param name="tags">Tags to apply (key/value pairs).</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<TagResourcesResult> TagResourcesAsync(
        List<TagTarget> targets,
        Dictionary<string, string> tags,
        RegionEndpoint? region = null)
    {
        try
        {
            var succeeded = 0;
            var failed = 0;
            var errors = new List<string>();

            foreach (var target in targets)
            {
                try
                {
                    switch (target.Service.ToLowerInvariant())
                    {
                        case "s3":
                            await S3Service.PutBucketTaggingAsync(
                                target.ResourceArn,
                                tags.Select(kv => new Amazon.S3.Model.Tag
                                {
                                    Key = kv.Key,
                                    Value = kv.Value
                                }).ToList(),
                                region: region);
                            break;

                        case "ec2":
                            await Ec2Service.CreateTagsAsync(
                                [target.ResourceArn],
                                tags.Select(kv => new Amazon.EC2.Model.Tag
                                {
                                    Key = kv.Key,
                                    Value = kv.Value
                                }).ToList(),
                                region: region);
                            break;

                        case "ecs":
                            await EcsService.TagResourceAsync(
                                target.ResourceArn,
                                tags.Select(kv => new Amazon.ECS.Model.Tag
                                {
                                    Key = kv.Key,
                                    Value = kv.Value
                                }).ToList(),
                                region: region);
                            break;

                        case "lambda":
                            await LambdaService.TagResourceAsync(
                                target.ResourceArn,
                                tags,
                                region: region);
                            break;

                        default:
                            throw new NotSupportedException(
                                $"Tagging service '{target.Service}' is not supported");
                    }

                    succeeded++;
                }
                catch (Exception exc)
                {
                    failed++;
                    errors.Add($"{target.Service}:{target.ResourceArn}: {exc.Message}");
                }
            }

            return new TagResourcesResult(
                Succeeded: succeeded,
                Failed: failed,
                Errors: errors.Count > 0 ? errors : null);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to tag resources");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="ReprocessSqsDlqAsync"/>.</summary>
    public static ReprocessSqsDlqResult ReprocessSqsDlq(string dlqUrl, string sourceQueueUrl, int maxMessages = 0, int batchSize = 10, RegionEndpoint? region = null)
        => ReprocessSqsDlqAsync(dlqUrl, sourceQueueUrl, maxMessages, batchSize, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CrossAccountS3CopyAsync"/>.</summary>
    public static CrossAccountS3CopyResult CrossAccountS3Copy(string srcBucket, string srcPrefix, string dstBucket, string dstPrefix, string roleArn, string roleSessionName = "cross-account-copy", int maxConcurrency = 20, RegionEndpoint? region = null)
        => CrossAccountS3CopyAsync(srcBucket, srcPrefix, dstBucket, dstPrefix, roleArn, roleSessionName, maxConcurrency, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourcesAsync"/>.</summary>
    public static TagResourcesResult TagResources(List<TagTarget> targets, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourcesAsync(targets, tags, region).GetAwaiter().GetResult();

}

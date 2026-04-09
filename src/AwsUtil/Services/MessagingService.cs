using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.DynamoDBv2.Model;
using Amazon.SimpleNotificationService.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of sending to multiple notification channels.</summary>
public sealed record MultiChannelNotifierResult(
    List<ChannelDelivery> Deliveries,
    int SuccessCount,
    int FailureCount)
{
    /// <summary>True when every channel delivery succeeded.</summary>
    public bool AllSucceeded => FailureCount == 0;
}

/// <summary>Delivery status for a single channel.</summary>
public sealed record ChannelDelivery(
    string Channel,
    string Destination,
    bool Success,
    string? MessageId = null,
    string? Error = null);

/// <summary>Result of deduplication check.</summary>
public sealed record EventDeduplicatorResult(
    string EventId,
    string DeduplicationKey,
    bool IsDuplicate,
    DateTime? FirstSeenAt = null);

/// <summary>Result of managing an SNS filter policy.</summary>
public sealed record SnsFilterPolicyManagerResult(
    string SubscriptionArn,
    Dictionary<string, List<string>> AppliedPolicy,
    bool Updated);

/// <summary>Result of sequencing messages in an SQS FIFO queue.</summary>
public sealed record SqsFifoSequencerResult(
    string QueueUrl,
    string MessageGroupId,
    List<string> MessageIds,
    int MessagesSent);

/// <summary>Result of digesting batch notifications.</summary>
public sealed record BatchNotificationDigesterResult(
    string TopicArn,
    int OriginalMessageCount,
    int DigestMessageCount,
    List<string> DigestMessageIds);

/// <summary>
/// Multi-channel messaging orchestration across SNS, SQS, SES, DynamoDB,
/// and EventBridge for notification delivery, deduplication, and sequencing.
/// </summary>
public static class MessagingService
{
    /// <summary>
    /// Send a notification to multiple channels (SNS, SQS, SES) in parallel
    /// and collect delivery results.
    /// </summary>
    public static async Task<MultiChannelNotifierResult> MultiChannelNotifierAsync(
        string subject,
        string message,
        List<string>? snsTopicArns = null,
        List<string>? sqsQueueUrls = null,
        List<(string From, string To)>? emailTargets = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var tasks = new List<Task<ChannelDelivery>>();

            // SNS topics
            foreach (var arn in snsTopicArns ?? Enumerable.Empty<string>())
            {
                var capturedArn = arn;
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var result = await SnsService.PublishAsync(
                            capturedArn, message, subject: subject, region: region);
                        return new ChannelDelivery(
                            "sns", capturedArn, true, MessageId: result.MessageId);
                    }
                    catch (Exception ex)
                    {
                        return new ChannelDelivery(
                            "sns", capturedArn, false, Error: ex.Message);
                    }
                }));
            }

            // SQS queues
            foreach (var url in sqsQueueUrls ?? Enumerable.Empty<string>())
            {
                var capturedUrl = url;
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var result = await SqsService.SendMessageAsync(
                            capturedUrl, message, region: region);
                        return new ChannelDelivery(
                            "sqs", capturedUrl, true, MessageId: result.MessageId);
                    }
                    catch (Exception ex)
                    {
                        return new ChannelDelivery(
                            "sqs", capturedUrl, false, Error: ex.Message);
                    }
                }));
            }

            // SES email
            foreach (var (from, to) in emailTargets ?? Enumerable.Empty<(string, string)>())
            {
                var capturedFrom = from;
                var capturedTo = to;
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var result = await SesService.SendEmailAsync(
                            capturedFrom,
                            new Amazon.SimpleEmail.Model.Destination
                            {
                                ToAddresses = new List<string> { capturedTo }
                            },
                            new Amazon.SimpleEmail.Model.Message
                            {
                                Subject = new Amazon.SimpleEmail.Model.Content(subject),
                                Body = new Amazon.SimpleEmail.Model.Body
                                {
                                    Text = new Amazon.SimpleEmail.Model.Content(message)
                                }
                            },
                            region: region);
                        return new ChannelDelivery(
                            "ses", capturedTo, true, MessageId: result.MessageId);
                    }
                    catch (Exception ex)
                    {
                        return new ChannelDelivery(
                            "ses", capturedTo, false, Error: ex.Message);
                    }
                }));
            }

            var deliveries = (await Task.WhenAll(tasks)).ToList();

            return new MultiChannelNotifierResult(
                Deliveries: deliveries,
                SuccessCount: deliveries.Count(d => d.Success),
                FailureCount: deliveries.Count(d => !d.Success));
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Multi-channel notification failed");
        }
    }

    /// <summary>
    /// Check if an event has already been processed using a DynamoDB-based
    /// deduplication table. If not a duplicate, records the event.
    /// </summary>
    public static async Task<EventDeduplicatorResult> EventDeduplicatorAsync(
        string tableName,
        string eventId,
        string? deduplicationKey = null,
        int ttlSeconds = 86400,
        RegionEndpoint? region = null)
    {
        try
        {
            var dedupKey = deduplicationKey ?? ComputeSha256(eventId);
            var pk = $"dedup#{dedupKey}";
            var expiresAt = DateTimeOffset.UtcNow.AddSeconds(ttlSeconds).ToUnixTimeSeconds();

            // Try conditional put: only succeeds if not already present
            try
            {
                await DynamoDbService.PutItemAsync(
                    tableName,
                    new Dictionary<string, AttributeValue>
                    {
                        ["pk"] = new() { S = pk },
                        ["eventId"] = new() { S = eventId },
                        ["firstSeenAt"] = new() { S = DateTime.UtcNow.ToString("o") },
                        ["ttl"] = new() { N = expiresAt.ToString() }
                    },
                    conditionExpression: "attribute_not_exists(pk)",
                    region: region);

                return new EventDeduplicatorResult(
                    EventId: eventId,
                    DeduplicationKey: dedupKey,
                    IsDuplicate: false,
                    FirstSeenAt: DateTime.UtcNow);
            }
            catch (AwsConflictException)
            {
                // Item already exists — this is a duplicate
                var existing = await DynamoDbService.GetItemAsync(
                    tableName,
                    new Dictionary<string, AttributeValue>
                    {
                        ["pk"] = new() { S = pk }
                    },
                    region: region);

                var firstSeen = existing?.GetValueOrDefault("firstSeenAt")?.S;

                return new EventDeduplicatorResult(
                    EventId: eventId,
                    DeduplicationKey: dedupKey,
                    IsDuplicate: true,
                    FirstSeenAt: firstSeen != null ? DateTime.Parse(firstSeen) : null);
            }
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Event deduplication failed");
        }
    }

    /// <summary>
    /// Set or update the filter policy on an SNS subscription.
    /// </summary>
    public static async Task<SnsFilterPolicyManagerResult> SnsFilterPolicyManagerAsync(
        string subscriptionArn,
        Dictionary<string, List<string>> filterPolicy,
        RegionEndpoint? region = null)
    {
        try
        {
            var policyJson = JsonSerializer.Serialize(filterPolicy);

            await SnsService.SetSubscriptionAttributesAsync(
                subscriptionArn,
                attributeName: "FilterPolicy",
                attributeValue: policyJson,
                region: region);

            return new SnsFilterPolicyManagerResult(
                SubscriptionArn: subscriptionArn,
                AppliedPolicy: filterPolicy,
                Updated: true);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to manage SNS filter policy");
        }
    }

    /// <summary>
    /// Send a sequence of messages to an SQS FIFO queue with proper message
    /// group ID and deduplication IDs.
    /// </summary>
    public static async Task<SqsFifoSequencerResult> SqsFifoSequencerAsync(
        string queueUrl,
        string messageGroupId,
        List<string> messages,
        RegionEndpoint? region = null)
    {
        try
        {
            var messageIds = new List<string>();

            for (var i = 0; i < messages.Count; i++)
            {
                var dedupId = $"{messageGroupId}-{DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}-{i}";

                var result = await SqsService.SendMessageAsync(
                    queueUrl,
                    messages[i],
                    messageGroupId: messageGroupId,
                    messageDeduplicationId: dedupId,
                    region: region);

                if (result.MessageId != null)
                    messageIds.Add(result.MessageId);
            }

            return new SqsFifoSequencerResult(
                QueueUrl: queueUrl,
                MessageGroupId: messageGroupId,
                MessageIds: messageIds,
                MessagesSent: messageIds.Count);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "FIFO message sequencing failed");
        }
    }

    /// <summary>
    /// Batch individual notification messages into digest summaries and publish
    /// them to an SNS topic, reducing notification noise.
    /// </summary>
    public static async Task<BatchNotificationDigesterResult> BatchNotificationDigesterAsync(
        string topicArn,
        List<string> messages,
        int batchSize = 10,
        string digestSubject = "Notification Digest",
        RegionEndpoint? region = null)
    {
        try
        {
            var digestIds = new List<string>();
            var batches = messages
                .Select((m, i) => new { Message = m, Index = i })
                .GroupBy(x => x.Index / batchSize)
                .Select(g => g.Select(x => x.Message).ToList())
                .ToList();

            foreach (var batch in batches)
            {
                var digestBody = new StringBuilder();
                digestBody.AppendLine($"Digest ({batch.Count} messages):");
                digestBody.AppendLine(new string('-', 40));

                for (var i = 0; i < batch.Count; i++)
                {
                    digestBody.AppendLine($"[{i + 1}] {batch[i]}");
                    digestBody.AppendLine();
                }

                var result = await SnsService.PublishAsync(
                    topicArn,
                    digestBody.ToString(),
                    subject: digestSubject,
                    region: region);

                if (result.MessageId != null)
                    digestIds.Add(result.MessageId);
            }

            return new BatchNotificationDigesterResult(
                TopicArn: topicArn,
                OriginalMessageCount: messages.Count,
                DigestMessageCount: digestIds.Count,
                DigestMessageIds: digestIds);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Batch notification digest failed");
        }
    }

    private static string ComputeSha256(string input)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexStringLower(bytes);
    }
}

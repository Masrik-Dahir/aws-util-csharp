using System.Text.Json;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result / model records
// ---------------------------------------------------------------------------

/// <summary>
/// A message received from an SQS queue.
/// </summary>
public sealed record SqsMessage(
    string MessageId,
    string ReceiptHandle,
    string Body,
    Dictionary<string, string>? Attributes = null,
    Dictionary<string, MessageAttributeValue>? MessageAttributes = null)
{
    /// <summary>
    /// Deserialise the message body as JSON.
    /// </summary>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns>The deserialised value.</returns>
    /// <exception cref="JsonException">If the body is not valid JSON.</exception>
    public T? BodyAsJson<T>() => JsonSerializer.Deserialize<T>(Body);

    /// <summary>
    /// Deserialise the message body as a <see cref="JsonElement"/>.
    /// </summary>
    public JsonElement BodyAsJson() => JsonSerializer.Deserialize<JsonElement>(Body);
}

/// <summary>Result of a successful <c>SendMessage</c> call.</summary>
public sealed record SendMessageResult(string MessageId, string? SequenceNumber = null);

/// <summary>Result of a successful <c>ReceiveMessage</c> call (raw SDK wrapper).</summary>
public sealed record ReceiveMessageResult(List<Message>? Messages = null);

/// <summary>Result of a successful <c>CreateQueue</c> call.</summary>
public sealed record CreateQueueResult(string? QueueUrl = null);

/// <summary>Result of <c>SendMessageBatch</c>.</summary>
public sealed record SendMessageBatchResult(
    List<SendMessageBatchResultEntry>? Successful = null,
    List<BatchResultErrorEntry>? Failed = null);

/// <summary>Result of <c>DeleteMessageBatch</c>.</summary>
public sealed record DeleteMessageBatchResult(
    List<DeleteMessageBatchResultEntry>? Successful = null,
    List<BatchResultErrorEntry>? Failed = null);

/// <summary>Result of <c>ChangeMessageVisibilityBatch</c>.</summary>
public sealed record ChangeMessageVisibilityBatchResult(
    List<ChangeMessageVisibilityBatchResultEntry>? Successful = null,
    List<BatchResultErrorEntry>? Failed = null);

/// <summary>Result of <c>ListQueues</c>.</summary>
public sealed record ListQueuesResult(List<string>? QueueUrls = null, string? NextToken = null);

/// <summary>Result of <c>ListQueueTags</c>.</summary>
public sealed record ListQueueTagsResult(Dictionary<string, string>? Tags = null);

/// <summary>Result of <c>ListDeadLetterSourceQueues</c>.</summary>
public sealed record ListDeadLetterSourceQueuesResult(
    List<string>? QueueUrls = null,
    string? NextToken = null);

/// <summary>Result of <c>StartMessageMoveTask</c>.</summary>
public sealed record StartMessageMoveTaskResult(string? TaskHandle = null);

/// <summary>Result of <c>CancelMessageMoveTask</c>.</summary>
public sealed record CancelMessageMoveTaskResult(long? ApproximateNumberOfMessagesMoved = null);

/// <summary>Result of <c>ListMessageMoveTasks</c>.</summary>
public sealed record ListMessageMoveTasksResult(
    List<ListMessageMoveTasksResultEntry>? Results = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon SQS.
/// </summary>
public static class SqsService
{
    private static AmazonSQSClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSQSClient>(region);

    // -----------------------------------------------------------------------
    // Core: Send
    // -----------------------------------------------------------------------

    /// <summary>
    /// Send a single message to an SQS queue.
    /// Dicts and lists passed as <paramref name="body"/> are serialised to JSON automatically.
    /// </summary>
    public static async Task<SendMessageResult> SendMessageAsync(
        string queueUrl,
        object body,
        int delaySeconds = 0,
        string? messageGroupId = null,
        string? messageDeduplicationId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var rawBody = body is string s ? s : JsonSerializer.Serialize(body);
        var request = new SendMessageRequest
        {
            QueueUrl = queueUrl,
            MessageBody = rawBody,
            DelaySeconds = delaySeconds
        };
        if (messageGroupId != null)
            request.MessageGroupId = messageGroupId;
        if (messageDeduplicationId != null)
            request.MessageDeduplicationId = messageDeduplicationId;

        try
        {
            var resp = await client.SendMessageAsync(request);
            return new SendMessageResult(
                resp.MessageId,
                string.IsNullOrEmpty(resp.SequenceNumber) ? null : resp.SequenceNumber);
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to send message to '{queueUrl}'");
        }
    }

    /// <summary>
    /// Send a batch of messages using the raw SDK <c>SendMessageBatch</c> API.
    /// </summary>
    public static async Task<SendMessageBatchResult> SendMessageBatchAsync(
        string queueUrl,
        List<SendMessageBatchRequestEntry> entries,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.SendMessageBatchAsync(new SendMessageBatchRequest
            {
                QueueUrl = queueUrl,
                Entries = entries
            });
            return new SendMessageBatchResult(resp.Successful, resp.Failed);
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to send message batch to '{queueUrl}'");
        }
    }

    // -----------------------------------------------------------------------
    // Core: Receive
    // -----------------------------------------------------------------------

    /// <summary>
    /// Receive up to <paramref name="maxNumber"/> messages from a queue.
    /// Returns parsed <see cref="SqsMessage"/> instances.
    /// </summary>
    public static async Task<List<SqsMessage>> ReceiveMessagesAsync(
        string queueUrl,
        int maxNumber = 1,
        int waitSeconds = 0,
        int visibilityTimeout = 30,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ReceiveMessageAsync(new ReceiveMessageRequest
            {
                QueueUrl = queueUrl,
                MaxNumberOfMessages = maxNumber,
                WaitTimeSeconds = waitSeconds,
                VisibilityTimeout = visibilityTimeout,
                AttributeNames = ["All"],
                MessageAttributeNames = ["All"]
            });

            return (resp.Messages ?? [])
                .Select(m => new SqsMessage(
                    m.MessageId,
                    m.ReceiptHandle,
                    m.Body,
                    m.Attributes,
                    m.MessageAttributes))
                .ToList();
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to receive messages from '{queueUrl}'");
        }
    }

    // -----------------------------------------------------------------------
    // Core: Delete
    // -----------------------------------------------------------------------

    /// <summary>
    /// Delete (acknowledge) a single message from a queue.
    /// </summary>
    public static async Task DeleteMessageAsync(
        string queueUrl,
        string receiptHandle,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteMessageAsync(new DeleteMessageRequest
            {
                QueueUrl = queueUrl,
                ReceiptHandle = receiptHandle
            });
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete message from '{queueUrl}'");
        }
    }

    /// <summary>
    /// Delete a batch of messages using the raw SDK <c>DeleteMessageBatch</c> API.
    /// </summary>
    public static async Task<DeleteMessageBatchResult> DeleteMessageBatchAsync(
        string queueUrl,
        List<DeleteMessageBatchRequestEntry> entries,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteMessageBatchAsync(new DeleteMessageBatchRequest
            {
                QueueUrl = queueUrl,
                Entries = entries
            });
            return new DeleteMessageBatchResult(resp.Successful, resp.Failed);
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete message batch from '{queueUrl}'");
        }
    }

    // -----------------------------------------------------------------------
    // Core: Queue management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a new SQS queue.
    /// </summary>
    public static async Task<CreateQueueResult> CreateQueueAsync(
        string queueName,
        Dictionary<string, string>? attributes = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateQueueRequest { QueueName = queueName };
        if (attributes != null)
            request.Attributes = attributes;
        if (tags != null)
            request.Tags = tags;

        try
        {
            var resp = await client.CreateQueueAsync(request);
            return new CreateQueueResult(resp.QueueUrl);
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create queue '{queueName}'");
        }
    }

    /// <summary>
    /// Delete an SQS queue.
    /// </summary>
    public static async Task DeleteQueueAsync(
        string queueUrl,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteQueueAsync(new DeleteQueueRequest { QueueUrl = queueUrl });
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete queue '{queueUrl}'");
        }
    }

    /// <summary>
    /// Resolve the URL for an SQS queue by name.
    /// </summary>
    public static async Task<string> GetQueueUrlAsync(
        string queueName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetQueueUrlAsync(new GetQueueUrlRequest
            {
                QueueName = queueName
            });
            return resp.QueueUrl;
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to resolve URL for queue '{queueName}'");
        }
    }

    /// <summary>
    /// Fetch queue attributes such as message count and ARN.
    /// </summary>
    public static async Task<Dictionary<string, string>> GetQueueAttributesAsync(
        string queueUrl,
        List<string>? attributeNames = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetQueueAttributesAsync(new GetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                AttributeNames = attributeNames ?? ["All"]
            });
            return resp.Attributes;
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get queue attributes for '{queueUrl}'");
        }
    }

    /// <summary>
    /// Set queue attributes.
    /// </summary>
    public static async Task SetQueueAttributesAsync(
        string queueUrl,
        Dictionary<string, string> attributes,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.SetQueueAttributesAsync(new SetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                Attributes = attributes
            });
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to set queue attributes for '{queueUrl}'");
        }
    }

    /// <summary>
    /// Purge all messages from a queue.
    /// SQS enforces a 60-second cooldown between purges.
    /// </summary>
    public static async Task PurgeQueueAsync(
        string queueUrl,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PurgeQueueAsync(new PurgeQueueRequest { QueueUrl = queueUrl });
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to purge queue '{queueUrl}'");
        }
    }

    // -----------------------------------------------------------------------
    // Core: Visibility
    // -----------------------------------------------------------------------

    /// <summary>
    /// Change the visibility timeout for a single message.
    /// </summary>
    public static async Task ChangeMessageVisibilityAsync(
        string queueUrl,
        string receiptHandle,
        int visibilityTimeout,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ChangeMessageVisibilityAsync(new ChangeMessageVisibilityRequest
            {
                QueueUrl = queueUrl,
                ReceiptHandle = receiptHandle,
                VisibilityTimeout = visibilityTimeout
            });
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to change message visibility for '{queueUrl}'");
        }
    }

    /// <summary>
    /// Batch-change visibility timeouts for up to 10 messages.
    /// </summary>
    public static async Task<ChangeMessageVisibilityBatchResult> ChangeMessageVisibilityBatchAsync(
        string queueUrl,
        List<ChangeMessageVisibilityBatchRequestEntry> entries,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ChangeMessageVisibilityBatchAsync(
                new ChangeMessageVisibilityBatchRequest
                {
                    QueueUrl = queueUrl,
                    Entries = entries
                });
            return new ChangeMessageVisibilityBatchResult(resp.Successful, resp.Failed);
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to change message visibility batch for '{queueUrl}'");
        }
    }

    // -----------------------------------------------------------------------
    // Core: List / Tags / Permissions
    // -----------------------------------------------------------------------

    /// <summary>
    /// List SQS queues, optionally filtered by name prefix.
    /// </summary>
    public static async Task<ListQueuesResult> ListQueuesAsync(
        string? queueNamePrefix = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListQueuesRequest();
        if (queueNamePrefix != null)
            request.QueueNamePrefix = queueNamePrefix;
        if (nextToken != null)
            request.NextToken = nextToken;
        if (maxResults.HasValue)
            request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListQueuesAsync(request);
            return new ListQueuesResult(resp.QueueUrls, resp.NextToken);
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list queues");
        }
    }

    /// <summary>
    /// Tag an SQS queue.
    /// </summary>
    public static async Task TagQueueAsync(
        string queueUrl,
        Dictionary<string, string> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagQueueAsync(new TagQueueRequest
            {
                QueueUrl = queueUrl,
                Tags = tags
            });
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to tag queue '{queueUrl}'");
        }
    }

    /// <summary>
    /// Remove tags from an SQS queue.
    /// </summary>
    public static async Task UntagQueueAsync(
        string queueUrl,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagQueueAsync(new UntagQueueRequest
            {
                QueueUrl = queueUrl,
                TagKeys = tagKeys
            });
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to untag queue '{queueUrl}'");
        }
    }

    /// <summary>
    /// List all tags on an SQS queue.
    /// </summary>
    public static async Task<ListQueueTagsResult> ListQueueTagsAsync(
        string queueUrl,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListQueueTagsAsync(new ListQueueTagsRequest
            {
                QueueUrl = queueUrl
            });
            return new ListQueueTagsResult(resp.Tags);
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to list queue tags for '{queueUrl}'");
        }
    }

    /// <summary>
    /// Add a permission to an SQS queue policy.
    /// </summary>
    public static async Task AddPermissionAsync(
        string queueUrl,
        string label,
        List<string> awsAccountIds,
        List<string> actions,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AddPermissionAsync(new AddPermissionRequest
            {
                QueueUrl = queueUrl,
                Label = label,
                AWSAccountIds = awsAccountIds,
                Actions = actions
            });
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to add permission to '{queueUrl}'");
        }
    }

    /// <summary>
    /// Remove a permission from an SQS queue policy.
    /// </summary>
    public static async Task RemovePermissionAsync(
        string queueUrl,
        string label,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemovePermissionAsync(new RemovePermissionRequest
            {
                QueueUrl = queueUrl,
                Label = label
            });
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to remove permission from '{queueUrl}'");
        }
    }

    // -----------------------------------------------------------------------
    // Core: Dead-letter & message move tasks
    // -----------------------------------------------------------------------

    /// <summary>
    /// List queues that have the specified queue configured as their dead-letter queue.
    /// </summary>
    public static async Task<ListDeadLetterSourceQueuesResult> ListDeadLetterSourceQueuesAsync(
        string queueUrl,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDeadLetterSourceQueuesRequest { QueueUrl = queueUrl };
        if (nextToken != null)
            request.NextToken = nextToken;
        if (maxResults.HasValue)
            request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListDeadLetterSourceQueuesAsync(request);
            return new ListDeadLetterSourceQueuesResult(resp.QueueUrls, resp.NextToken);
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to list dead-letter source queues for '{queueUrl}'");
        }
    }

    /// <summary>
    /// Start a message move task to move messages from a DLQ.
    /// </summary>
    public static async Task<StartMessageMoveTaskResult> StartMessageMoveTaskAsync(
        string sourceArn,
        string? destinationArn = null,
        int? maxNumberOfMessagesPerSecond = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartMessageMoveTaskRequest { SourceArn = sourceArn };
        if (destinationArn != null)
            request.DestinationArn = destinationArn;
        if (maxNumberOfMessagesPerSecond.HasValue)
            request.MaxNumberOfMessagesPerSecond = maxNumberOfMessagesPerSecond.Value;

        try
        {
            var resp = await client.StartMessageMoveTaskAsync(request);
            return new StartMessageMoveTaskResult(resp.TaskHandle);
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start message move task");
        }
    }

    /// <summary>
    /// Cancel a running message move task.
    /// </summary>
    public static async Task<CancelMessageMoveTaskResult> CancelMessageMoveTaskAsync(
        string taskHandle,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelMessageMoveTaskAsync(new CancelMessageMoveTaskRequest
            {
                TaskHandle = taskHandle
            });
            return new CancelMessageMoveTaskResult(resp.ApproximateNumberOfMessagesMoved);
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to cancel message move task");
        }
    }

    /// <summary>
    /// List message move tasks for a given source ARN.
    /// </summary>
    public static async Task<ListMessageMoveTasksResult> ListMessageMoveTasksAsync(
        string sourceArn,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListMessageMoveTasksRequest { SourceArn = sourceArn };
        if (maxResults.HasValue)
            request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListMessageMoveTasksAsync(request);
            return new ListMessageMoveTasksResult(resp.Results);
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list message move tasks");
        }
    }

    // -----------------------------------------------------------------------
    // Convenience: SendBatchAsync (auto-chunks into 10-message batches)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Send up to 10 messages in a single batch, auto-serialising dicts/lists to JSON.
    /// </summary>
    /// <exception cref="ArgumentException">More than 10 messages supplied.</exception>
    /// <exception cref="AwsUtilException">If the batch call fails or any message is rejected.</exception>
    public static async Task<List<SendMessageResult>> SendBatchAsync(
        string queueUrl,
        List<object> messages,
        RegionEndpoint? region = null)
    {
        if (messages.Count > 10)
            throw new ArgumentException("SendBatchAsync supports at most 10 messages per call.");

        var client = GetClient(region);
        var entries = messages.Select((m, i) => new SendMessageBatchRequestEntry
        {
            Id = i.ToString(),
            MessageBody = m is string s ? s : JsonSerializer.Serialize(m)
        }).ToList();

        try
        {
            var resp = await client.SendMessageBatchAsync(new SendMessageBatchRequest
            {
                QueueUrl = queueUrl,
                Entries = entries
            });

            if (resp.Failed is { Count: > 0 })
            {
                var failures = resp.Failed.Select(f => f.Message).ToList();
                throw new AwsServiceException(
                    $"Batch send partially failed for '{queueUrl}': [{string.Join(", ", failures)}]",
                    null);
            }

            return resp.Successful
                .Select(r => new SendMessageResult(
                    r.MessageId,
                    string.IsNullOrEmpty(r.SequenceNumber) ? null : r.SequenceNumber))
                .ToList();
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to send message batch to '{queueUrl}'");
        }
    }

    // -----------------------------------------------------------------------
    // Convenience: SendLargeBatchAsync (splits into 10-message chunks)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Send any number of messages, automatically splitting into 10-message batches.
    /// </summary>
    /// <returns>Total number of messages sent.</returns>
    public static async Task<int> SendLargeBatchAsync(
        string queueUrl,
        List<object> messages,
        RegionEndpoint? region = null)
    {
        var totalSent = 0;
        for (var i = 0; i < messages.Count; i += 10)
        {
            var chunk = messages.Skip(i).Take(10).ToList();
            await SendBatchAsync(queueUrl, chunk, region);
            totalSent += chunk.Count;
        }
        return totalSent;
    }

    // -----------------------------------------------------------------------
    // Convenience: DeleteBatchAsync (up to 10 receipt handles)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Delete up to 10 messages by receipt handle in a single batch request.
    /// </summary>
    /// <exception cref="ArgumentException">More than 10 handles supplied.</exception>
    /// <exception cref="AwsUtilException">If the batch delete fails or any deletion is rejected.</exception>
    public static async Task DeleteBatchAsync(
        string queueUrl,
        List<string> receiptHandles,
        RegionEndpoint? region = null)
    {
        if (receiptHandles.Count > 10)
            throw new ArgumentException("DeleteBatchAsync supports at most 10 handles per call.");

        var client = GetClient(region);
        var entries = receiptHandles.Select((rh, i) => new DeleteMessageBatchRequestEntry
        {
            Id = i.ToString(),
            ReceiptHandle = rh
        }).ToList();

        try
        {
            var resp = await client.DeleteMessageBatchAsync(new DeleteMessageBatchRequest
            {
                QueueUrl = queueUrl,
                Entries = entries
            });

            if (resp.Failed is { Count: > 0 })
            {
                var failures = resp.Failed.Select(f => f.Message).ToList();
                throw new AwsServiceException(
                    $"Batch delete partially failed for '{queueUrl}': [{string.Join(", ", failures)}]",
                    null);
            }
        }
        catch (AmazonSQSException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete message batch from '{queueUrl}'");
        }
    }

    // -----------------------------------------------------------------------
    // Convenience: DrainQueueAsync
    // -----------------------------------------------------------------------

    /// <summary>
    /// Continuously receive and process messages until the queue is empty.
    /// Calls <paramref name="handler"/> for each message. Deletes the message
    /// automatically if the handler returns without throwing.
    /// </summary>
    /// <param name="queueUrl">Full SQS queue URL.</param>
    /// <param name="handler">Async callback invoked for each message.</param>
    /// <param name="batchSize">Messages per poll (1-10).</param>
    /// <param name="maxMessages">Stop after this many messages. Null drains completely.</param>
    /// <param name="visibilityTimeout">Seconds the message stays invisible while being processed.</param>
    /// <param name="waitSeconds">Long-poll duration per receive call.</param>
    /// <param name="region">AWS region override.</param>
    /// <returns>Total number of messages successfully processed.</returns>
    public static async Task<int> DrainQueueAsync(
        string queueUrl,
        Func<SqsMessage, Task> handler,
        int batchSize = 10,
        int? maxMessages = null,
        int visibilityTimeout = 60,
        int waitSeconds = 5,
        RegionEndpoint? region = null)
    {
        var processed = 0;
        var consecutiveEmpty = 0;

        while (true)
        {
            if (maxMessages.HasValue && processed >= maxMessages.Value)
                break;

            var remaining = maxMessages.HasValue
                ? Math.Min(batchSize, maxMessages.Value - processed)
                : batchSize;

            var messages = await ReceiveMessagesAsync(
                queueUrl,
                maxNumber: remaining,
                waitSeconds: waitSeconds,
                visibilityTimeout: visibilityTimeout,
                region: region);

            if (messages.Count == 0)
            {
                consecutiveEmpty++;
                if (consecutiveEmpty >= 2)
                    break;
                continue;
            }

            consecutiveEmpty = 0;
            foreach (var msg in messages)
            {
                try
                {
                    await handler(msg);
                    await DeleteMessageAsync(queueUrl, msg.ReceiptHandle, region);
                    processed++;
                }
                catch (Exception)
                {
                    // Message becomes visible again after visibilityTimeout expires.
                }
            }
        }

        return processed;
    }

    // -----------------------------------------------------------------------
    // Convenience: ReplayDlqAsync
    // -----------------------------------------------------------------------

    /// <summary>
    /// Move messages from a dead-letter queue back to a target queue.
    /// Useful for replaying failed messages after fixing the underlying issue.
    /// </summary>
    /// <returns>Number of messages successfully moved.</returns>
    public static async Task<int> ReplayDlqAsync(
        string dlqUrl,
        string targetUrl,
        int? maxMessages = null,
        RegionEndpoint? region = null)
    {
        return await DrainQueueAsync(
            dlqUrl,
            handler: async msg =>
            {
                var request = new SendMessageRequest
                {
                    QueueUrl = targetUrl,
                    MessageBody = msg.Body
                };
                if (msg.MessageAttributes is { Count: > 0 })
                    request.MessageAttributes = msg.MessageAttributes;

                var client = GetClient(region);
                try
                {
                    await client.SendMessageAsync(request);
                }
                catch (AmazonSQSException exc)
                {
                    throw ErrorClassifier.WrapAwsError(exc,
                        $"Failed to replay message to '{targetUrl}'");
                }
            },
            maxMessages: maxMessages,
            region: region);
    }

    // -----------------------------------------------------------------------
    // Convenience: WaitForMessageAsync
    // -----------------------------------------------------------------------

    /// <summary>
    /// Poll a queue until a message matching <paramref name="predicate"/> arrives
    /// or <paramref name="timeout"/> expires.
    /// </summary>
    /// <param name="queueUrl">Full SQS queue URL.</param>
    /// <param name="predicate">Optional filter. Null accepts the first message.</param>
    /// <param name="timeout">Maximum time to wait.</param>
    /// <param name="pollInterval">Seconds between receive calls.</param>
    /// <param name="visibilityTimeout">Seconds the message stays invisible after receipt.</param>
    /// <param name="deleteOnMatch">If true, delete the matching message automatically.</param>
    /// <param name="region">AWS region override.</param>
    /// <returns>The first matching message, or null if timeout expires.</returns>
    public static async Task<SqsMessage?> WaitForMessageAsync(
        string queueUrl,
        Func<SqsMessage, bool>? predicate = null,
        TimeSpan? timeout = null,
        TimeSpan? pollInterval = null,
        int visibilityTimeout = 30,
        bool deleteOnMatch = true,
        RegionEndpoint? region = null)
    {
        var effectiveTimeout = timeout ?? TimeSpan.FromSeconds(60);
        var effectivePoll = pollInterval ?? TimeSpan.FromSeconds(2);
        var deadline = DateTime.UtcNow + effectiveTimeout;

        while (DateTime.UtcNow < deadline)
        {
            var waitSec = Math.Min((int)effectivePoll.TotalSeconds, 20);
            var messages = await ReceiveMessagesAsync(
                queueUrl,
                maxNumber: 10,
                waitSeconds: waitSec,
                visibilityTimeout: visibilityTimeout,
                region: region);

            foreach (var msg in messages)
            {
                if (predicate == null || predicate(msg))
                {
                    if (deleteOnMatch)
                        await DeleteMessageAsync(queueUrl, msg.ReceiptHandle, region);
                    return msg;
                }
            }

            var delay = effectivePoll - TimeSpan.FromSeconds(1);
            if (delay > TimeSpan.Zero)
                await Task.Delay(delay);
        }

        return null;
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="SendMessageAsync"/>.</summary>
    public static SendMessageResult SendMessage(string queueUrl, object body, int delaySeconds = 0, string? messageGroupId = null, string? messageDeduplicationId = null, RegionEndpoint? region = null)
        => SendMessageAsync(queueUrl, body, delaySeconds, messageGroupId, messageDeduplicationId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SendMessageBatchAsync"/>.</summary>
    public static SendMessageBatchResult SendMessageBatch(string queueUrl, List<SendMessageBatchRequestEntry> entries, RegionEndpoint? region = null)
        => SendMessageBatchAsync(queueUrl, entries, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ReceiveMessagesAsync"/>.</summary>
    public static List<SqsMessage> ReceiveMessages(string queueUrl, int maxNumber = 1, int waitSeconds = 0, int visibilityTimeout = 30, RegionEndpoint? region = null)
        => ReceiveMessagesAsync(queueUrl, maxNumber, waitSeconds, visibilityTimeout, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteMessageAsync"/>.</summary>
    public static void DeleteMessage(string queueUrl, string receiptHandle, RegionEndpoint? region = null)
        => DeleteMessageAsync(queueUrl, receiptHandle, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteMessageBatchAsync"/>.</summary>
    public static DeleteMessageBatchResult DeleteMessageBatch(string queueUrl, List<DeleteMessageBatchRequestEntry> entries, RegionEndpoint? region = null)
        => DeleteMessageBatchAsync(queueUrl, entries, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateQueueAsync"/>.</summary>
    public static CreateQueueResult CreateQueue(string queueName, Dictionary<string, string>? attributes = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateQueueAsync(queueName, attributes, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteQueueAsync"/>.</summary>
    public static void DeleteQueue(string queueUrl, RegionEndpoint? region = null)
        => DeleteQueueAsync(queueUrl, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetQueueUrlAsync"/>.</summary>
    public static string GetQueueUrl(string queueName, RegionEndpoint? region = null)
        => GetQueueUrlAsync(queueName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetQueueAttributesAsync"/>.</summary>
    public static Dictionary<string, string> GetQueueAttributes(string queueUrl, List<string>? attributeNames = null, RegionEndpoint? region = null)
        => GetQueueAttributesAsync(queueUrl, attributeNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SetQueueAttributesAsync"/>.</summary>
    public static void SetQueueAttributes(string queueUrl, Dictionary<string, string> attributes, RegionEndpoint? region = null)
        => SetQueueAttributesAsync(queueUrl, attributes, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PurgeQueueAsync"/>.</summary>
    public static void PurgeQueue(string queueUrl, RegionEndpoint? region = null)
        => PurgeQueueAsync(queueUrl, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ChangeMessageVisibilityAsync"/>.</summary>
    public static void ChangeMessageVisibility(string queueUrl, string receiptHandle, int visibilityTimeout, RegionEndpoint? region = null)
        => ChangeMessageVisibilityAsync(queueUrl, receiptHandle, visibilityTimeout, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ChangeMessageVisibilityBatchAsync"/>.</summary>
    public static ChangeMessageVisibilityBatchResult ChangeMessageVisibilityBatch(string queueUrl, List<ChangeMessageVisibilityBatchRequestEntry> entries, RegionEndpoint? region = null)
        => ChangeMessageVisibilityBatchAsync(queueUrl, entries, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListQueuesAsync"/>.</summary>
    public static ListQueuesResult ListQueues(string? queueNamePrefix = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListQueuesAsync(queueNamePrefix, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagQueueAsync"/>.</summary>
    public static void TagQueue(string queueUrl, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagQueueAsync(queueUrl, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagQueueAsync"/>.</summary>
    public static void UntagQueue(string queueUrl, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagQueueAsync(queueUrl, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListQueueTagsAsync"/>.</summary>
    public static ListQueueTagsResult ListQueueTags(string queueUrl, RegionEndpoint? region = null)
        => ListQueueTagsAsync(queueUrl, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddPermissionAsync"/>.</summary>
    public static void AddPermission(string queueUrl, string label, List<string> awsAccountIds, List<string> actions, RegionEndpoint? region = null)
        => AddPermissionAsync(queueUrl, label, awsAccountIds, actions, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RemovePermissionAsync"/>.</summary>
    public static void RemovePermission(string queueUrl, string label, RegionEndpoint? region = null)
        => RemovePermissionAsync(queueUrl, label, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDeadLetterSourceQueuesAsync"/>.</summary>
    public static ListDeadLetterSourceQueuesResult ListDeadLetterSourceQueues(string queueUrl, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListDeadLetterSourceQueuesAsync(queueUrl, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartMessageMoveTaskAsync"/>.</summary>
    public static StartMessageMoveTaskResult StartMessageMoveTask(string sourceArn, string? destinationArn = null, int? maxNumberOfMessagesPerSecond = null, RegionEndpoint? region = null)
        => StartMessageMoveTaskAsync(sourceArn, destinationArn, maxNumberOfMessagesPerSecond, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CancelMessageMoveTaskAsync"/>.</summary>
    public static CancelMessageMoveTaskResult CancelMessageMoveTask(string taskHandle, RegionEndpoint? region = null)
        => CancelMessageMoveTaskAsync(taskHandle, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListMessageMoveTasksAsync"/>.</summary>
    public static ListMessageMoveTasksResult ListMessageMoveTasks(string sourceArn, int? maxResults = null, RegionEndpoint? region = null)
        => ListMessageMoveTasksAsync(sourceArn, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SendBatchAsync"/>.</summary>
    public static List<SendMessageResult> SendBatch(string queueUrl, List<object> messages, RegionEndpoint? region = null)
        => SendBatchAsync(queueUrl, messages, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SendLargeBatchAsync"/>.</summary>
    public static int SendLargeBatch(string queueUrl, List<object> messages, RegionEndpoint? region = null)
        => SendLargeBatchAsync(queueUrl, messages, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteBatchAsync"/>.</summary>
    public static void DeleteBatch(string queueUrl, List<string> receiptHandles, RegionEndpoint? region = null)
        => DeleteBatchAsync(queueUrl, receiptHandles, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DrainQueueAsync"/>.</summary>
    public static int DrainQueue(string queueUrl, Func<SqsMessage, Task> handler, int batchSize = 10, int? maxMessages = null, int visibilityTimeout = 60, int waitSeconds = 5, RegionEndpoint? region = null)
        => DrainQueueAsync(queueUrl, handler, batchSize, maxMessages, visibilityTimeout, waitSeconds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ReplayDlqAsync"/>.</summary>
    public static int ReplayDlq(string dlqUrl, string targetUrl, int? maxMessages = null, RegionEndpoint? region = null)
        => ReplayDlqAsync(dlqUrl, targetUrl, maxMessages, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="WaitForMessageAsync"/>.</summary>
    public static SqsMessage? WaitForMessage(string queueUrl, Func<SqsMessage, bool>? predicate = null, TimeSpan? timeout = null, TimeSpan? pollInterval = null, int visibilityTimeout = 30, bool deleteOnMatch = true, RegionEndpoint? region = null)
        => WaitForMessageAsync(queueUrl, predicate, timeout, pollInterval, visibilityTimeout, deleteOnMatch, region).GetAwaiter().GetResult();

}

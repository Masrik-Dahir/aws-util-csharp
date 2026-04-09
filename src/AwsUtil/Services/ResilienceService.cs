using System.Text.Json;
using Amazon;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>State of a circuit breaker.</summary>
public sealed record CircuitBreakerState(
    string Status,
    int FailureCount = 0,
    int SuccessCount = 0,
    DateTime? LastFailureTime = null,
    DateTime? LastSuccessTime = null,
    string? LastError = null);

/// <summary>Result of a retry operation.</summary>
public sealed record RetryResult<T>(
    T? Value = default,
    bool Succeeded = false,
    int AttemptsUsed = 0,
    string? LastError = null);

/// <summary>Result of monitoring a DLQ.</summary>
public sealed record DlqMonitorAndAlertResult(
    string? QueueUrl = null,
    int ApproximateMessageCount = 0,
    bool AlertTriggered = false,
    string? AlertMessage = null);

/// <summary>Result of poison pill handling.</summary>
public sealed record PoisonPillHandlerResult(
    string? MessageId = null,
    bool IsPoisonPill = false,
    bool MovedToDlq = false,
    int ReceiveCount = 0);

/// <summary>Result of Lambda destination routing.</summary>
public sealed record LambdaDestinationRouterResult(
    string? Destination = null,
    bool IsSuccess = false,
    string? Payload = null);

/// <summary>Result of a graceful degradation attempt.</summary>
public sealed record GracefulDegradationResult<T>(
    T? Value = default,
    bool UsedFallback = false,
    string? Error = null);

/// <summary>Result of a timeout sentinel operation.</summary>
public sealed record TimeoutSentinelResult<T>(
    T? Value = default,
    bool TimedOut = false,
    TimeSpan Elapsed = default);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Resilience patterns for distributed AWS workloads: circuit breaker,
/// retry, DLQ monitoring, poison pill handling, and graceful degradation.
/// </summary>
public static class ResilienceService
{
    private static AmazonSQSClient GetSqsClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSQSClient>(region);

    private static AmazonCloudWatchClient GetCwClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCloudWatchClient>(region);

    /// <summary>
    /// Execute an action through a circuit breaker. The breaker opens after
    /// <paramref name="failureThreshold"/> consecutive failures and stays open for
    /// <paramref name="resetTimeoutSeconds"/> seconds before transitioning to half-open.
    /// </summary>
    public static async Task<(T? Result, CircuitBreakerState State)> CircuitBreakerAsync<T>(
        Func<Task<T>> action,
        CircuitBreakerState currentState,
        int failureThreshold = 5,
        int resetTimeoutSeconds = 60)
    {
        // If circuit is OPEN, check if reset timeout has elapsed
        if (currentState.Status == "OPEN")
        {
            if (currentState.LastFailureTime.HasValue &&
                DateTime.UtcNow - currentState.LastFailureTime.Value <
                TimeSpan.FromSeconds(resetTimeoutSeconds))
            {
                return (default, currentState);
            }

            // Transition to half-open
            currentState = currentState with { Status = "HALF_OPEN" };
        }

        try
        {
            var result = await action();
            var newState = new CircuitBreakerState(
                Status: "CLOSED",
                FailureCount: 0,
                SuccessCount: currentState.SuccessCount + 1,
                LastSuccessTime: DateTime.UtcNow);
            return (result, newState);
        }
        catch (Exception ex)
        {
            var newFailureCount = currentState.FailureCount + 1;
            var newStatus = newFailureCount >= failureThreshold ? "OPEN" : "CLOSED";

            var newState = new CircuitBreakerState(
                Status: newStatus,
                FailureCount: newFailureCount,
                SuccessCount: currentState.SuccessCount,
                LastFailureTime: DateTime.UtcNow,
                LastSuccessTime: currentState.LastSuccessTime,
                LastError: ex.Message);
            return (default, newState);
        }
    }

    /// <summary>
    /// Retry an action with exponential backoff and optional jitter.
    /// </summary>
    public static async Task<RetryResult<T>> RetryWithBackoffAsync<T>(
        Func<Task<T>> action,
        int maxRetries = 3,
        int baseDelayMs = 100,
        int maxDelayMs = 30000,
        double jitterFactor = 0.1,
        Func<Exception, bool>? retryableExceptionFilter = null)
    {
        var random = new Random();
        string? lastError = null;

        for (var attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var result = await action();
                return new RetryResult<T>(
                    Value: result,
                    Succeeded: true,
                    AttemptsUsed: attempt);
            }
            catch (Exception ex)
            {
                lastError = ex.Message;

                if (retryableExceptionFilter != null && !retryableExceptionFilter(ex))
                {
                    return new RetryResult<T>(
                        Succeeded: false,
                        AttemptsUsed: attempt,
                        LastError: lastError);
                }

                if (attempt < maxRetries)
                {
                    var delay = Math.Min(
                        baseDelayMs * (int)Math.Pow(2, attempt - 1),
                        maxDelayMs);
                    var jitter = (int)(delay * jitterFactor * (random.NextDouble() * 2 - 1));
                    await Task.Delay(Math.Max(0, delay + jitter));
                }
            }
        }

        return new RetryResult<T>(
            Succeeded: false,
            AttemptsUsed: maxRetries,
            LastError: lastError);
    }

    /// <summary>
    /// Monitor a Dead Letter Queue depth and trigger an alert if the message count
    /// exceeds the threshold.
    /// </summary>
    public static async Task<DlqMonitorAndAlertResult> DlqMonitorAndAlertAsync(
        string queueUrl,
        int alertThreshold = 10,
        string? snsTopicArn = null,
        RegionEndpoint? region = null)
    {
        var sqs = GetSqsClient(region);

        try
        {
            var attrsResp = await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                AttributeNames = new List<string>
                    { "ApproximateNumberOfMessages" }
            });

            var count = 0;
            if (attrsResp.Attributes.TryGetValue("ApproximateNumberOfMessages", out var countStr))
                int.TryParse(countStr, out count);

            var alertTriggered = count >= alertThreshold;
            string? alertMessage = null;

            if (alertTriggered)
            {
                alertMessage =
                    $"DLQ depth alert: {count} messages in queue (threshold: {alertThreshold})";

                if (snsTopicArn != null)
                {
                    await SnsService.PublishAsync(
                        snsTopicArn,
                        alertMessage,
                        subject: "DLQ Depth Alert",
                        region: region);
                }
            }

            return new DlqMonitorAndAlertResult(
                QueueUrl: queueUrl,
                ApproximateMessageCount: count,
                AlertTriggered: alertTriggered,
                AlertMessage: alertMessage);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to monitor DLQ '{queueUrl}'");
        }
    }

    /// <summary>
    /// Handle poison pill messages by checking receive count and moving to a DLQ
    /// if the threshold is exceeded.
    /// </summary>
    public static async Task<PoisonPillHandlerResult> PoisonPillHandlerAsync(
        string queueUrl,
        string receiptHandle,
        string messageId,
        int maxReceiveCount = 3,
        string? dlqUrl = null,
        string? messageBody = null,
        RegionEndpoint? region = null)
    {
        var sqs = GetSqsClient(region);

        try
        {
            // Get message attributes to check receive count
            var receiveResp = await sqs.GetQueueAttributesAsync(new GetQueueAttributesRequest
            {
                QueueUrl = queueUrl,
                AttributeNames = new List<string>
                    { "ApproximateNumberOfMessages", "RedrivePolicy" }
            });

            // Approximate receive count check: if we have a DLQ URL and know the
            // receive count exceeds threshold, move the message there.
            var receiveCount = 0;

            // Try to parse from the message attributes if available
            if (receiveResp.Attributes.TryGetValue("ApproximateReceiveCount", out var rcStr))
                int.TryParse(rcStr, out receiveCount);

            var isPoisonPill = receiveCount >= maxReceiveCount;
            var movedToDlq = false;

            if (isPoisonPill && dlqUrl != null && messageBody != null)
            {
                await sqs.SendMessageAsync(new SendMessageRequest
                {
                    QueueUrl = dlqUrl,
                    MessageBody = messageBody,
                    MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        ["OriginalMessageId"] = new MessageAttributeValue
                        {
                            DataType = "String",
                            StringValue = messageId
                        },
                        ["PoisonPillReason"] = new MessageAttributeValue
                        {
                            DataType = "String",
                            StringValue =
                                $"Exceeded max receive count of {maxReceiveCount}"
                        }
                    }
                });

                await sqs.DeleteMessageAsync(new DeleteMessageRequest
                {
                    QueueUrl = queueUrl,
                    ReceiptHandle = receiptHandle
                });

                movedToDlq = true;
            }

            return new PoisonPillHandlerResult(
                MessageId: messageId,
                IsPoisonPill: isPoisonPill,
                MovedToDlq: movedToDlq,
                ReceiveCount: receiveCount);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to handle poison pill message '{messageId}'");
        }
    }

    /// <summary>
    /// Route a Lambda invocation result based on success or failure destination.
    /// </summary>
    public static async Task<LambdaDestinationRouterResult> LambdaDestinationRouterAsync(
        string payload,
        bool isSuccess,
        string? successDestinationArn = null,
        string? failureDestinationArn = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var destinationArn = isSuccess ? successDestinationArn : failureDestinationArn;

            if (destinationArn == null)
            {
                return new LambdaDestinationRouterResult(
                    Destination: null,
                    IsSuccess: isSuccess,
                    Payload: payload);
            }

            // Route to SQS if the destination looks like an SQS ARN
            if (destinationArn.Contains(":sqs:"))
            {
                // Derive the queue URL from the ARN
                var arnParts = destinationArn.Split(':');
                var awsRegion = arnParts.Length > 3 ? arnParts[3] : "";
                var accountId = arnParts.Length > 4 ? arnParts[4] : "";
                var queueName = arnParts.Length > 5 ? arnParts[5] : "";
                var queueUrl = $"https://sqs.{awsRegion}.amazonaws.com/{accountId}/{queueName}";

                var sqs = GetSqsClient(region);
                await sqs.SendMessageAsync(new SendMessageRequest
                {
                    QueueUrl = queueUrl,
                    MessageBody = payload
                });
            }
            else if (destinationArn.Contains(":sns:"))
            {
                await SnsService.PublishAsync(
                    destinationArn,
                    payload,
                    region: region);
            }
            else if (destinationArn.Contains(":lambda:"))
            {
                await LambdaService.InvokeAsync(
                    destinationArn,
                    payload: payload,
                    invocationType: "Event",
                    region: region);
            }

            return new LambdaDestinationRouterResult(
                Destination: destinationArn,
                IsSuccess: isSuccess,
                Payload: payload);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to route Lambda destination");
        }
    }

    /// <summary>
    /// Execute a primary action with a fallback on failure.
    /// </summary>
    public static async Task<GracefulDegradationResult<T>> GracefulDegradationAsync<T>(
        Func<Task<T>> primaryAction,
        Func<Task<T>> fallbackAction)
    {
        try
        {
            var result = await primaryAction();
            return new GracefulDegradationResult<T>(Value: result);
        }
        catch (Exception primaryEx)
        {
            try
            {
                var fallback = await fallbackAction();
                return new GracefulDegradationResult<T>(
                    Value: fallback,
                    UsedFallback: true,
                    Error: primaryEx.Message);
            }
            catch (Exception fallbackEx)
            {
                throw ErrorClassifier.WrapAwsError(fallbackEx,
                    $"Both primary and fallback failed. Primary: {primaryEx.Message}");
            }
        }
    }

    /// <summary>
    /// Execute an action with a timeout. If the timeout is exceeded, return a
    /// sentinel value instead of throwing.
    /// </summary>
    public static async Task<TimeoutSentinelResult<T>> TimeoutSentinelAsync<T>(
        Func<Task<T>> action,
        TimeSpan timeout,
        T? sentinelValue = default)
    {
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        using var cts = new CancellationTokenSource(timeout);
        try
        {
            var task = action();
            var completedTask = await Task.WhenAny(task, Task.Delay(timeout, cts.Token));

            if (completedTask == task)
            {
                stopwatch.Stop();
                var result = await task;
                return new TimeoutSentinelResult<T>(
                    Value: result,
                    TimedOut: false,
                    Elapsed: stopwatch.Elapsed);
            }

            stopwatch.Stop();
            return new TimeoutSentinelResult<T>(
                Value: sentinelValue,
                TimedOut: true,
                Elapsed: stopwatch.Elapsed);
        }
        catch (OperationCanceledException)
        {
            stopwatch.Stop();
            return new TimeoutSentinelResult<T>(
                Value: sentinelValue,
                TimedOut: true,
                Elapsed: stopwatch.Elapsed);
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="DlqMonitorAndAlertAsync"/>.</summary>
    public static DlqMonitorAndAlertResult DlqMonitorAndAlert(string queueUrl, int alertThreshold = 10, string? snsTopicArn = null, RegionEndpoint? region = null)
        => DlqMonitorAndAlertAsync(queueUrl, alertThreshold, snsTopicArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PoisonPillHandlerAsync"/>.</summary>
    public static PoisonPillHandlerResult PoisonPillHandler(string queueUrl, string receiptHandle, string messageId, int maxReceiveCount = 3, string? dlqUrl = null, string? messageBody = null, RegionEndpoint? region = null)
        => PoisonPillHandlerAsync(queueUrl, receiptHandle, messageId, maxReceiveCount, dlqUrl, messageBody, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="LambdaDestinationRouterAsync"/>.</summary>
    public static LambdaDestinationRouterResult LambdaDestinationRouter(string payload, bool isSuccess, string? successDestinationArn = null, string? failureDestinationArn = null, RegionEndpoint? region = null)
        => LambdaDestinationRouterAsync(payload, isSuccess, successDestinationArn, failureDestinationArn, region).GetAwaiter().GetResult();

}

using Amazon;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Amazon.SimpleEmail;
using Amazon.SimpleEmail.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using AwsUtil.Exceptions;
using System.Text.Json;

namespace AwsUtil.Services;

/// <summary>
/// Result of a single notification delivery attempt.
/// </summary>
public sealed record NotificationResult(
    string Channel,
    string Destination,
    bool Success,
    string? MessageId = null,
    string? Error = null);

/// <summary>
/// Aggregated result of a broadcast call.
/// </summary>
public sealed record BroadcastResult(List<NotificationResult> Results)
{
    public List<NotificationResult> Succeeded => Results.Where(r => r.Success).ToList();
    public List<NotificationResult> Failed => Results.Where(r => !r.Success).ToList();
    public bool AllSucceeded => Results.All(r => r.Success);
}

/// <summary>
/// Unified alerting across SNS, SES, and SQS.
/// </summary>
public static class NotifierService
{
    private static async Task<NotificationResult> PublishSns(
        string topicArn, string subject, string message, RegionEndpoint? region)
    {
        var client = ClientFactory.GetClient<AmazonSimpleNotificationServiceClient>(region);
        try
        {
            var resp = await client.PublishAsync(new PublishRequest
            {
                TopicArn = topicArn,
                Subject = subject.Length > 100 ? subject[..100] : subject,
                Message = message
            });
            return new NotificationResult("sns", topicArn, true, MessageId: resp.MessageId);
        }
        catch (Exception exc)
        {
            return new NotificationResult("sns", topicArn, false, Error: exc.Message);
        }
    }

    private static async Task<NotificationResult> SendSes(
        string from, string to, string subject, string body, RegionEndpoint? region)
    {
        var client = ClientFactory.GetClient<AmazonSimpleEmailServiceClient>(region);
        try
        {
            var resp = await client.SendEmailAsync(new Amazon.SimpleEmail.Model.SendEmailRequest
            {
                Source = from,
                Destination = new Amazon.SimpleEmail.Model.Destination { ToAddresses = [to] },
                Message = new Amazon.SimpleEmail.Model.Message
                {
                    Subject = new Content(subject),
                    Body = new Body { Text = new Content(body) }
                }
            });
            return new NotificationResult("ses", to, true, MessageId: resp.MessageId);
        }
        catch (Exception exc)
        {
            return new NotificationResult("ses", to, false, Error: exc.Message);
        }
    }

    private static async Task<NotificationResult> SendSqs(
        string queueUrl, string message, RegionEndpoint? region)
    {
        var client = ClientFactory.GetClient<AmazonSQSClient>(region);
        try
        {
            var resp = await client.SendMessageAsync(new Amazon.SQS.Model.SendMessageRequest
            {
                QueueUrl = queueUrl,
                MessageBody = message
            });
            return new NotificationResult("sqs", queueUrl, true, MessageId: resp.MessageId);
        }
        catch (Exception exc)
        {
            return new NotificationResult("sqs", queueUrl, false, Error: exc.Message);
        }
    }

    /// <summary>
    /// Send an alert to an SNS topic.
    /// </summary>
    public static async Task<NotificationResult> SendAlertAsync(
        string topicArn,
        string subject,
        string message,
        RegionEndpoint? region = null)
    {
        return await PublishSns(topicArn, subject, message, region);
    }

    /// <summary>
    /// Broadcast a message to multiple destinations (SNS topics, SES emails, SQS queues).
    /// </summary>
    public static async Task<BroadcastResult> BroadcastAsync(
        string subject,
        string message,
        List<string>? snsTopicArns = null,
        List<(string From, string To)>? sesTargets = null,
        List<string>? sqsQueueUrls = null,
        RegionEndpoint? region = null)
    {
        var tasks = new List<Task<NotificationResult>>();

        if (snsTopicArns != null)
            tasks.AddRange(snsTopicArns.Select(arn => PublishSns(arn, subject, message, region)));

        if (sesTargets != null)
            tasks.AddRange(sesTargets.Select(t => SendSes(t.From, t.To, subject, message, region)));

        if (sqsQueueUrls != null)
            tasks.AddRange(sqsQueueUrls.Select(url => SendSqs(url, message, region)));

        var results = await Task.WhenAll(tasks);
        return new BroadcastResult(results.ToList());
    }

    /// <summary>
    /// Decorator-style method: wraps an async action and sends an alert on exception.
    /// </summary>
    public static async Task<T> NotifyOnExceptionAsync<T>(
        Func<Task<T>> action,
        string topicArn,
        string subject = "Exception Alert",
        RegionEndpoint? region = null)
    {
        try
        {
            return await action();
        }
        catch (Exception exc)
        {
            var message = $"Exception: {exc.GetType().Name}\n\n{exc.Message}\n\n{exc.StackTrace}";
            await SendAlertAsync(topicArn, subject, message, region);
            throw;
        }
    }

    /// <summary>
    /// Resolve placeholder strings in the message and send notification.
    /// </summary>
    public static async Task<NotificationResult> ResolveAndNotifyAsync(
        string topicArn,
        string subject,
        string messageTemplate,
        RegionEndpoint? region = null)
    {
        var resolved = (string)(await Placeholder.RetrieveAsync(messageTemplate))!;
        return await SendAlertAsync(topicArn, subject, resolved, region);
    }
}

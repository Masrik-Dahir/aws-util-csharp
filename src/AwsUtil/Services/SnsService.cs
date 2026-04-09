using Amazon;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for SNS operations.
/// </summary>
public sealed record PublishResult(string? MessageId = null);

public sealed record PublishBatchResult(
    List<PublishBatchResultEntry>? Successful = null,
    List<BatchResultErrorEntry>? Failed = null);

public sealed record CreateTopicResult(string? TopicArn = null);

public sealed record SubscribeResult(string? SubscriptionArn = null);

public sealed record ConfirmSubscriptionResult(string? SubscriptionArn = null);

public sealed record ListTopicsResult(List<string>? TopicArns = null, string? NextToken = null);

public sealed record ListSubscriptionsResult(
    List<SubscriptionInfo>? Subscriptions = null,
    string? NextToken = null);

public sealed record SubscriptionInfo(
    string? SubscriptionArn = null,
    string? Owner = null,
    string? Protocol = null,
    string? Endpoint = null,
    string? TopicArn = null);

public sealed record TopicAttributesResult(Dictionary<string, string>? Attributes = null);

public sealed record SubscriptionAttributesResult(Dictionary<string, string>? Attributes = null);

public sealed record SnsListTagsResult(List<KeyValuePair<string, string>>? Tags = null);

public sealed record CreatePlatformApplicationResult(string? PlatformApplicationArn = null);

public sealed record CreatePlatformEndpointResult(string? EndpointArn = null);

public sealed record SmsAttributesResult(Dictionary<string, string>? Attributes = null);

public sealed record CheckIfPhoneNumberIsOptedOutResult(bool IsOptedOut);

public sealed record ListPhoneNumbersOptedOutResult(
    List<string>? PhoneNumbers = null,
    string? NextToken = null);

/// <summary>
/// Utility helpers for Amazon Simple Notification Service (SNS).
/// </summary>
public static class SnsService
{
    private static AmazonSimpleNotificationServiceClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSimpleNotificationServiceClient>(region);

    /// <summary>
    /// Publish a message to an SNS topic or target ARN.
    /// </summary>
    public static async Task<PublishResult> PublishAsync(
        string message,
        string? topicArn = null,
        string? targetArn = null,
        string? phoneNumber = null,
        string? subject = null,
        Dictionary<string, MessageAttributeValue>? messageAttributes = null,
        string? messageStructure = null,
        string? messageDeduplicationId = null,
        string? messageGroupId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PublishRequest { Message = message };
        if (topicArn != null) request.TopicArn = topicArn;
        if (targetArn != null) request.TargetArn = targetArn;
        if (phoneNumber != null) request.PhoneNumber = phoneNumber;
        if (subject != null) request.Subject = subject;
        if (messageAttributes != null) request.MessageAttributes = messageAttributes;
        if (messageStructure != null) request.MessageStructure = messageStructure;
        if (messageDeduplicationId != null) request.MessageDeduplicationId = messageDeduplicationId;
        if (messageGroupId != null) request.MessageGroupId = messageGroupId;

        try
        {
            var resp = await client.PublishAsync(request);
            return new PublishResult(MessageId: resp.MessageId);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to publish SNS message");
        }
    }

    /// <summary>
    /// Create an SNS topic.
    /// </summary>
    public static async Task<CreateTopicResult> CreateTopicAsync(
        string name,
        Dictionary<string, string>? attributes = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateTopicRequest { Name = name };
        if (attributes != null) request.Attributes = attributes;
        if (tags != null)
            request.Tags = tags.Select(kv => new Tag { Key = kv.Key, Value = kv.Value }).ToList();

        try
        {
            var resp = await client.CreateTopicAsync(request);
            return new CreateTopicResult(TopicArn: resp.TopicArn);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create topic '{name}'");
        }
    }

    /// <summary>
    /// Delete an SNS topic.
    /// </summary>
    public static async Task DeleteTopicAsync(
        string topicArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTopicAsync(new DeleteTopicRequest { TopicArn = topicArn });
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete topic '{topicArn}'");
        }
    }

    /// <summary>
    /// Subscribe to an SNS topic.
    /// </summary>
    public static async Task<SubscribeResult> SubscribeAsync(
        string topicArn,
        string protocol,
        string? endpoint = null,
        Dictionary<string, string>? attributes = null,
        bool returnSubscriptionArn = true,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SubscribeRequest
        {
            TopicArn = topicArn,
            Protocol = protocol,
            ReturnSubscriptionArn = returnSubscriptionArn
        };
        if (endpoint != null) request.Endpoint = endpoint;
        if (attributes != null) request.Attributes = attributes;

        try
        {
            var resp = await client.SubscribeAsync(request);
            return new SubscribeResult(SubscriptionArn: resp.SubscriptionArn);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to subscribe to topic '{topicArn}'");
        }
    }

    /// <summary>
    /// Unsubscribe from an SNS subscription.
    /// </summary>
    public static async Task UnsubscribeAsync(
        string subscriptionArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UnsubscribeAsync(new UnsubscribeRequest { SubscriptionArn = subscriptionArn });
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to unsubscribe '{subscriptionArn}'");
        }
    }

    /// <summary>
    /// List all SNS topics, automatically paginating.
    /// </summary>
    public static async Task<ListTopicsResult> ListTopicsAsync(RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var topicArns = new List<string>();
        var request = new ListTopicsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListTopicsAsync(request);
                topicArns.AddRange(resp.Topics.Select(t => t.TopicArn));
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list topics");
        }

        return new ListTopicsResult(TopicArns: topicArns);
    }

    /// <summary>
    /// List all SNS subscriptions, automatically paginating.
    /// </summary>
    public static async Task<ListSubscriptionsResult> ListSubscriptionsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var subscriptions = new List<SubscriptionInfo>();
        var request = new ListSubscriptionsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListSubscriptionsAsync(request);
                subscriptions.AddRange(resp.Subscriptions.Select(s => new SubscriptionInfo(
                    SubscriptionArn: s.SubscriptionArn,
                    Owner: s.Owner,
                    Protocol: s.Protocol,
                    Endpoint: s.Endpoint,
                    TopicArn: s.TopicArn)));
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list subscriptions");
        }

        return new ListSubscriptionsResult(Subscriptions: subscriptions);
    }

    /// <summary>
    /// List subscriptions for a specific topic, automatically paginating.
    /// </summary>
    public static async Task<ListSubscriptionsResult> ListSubscriptionsByTopicAsync(
        string topicArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var subscriptions = new List<SubscriptionInfo>();
        var request = new ListSubscriptionsByTopicRequest { TopicArn = topicArn };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListSubscriptionsByTopicAsync(request);
                subscriptions.AddRange(resp.Subscriptions.Select(s => new SubscriptionInfo(
                    SubscriptionArn: s.SubscriptionArn,
                    Owner: s.Owner,
                    Protocol: s.Protocol,
                    Endpoint: s.Endpoint,
                    TopicArn: s.TopicArn)));
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to list subscriptions for topic '{topicArn}'");
        }

        return new ListSubscriptionsResult(Subscriptions: subscriptions);
    }

    /// <summary>
    /// Get attributes of an SNS topic.
    /// </summary>
    public static async Task<TopicAttributesResult> GetTopicAttributesAsync(
        string topicArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetTopicAttributesAsync(
                new GetTopicAttributesRequest { TopicArn = topicArn });
            return new TopicAttributesResult(Attributes: resp.Attributes);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get topic attributes for '{topicArn}'");
        }
    }

    /// <summary>
    /// Set an attribute on an SNS topic.
    /// </summary>
    public static async Task SetTopicAttributesAsync(
        string topicArn,
        string attributeName,
        string attributeValue,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.SetTopicAttributesAsync(new SetTopicAttributesRequest
            {
                TopicArn = topicArn,
                AttributeName = attributeName,
                AttributeValue = attributeValue
            });
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to set topic attribute '{attributeName}' on '{topicArn}'");
        }
    }

    /// <summary>
    /// Get attributes of an SNS subscription.
    /// </summary>
    public static async Task<SubscriptionAttributesResult> GetSubscriptionAttributesAsync(
        string subscriptionArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetSubscriptionAttributesAsync(
                new GetSubscriptionAttributesRequest { SubscriptionArn = subscriptionArn });
            return new SubscriptionAttributesResult(Attributes: resp.Attributes);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get subscription attributes for '{subscriptionArn}'");
        }
    }

    /// <summary>
    /// Set an attribute on an SNS subscription.
    /// </summary>
    public static async Task SetSubscriptionAttributesAsync(
        string subscriptionArn,
        string attributeName,
        string attributeValue,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.SetSubscriptionAttributesAsync(new SetSubscriptionAttributesRequest
            {
                SubscriptionArn = subscriptionArn,
                AttributeName = attributeName,
                AttributeValue = attributeValue
            });
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to set subscription attribute '{attributeName}' on '{subscriptionArn}'");
        }
    }

    /// <summary>
    /// Confirm a pending subscription by token.
    /// </summary>
    public static async Task<ConfirmSubscriptionResult> ConfirmSubscriptionAsync(
        string topicArn,
        string token,
        bool? authenticateOnUnsubscribe = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ConfirmSubscriptionRequest
        {
            TopicArn = topicArn,
            Token = token
        };
        if (authenticateOnUnsubscribe.HasValue)
            request.AuthenticateOnUnsubscribe = authenticateOnUnsubscribe.Value ? "true" : "false";

        try
        {
            var resp = await client.ConfirmSubscriptionAsync(request);
            return new ConfirmSubscriptionResult(SubscriptionArn: resp.SubscriptionArn);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to confirm subscription for topic '{topicArn}'");
        }
    }

    /// <summary>
    /// Add tags to an SNS resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceArn,
        Dictionary<string, string> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                ResourceArn = resourceArn,
                Tags = tags.Select(kv => new Tag { Key = kv.Key, Value = kv.Value }).ToList()
            });
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from an SNS resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceArn = resourceArn,
                TagKeys = tagKeys
            });
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for an SNS resource.
    /// </summary>
    public static async Task<SnsListTagsResult> ListTagsForResourceAsync(
        string resourceArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            var tags = resp.Tags
                .Select(t => new KeyValuePair<string, string>(t.Key, t.Value))
                .ToList();
            return new SnsListTagsResult(Tags: tags);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Create a platform application for push notifications.
    /// </summary>
    public static async Task<CreatePlatformApplicationResult> CreatePlatformApplicationAsync(
        string name,
        string platform,
        Dictionary<string, string> attributes,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreatePlatformApplicationAsync(
                new CreatePlatformApplicationRequest
                {
                    Name = name,
                    Platform = platform,
                    Attributes = attributes
                });
            return new CreatePlatformApplicationResult(
                PlatformApplicationArn: resp.PlatformApplicationArn);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create platform application '{name}'");
        }
    }

    /// <summary>
    /// Create a platform endpoint for push notifications.
    /// </summary>
    public static async Task<CreatePlatformEndpointResult> CreatePlatformEndpointAsync(
        string platformApplicationArn,
        string token,
        string? customUserData = null,
        Dictionary<string, string>? attributes = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreatePlatformEndpointRequest
        {
            PlatformApplicationArn = platformApplicationArn,
            Token = token
        };
        if (customUserData != null) request.CustomUserData = customUserData;
        if (attributes != null) request.Attributes = attributes;

        try
        {
            var resp = await client.CreatePlatformEndpointAsync(request);
            return new CreatePlatformEndpointResult(EndpointArn: resp.EndpointArn);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create platform endpoint");
        }
    }

    /// <summary>
    /// Publish a batch of messages to an SNS topic.
    /// </summary>
    public static async Task<PublishBatchResult> PublishBatchAsync(
        string topicArn,
        List<PublishBatchRequestEntry> entries,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PublishBatchAsync(new PublishBatchRequest
            {
                TopicArn = topicArn,
                PublishBatchRequestEntries = entries
            });
            return new PublishBatchResult(
                Successful: resp.Successful,
                Failed: resp.Failed);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to publish batch to topic '{topicArn}'");
        }
    }

    /// <summary>
    /// Set default SMS attributes for the account.
    /// </summary>
    public static async Task SetSmsAttributesAsync(
        Dictionary<string, string> attributes,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.SetSMSAttributesAsync(new SetSMSAttributesRequest
            {
                Attributes = attributes
            });
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to set SMS attributes");
        }
    }

    /// <summary>
    /// Check if a phone number is opted out of receiving SMS messages.
    /// </summary>
    public static async Task<CheckIfPhoneNumberIsOptedOutResult> CheckIfPhoneNumberIsOptedOutAsync(
        string phoneNumber, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CheckIfPhoneNumberIsOptedOutAsync(
                new CheckIfPhoneNumberIsOptedOutRequest { PhoneNumber = phoneNumber });
            return new CheckIfPhoneNumberIsOptedOutResult(IsOptedOut: resp.IsOptedOut ?? false);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to check opted-out status for '{phoneNumber}'");
        }
    }

    /// <summary>
    /// List phone numbers that have opted out of SMS messages, automatically paginating.
    /// </summary>
    public static async Task<ListPhoneNumbersOptedOutResult> ListPhoneNumbersOptedOutAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var phoneNumbers = new List<string>();
        var request = new ListPhoneNumbersOptedOutRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListPhoneNumbersOptedOutAsync(request);
                phoneNumbers.AddRange(resp.PhoneNumbers);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list opted-out phone numbers");
        }

        return new ListPhoneNumbersOptedOutResult(PhoneNumbers: phoneNumbers);
    }

    /// <summary>
    /// Opt in a phone number that was previously opted out of SMS messages.
    /// </summary>
    public static async Task OptInPhoneNumberAsync(
        string phoneNumber, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.OptInPhoneNumberAsync(
                new OptInPhoneNumberRequest { PhoneNumber = phoneNumber });
        }
        catch (AmazonSimpleNotificationServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to opt in phone number '{phoneNumber}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="PublishAsync"/>.</summary>
    public static PublishResult Publish(string message, string? topicArn = null, string? targetArn = null, string? phoneNumber = null, string? subject = null, Dictionary<string, MessageAttributeValue>? messageAttributes = null, string? messageStructure = null, string? messageDeduplicationId = null, string? messageGroupId = null, RegionEndpoint? region = null)
        => PublishAsync(message, topicArn, targetArn, phoneNumber, subject, messageAttributes, messageStructure, messageDeduplicationId, messageGroupId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateTopicAsync"/>.</summary>
    public static CreateTopicResult CreateTopic(string name, Dictionary<string, string>? attributes = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateTopicAsync(name, attributes, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteTopicAsync"/>.</summary>
    public static void DeleteTopic(string topicArn, RegionEndpoint? region = null)
        => DeleteTopicAsync(topicArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SubscribeAsync"/>.</summary>
    public static SubscribeResult Subscribe(string topicArn, string protocol, string? endpoint = null, Dictionary<string, string>? attributes = null, bool returnSubscriptionArn = true, RegionEndpoint? region = null)
        => SubscribeAsync(topicArn, protocol, endpoint, attributes, returnSubscriptionArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UnsubscribeAsync"/>.</summary>
    public static void Unsubscribe(string subscriptionArn, RegionEndpoint? region = null)
        => UnsubscribeAsync(subscriptionArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTopicsAsync"/>.</summary>
    public static ListTopicsResult ListTopics(RegionEndpoint? region = null)
        => ListTopicsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListSubscriptionsAsync"/>.</summary>
    public static ListSubscriptionsResult ListSubscriptions(RegionEndpoint? region = null)
        => ListSubscriptionsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListSubscriptionsByTopicAsync"/>.</summary>
    public static ListSubscriptionsResult ListSubscriptionsByTopic(string topicArn, RegionEndpoint? region = null)
        => ListSubscriptionsByTopicAsync(topicArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetTopicAttributesAsync"/>.</summary>
    public static TopicAttributesResult GetTopicAttributes(string topicArn, RegionEndpoint? region = null)
        => GetTopicAttributesAsync(topicArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SetTopicAttributesAsync"/>.</summary>
    public static void SetTopicAttributes(string topicArn, string attributeName, string attributeValue, RegionEndpoint? region = null)
        => SetTopicAttributesAsync(topicArn, attributeName, attributeValue, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetSubscriptionAttributesAsync"/>.</summary>
    public static SubscriptionAttributesResult GetSubscriptionAttributes(string subscriptionArn, RegionEndpoint? region = null)
        => GetSubscriptionAttributesAsync(subscriptionArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SetSubscriptionAttributesAsync"/>.</summary>
    public static void SetSubscriptionAttributes(string subscriptionArn, string attributeName, string attributeValue, RegionEndpoint? region = null)
        => SetSubscriptionAttributesAsync(subscriptionArn, attributeName, attributeValue, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ConfirmSubscriptionAsync"/>.</summary>
    public static ConfirmSubscriptionResult ConfirmSubscription(string topicArn, string token, bool? authenticateOnUnsubscribe = null, RegionEndpoint? region = null)
        => ConfirmSubscriptionAsync(topicArn, token, authenticateOnUnsubscribe, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static SnsListTagsResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreatePlatformApplicationAsync"/>.</summary>
    public static CreatePlatformApplicationResult CreatePlatformApplication(string name, string platform, Dictionary<string, string> attributes, RegionEndpoint? region = null)
        => CreatePlatformApplicationAsync(name, platform, attributes, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreatePlatformEndpointAsync"/>.</summary>
    public static CreatePlatformEndpointResult CreatePlatformEndpoint(string platformApplicationArn, string token, string? customUserData = null, Dictionary<string, string>? attributes = null, RegionEndpoint? region = null)
        => CreatePlatformEndpointAsync(platformApplicationArn, token, customUserData, attributes, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PublishBatchAsync"/>.</summary>
    public static PublishBatchResult PublishBatch(string topicArn, List<PublishBatchRequestEntry> entries, RegionEndpoint? region = null)
        => PublishBatchAsync(topicArn, entries, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SetSmsAttributesAsync"/>.</summary>
    public static void SetSmsAttributes(Dictionary<string, string> attributes, RegionEndpoint? region = null)
        => SetSmsAttributesAsync(attributes, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CheckIfPhoneNumberIsOptedOutAsync"/>.</summary>
    public static CheckIfPhoneNumberIsOptedOutResult CheckIfPhoneNumberIsOptedOut(string phoneNumber, RegionEndpoint? region = null)
        => CheckIfPhoneNumberIsOptedOutAsync(phoneNumber, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListPhoneNumbersOptedOutAsync"/>.</summary>
    public static ListPhoneNumbersOptedOutResult ListPhoneNumbersOptedOut(RegionEndpoint? region = null)
        => ListPhoneNumbersOptedOutAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="OptInPhoneNumberAsync"/>.</summary>
    public static void OptInPhoneNumber(string phoneNumber, RegionEndpoint? region = null)
        => OptInPhoneNumberAsync(phoneNumber, region).GetAwaiter().GetResult();

}

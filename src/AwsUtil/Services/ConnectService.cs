using Amazon;
using Amazon.Connect;
using Amazon.Connect.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for Amazon Connect operations.
/// </summary>
public sealed record CreateConnectInstanceResult(string? Id = null, string? Arn = null);
public sealed record DescribeConnectInstanceResult(Instance? Instance = null);
public sealed record ListConnectInstancesResult(List<InstanceSummary>? InstanceSummaryList = null);
public sealed record DescribeInstanceAttributeResult(Amazon.Connect.Model.Attribute? AttributeValue = null);
public sealed record ListInstanceAttributesResult(List<Amazon.Connect.Model.Attribute>? Attributes = null);
public sealed record CreateConnectUserResult(string? UserId = null, string? UserArn = null);
public sealed record DescribeConnectUserResult(User? User = null);
public sealed record ListConnectUsersResult(List<UserSummary>? UserSummaryList = null);
public sealed record CreateContactFlowResult(string? ContactFlowId = null, string? ContactFlowArn = null);
public sealed record DescribeContactFlowResult(ContactFlow? ContactFlow = null);
public sealed record ListContactFlowsResult(List<ContactFlowSummary>? ContactFlowSummaryList = null);
public sealed record CreateConnectQueueResult(string? QueueId = null, string? QueueArn = null);
public sealed record DescribeConnectQueueResult(Amazon.Connect.Model.Queue? Queue = null);
public sealed record ListConnectQueuesResult(List<QueueSummary>? QueueSummaryList = null);
public sealed record CreateRoutingProfileResult(string? RoutingProfileId = null, string? RoutingProfileArn = null);
public sealed record DescribeRoutingProfileResult(RoutingProfile? RoutingProfile = null);
public sealed record ListRoutingProfilesResult(List<RoutingProfileSummary>? RoutingProfileSummaryList = null);
public sealed record StartOutboundVoiceContactResult(string? ContactId = null);
public sealed record GetCurrentMetricDataResult(List<CurrentMetricResult>? MetricResults = null);
public sealed record GetMetricDataResult(List<HistoricalMetricResult>? MetricResults = null);
public sealed record DescribeContactResult(Contact? Contact = null);
public sealed record ListContactsResult(List<ContactSearchSummary>? Contacts = null);
public sealed record SearchContactsResult(List<ContactSearchSummary>? Contacts = null);
public sealed record ConnectTagResourceResult(bool Success = true);
public sealed record ConnectUntagResourceResult(bool Success = true);
public sealed record ListConnectTagsResult(Dictionary<string, string>? Tags = null);

/// <summary>
/// Utility helpers for Amazon Connect.
/// </summary>
public static class ConnectService
{
    private static AmazonConnectClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonConnectClient>(region);

    // ──────────────────────────── Instances ────────────────────────────

    /// <summary>
    /// Create an Amazon Connect instance.
    /// </summary>
    public static async Task<CreateConnectInstanceResult> CreateInstanceAsync(
        CreateInstanceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateInstanceAsync(request);
            return new CreateConnectInstanceResult(Id: resp.Id, Arn: resp.Arn);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create Connect instance");
        }
    }

    /// <summary>
    /// Delete an Amazon Connect instance.
    /// </summary>
    public static async Task DeleteInstanceAsync(
        string instanceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteInstanceAsync(
                new DeleteInstanceRequest { InstanceId = instanceId });
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Connect instance '{instanceId}'");
        }
    }

    /// <summary>
    /// Describe an Amazon Connect instance.
    /// </summary>
    public static async Task<DescribeConnectInstanceResult> DescribeInstanceAsync(
        string instanceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeInstanceAsync(
                new DescribeInstanceRequest { InstanceId = instanceId });
            return new DescribeConnectInstanceResult(Instance: resp.Instance);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Connect instance '{instanceId}'");
        }
    }

    /// <summary>
    /// List Amazon Connect instances, automatically paginating.
    /// </summary>
    public static async Task<ListConnectInstancesResult> ListInstancesAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var instances = new List<InstanceSummary>();
        var request = new ListInstancesRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListInstancesAsync(request);
                if (resp.InstanceSummaryList != null)
                    instances.AddRange(resp.InstanceSummaryList);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list Connect instances");
        }

        return new ListConnectInstancesResult(InstanceSummaryList: instances);
    }

    /// <summary>
    /// Update an instance attribute.
    /// </summary>
    public static async Task UpdateInstanceAttributeAsync(
        UpdateInstanceAttributeRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateInstanceAttributeAsync(request);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Connect instance attribute for '{request.InstanceId}'");
        }
    }

    /// <summary>
    /// Describe an instance attribute.
    /// </summary>
    public static async Task<DescribeInstanceAttributeResult> DescribeInstanceAttributeAsync(
        string instanceId,
        string attributeType,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeInstanceAttributeAsync(
                new DescribeInstanceAttributeRequest
                {
                    InstanceId = instanceId,
                    AttributeType = attributeType
                });
            return new DescribeInstanceAttributeResult(AttributeValue: resp.Attribute);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Connect instance attribute '{attributeType}'");
        }
    }

    /// <summary>
    /// List instance attributes, automatically paginating.
    /// </summary>
    public static async Task<ListInstanceAttributesResult> ListInstanceAttributesAsync(
        string instanceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var attributes = new List<Amazon.Connect.Model.Attribute>();
        var request = new ListInstanceAttributesRequest { InstanceId = instanceId };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListInstanceAttributesAsync(request);
                if (resp.Attributes != null) attributes.AddRange(resp.Attributes);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list Connect instance attributes for '{instanceId}'");
        }

        return new ListInstanceAttributesResult(Attributes: attributes);
    }

    // ──────────────────────────── Users ────────────────────────────

    /// <summary>
    /// Create a Connect user.
    /// </summary>
    public static async Task<CreateConnectUserResult> CreateUserAsync(
        CreateUserRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateUserAsync(request);
            return new CreateConnectUserResult(
                UserId: resp.UserId,
                UserArn: resp.UserArn);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Connect user '{request.Username}'");
        }
    }

    /// <summary>
    /// Delete a Connect user.
    /// </summary>
    public static async Task DeleteUserAsync(
        string instanceId,
        string userId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteUserAsync(new DeleteUserRequest
            {
                InstanceId = instanceId,
                UserId = userId
            });
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Connect user '{userId}'");
        }
    }

    /// <summary>
    /// Describe a Connect user.
    /// </summary>
    public static async Task<DescribeConnectUserResult> DescribeUserAsync(
        string instanceId,
        string userId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeUserAsync(new DescribeUserRequest
            {
                InstanceId = instanceId,
                UserId = userId
            });
            return new DescribeConnectUserResult(User: resp.User);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Connect user '{userId}'");
        }
    }

    /// <summary>
    /// List Connect users, automatically paginating.
    /// </summary>
    public static async Task<ListConnectUsersResult> ListUsersAsync(
        string instanceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var users = new List<UserSummary>();
        var request = new ListUsersRequest { InstanceId = instanceId };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListUsersAsync(request);
                if (resp.UserSummaryList != null) users.AddRange(resp.UserSummaryList);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list Connect users for instance '{instanceId}'");
        }

        return new ListConnectUsersResult(UserSummaryList: users);
    }

    /// <summary>
    /// Update user identity info.
    /// </summary>
    public static async Task UpdateUserIdentityInfoAsync(
        UpdateUserIdentityInfoRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateUserIdentityInfoAsync(request);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Connect user identity info for '{request.UserId}'");
        }
    }

    /// <summary>
    /// Update user phone config.
    /// </summary>
    public static async Task UpdateUserPhoneConfigAsync(
        UpdateUserPhoneConfigRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateUserPhoneConfigAsync(request);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Connect user phone config for '{request.UserId}'");
        }
    }

    /// <summary>
    /// Update user routing profile.
    /// </summary>
    public static async Task UpdateUserRoutingProfileAsync(
        UpdateUserRoutingProfileRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateUserRoutingProfileAsync(request);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Connect user routing profile for '{request.UserId}'");
        }
    }

    // ──────────────────────────── Contact Flows ────────────────────────────

    /// <summary>
    /// Create a contact flow.
    /// </summary>
    public static async Task<CreateContactFlowResult> CreateContactFlowAsync(
        CreateContactFlowRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateContactFlowAsync(request);
            return new CreateContactFlowResult(
                ContactFlowId: resp.ContactFlowId,
                ContactFlowArn: resp.ContactFlowArn);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Connect contact flow '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a contact flow.
    /// </summary>
    public static async Task DeleteContactFlowAsync(
        string instanceId,
        string contactFlowId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteContactFlowAsync(new DeleteContactFlowRequest
            {
                InstanceId = instanceId,
                ContactFlowId = contactFlowId
            });
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Connect contact flow '{contactFlowId}'");
        }
    }

    /// <summary>
    /// Describe a contact flow.
    /// </summary>
    public static async Task<DescribeContactFlowResult> DescribeContactFlowAsync(
        string instanceId,
        string contactFlowId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeContactFlowAsync(
                new DescribeContactFlowRequest
                {
                    InstanceId = instanceId,
                    ContactFlowId = contactFlowId
                });
            return new DescribeContactFlowResult(ContactFlow: resp.ContactFlow);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Connect contact flow '{contactFlowId}'");
        }
    }

    /// <summary>
    /// List contact flows, automatically paginating.
    /// </summary>
    public static async Task<ListContactFlowsResult> ListContactFlowsAsync(
        string instanceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var flows = new List<ContactFlowSummary>();
        var request = new ListContactFlowsRequest { InstanceId = instanceId };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListContactFlowsAsync(request);
                if (resp.ContactFlowSummaryList != null)
                    flows.AddRange(resp.ContactFlowSummaryList);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list Connect contact flows for '{instanceId}'");
        }

        return new ListContactFlowsResult(ContactFlowSummaryList: flows);
    }

    /// <summary>
    /// Update contact flow content.
    /// </summary>
    public static async Task UpdateContactFlowContentAsync(
        UpdateContactFlowContentRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateContactFlowContentAsync(request);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Connect contact flow content for '{request.ContactFlowId}'");
        }
    }

    /// <summary>
    /// Update contact flow name and description.
    /// </summary>
    public static async Task UpdateContactFlowNameAsync(
        UpdateContactFlowNameRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateContactFlowNameAsync(request);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Connect contact flow name for '{request.ContactFlowId}'");
        }
    }

    // ──────────────────────────── Queues ────────────────────────────

    /// <summary>
    /// Create a Connect queue.
    /// </summary>
    public static async Task<CreateConnectQueueResult> CreateQueueAsync(
        CreateQueueRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateQueueAsync(request);
            return new CreateConnectQueueResult(
                QueueId: resp.QueueId,
                QueueArn: resp.QueueArn);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Connect queue '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a Connect queue.
    /// </summary>
    public static async Task DeleteQueueAsync(
        string instanceId,
        string queueId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteQueueAsync(new DeleteQueueRequest
            {
                InstanceId = instanceId,
                QueueId = queueId
            });
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Connect queue '{queueId}'");
        }
    }

    /// <summary>
    /// Describe a Connect queue.
    /// </summary>
    public static async Task<DescribeConnectQueueResult> DescribeQueueAsync(
        string instanceId,
        string queueId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeQueueAsync(new DescribeQueueRequest
            {
                InstanceId = instanceId,
                QueueId = queueId
            });
            return new DescribeConnectQueueResult(Queue: resp.Queue);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Connect queue '{queueId}'");
        }
    }

    /// <summary>
    /// List Connect queues, automatically paginating.
    /// </summary>
    public static async Task<ListConnectQueuesResult> ListQueuesAsync(
        string instanceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var queues = new List<QueueSummary>();
        var request = new ListQueuesRequest { InstanceId = instanceId };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListQueuesAsync(request);
                if (resp.QueueSummaryList != null) queues.AddRange(resp.QueueSummaryList);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list Connect queues for '{instanceId}'");
        }

        return new ListConnectQueuesResult(QueueSummaryList: queues);
    }

    /// <summary>
    /// Update a Connect queue name.
    /// </summary>
    public static async Task UpdateQueueNameAsync(
        UpdateQueueNameRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateQueueNameAsync(request);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Connect queue name for '{request.QueueId}'");
        }
    }

    // ──────────────────────────── Routing Profiles ────────────────────────────

    /// <summary>
    /// Create a routing profile.
    /// </summary>
    public static async Task<CreateRoutingProfileResult> CreateRoutingProfileAsync(
        CreateRoutingProfileRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateRoutingProfileAsync(request);
            return new CreateRoutingProfileResult(
                RoutingProfileId: resp.RoutingProfileId,
                RoutingProfileArn: resp.RoutingProfileArn);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Connect routing profile '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a routing profile.
    /// </summary>
    public static async Task DeleteRoutingProfileAsync(
        string instanceId,
        string routingProfileId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteRoutingProfileAsync(new DeleteRoutingProfileRequest
            {
                InstanceId = instanceId,
                RoutingProfileId = routingProfileId
            });
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Connect routing profile '{routingProfileId}'");
        }
    }

    /// <summary>
    /// Describe a routing profile.
    /// </summary>
    public static async Task<DescribeRoutingProfileResult> DescribeRoutingProfileAsync(
        string instanceId,
        string routingProfileId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeRoutingProfileAsync(
                new DescribeRoutingProfileRequest
                {
                    InstanceId = instanceId,
                    RoutingProfileId = routingProfileId
                });
            return new DescribeRoutingProfileResult(RoutingProfile: resp.RoutingProfile);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Connect routing profile '{routingProfileId}'");
        }
    }

    /// <summary>
    /// List routing profiles, automatically paginating.
    /// </summary>
    public static async Task<ListRoutingProfilesResult> ListRoutingProfilesAsync(
        string instanceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var profiles = new List<RoutingProfileSummary>();
        var request = new ListRoutingProfilesRequest { InstanceId = instanceId };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListRoutingProfilesAsync(request);
                if (resp.RoutingProfileSummaryList != null)
                    profiles.AddRange(resp.RoutingProfileSummaryList);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list Connect routing profiles for '{instanceId}'");
        }

        return new ListRoutingProfilesResult(RoutingProfileSummaryList: profiles);
    }

    /// <summary>
    /// Update a routing profile name.
    /// </summary>
    public static async Task UpdateRoutingProfileNameAsync(
        UpdateRoutingProfileNameRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateRoutingProfileNameAsync(request);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Connect routing profile name for '{request.RoutingProfileId}'");
        }
    }

    // ──────────────────────────── Voice Contacts ────────────────────────────

    /// <summary>
    /// Start an outbound voice contact.
    /// </summary>
    public static async Task<StartOutboundVoiceContactResult> StartOutboundVoiceContactAsync(
        StartOutboundVoiceContactRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartOutboundVoiceContactAsync(request);
            return new StartOutboundVoiceContactResult(ContactId: resp.ContactId);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start outbound voice contact");
        }
    }

    /// <summary>
    /// Stop an active contact.
    /// </summary>
    public static async Task StopContactAsync(
        string instanceId,
        string contactId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StopContactAsync(new StopContactRequest
            {
                InstanceId = instanceId,
                ContactId = contactId
            });
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop Connect contact '{contactId}'");
        }
    }

    // ──────────────────────────── Metrics ────────────────────────────

    /// <summary>
    /// Get current metric data for a Connect instance.
    /// </summary>
    public static async Task<GetCurrentMetricDataResult> GetCurrentMetricDataAsync(
        GetCurrentMetricDataRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetCurrentMetricDataAsync(request);
            return new GetCurrentMetricDataResult(MetricResults: resp.MetricResults);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get Connect current metric data");
        }
    }

    /// <summary>
    /// Get historical metric data for a Connect instance.
    /// </summary>
    public static async Task<GetMetricDataResult> GetMetricDataAsync(
        GetMetricDataRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetMetricDataAsync(request);
            return new GetMetricDataResult(MetricResults: resp.MetricResults);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get Connect metric data");
        }
    }

    // ──────────────────────────── Contacts ────────────────────────────

    /// <summary>
    /// Describe a contact.
    /// </summary>
    public static async Task<DescribeContactResult> DescribeContactAsync(
        string instanceId,
        string contactId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeContactAsync(new DescribeContactRequest
            {
                InstanceId = instanceId,
                ContactId = contactId
            });
            return new DescribeContactResult(Contact: resp.Contact);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Connect contact '{contactId}'");
        }
    }

    /// <summary>
    /// Search contacts.
    /// </summary>
    public static async Task<SearchContactsResult> SearchContactsAsync(
        SearchContactsRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.SearchContactsAsync(request);
            return new SearchContactsResult(Contacts: resp.Contacts);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to search Connect contacts");
        }
    }

    // ──────────────────────────── Tags ────────────────────────────

    /// <summary>
    /// Add tags to a Connect resource.
    /// </summary>
    public static async Task<ConnectTagResourceResult> TagResourceAsync(
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
                Tags = tags
            });
            return new ConnectTagResourceResult();
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Connect resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Connect resource.
    /// </summary>
    public static async Task<ConnectUntagResourceResult> UntagResourceAsync(
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
            return new ConnectUntagResourceResult();
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Connect resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Connect resource.
    /// </summary>
    public static async Task<ListConnectTagsResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new ListConnectTagsResult(Tags: resp.Tags);
        }
        catch (AmazonConnectException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Connect resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateInstanceAsync"/>.</summary>
    public static CreateConnectInstanceResult CreateInstance(CreateInstanceRequest request, RegionEndpoint? region = null)
        => CreateInstanceAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteInstanceAsync"/>.</summary>
    public static void DeleteInstance(string instanceId, RegionEndpoint? region = null)
        => DeleteInstanceAsync(instanceId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeInstanceAsync"/>.</summary>
    public static DescribeConnectInstanceResult DescribeInstance(string instanceId, RegionEndpoint? region = null)
        => DescribeInstanceAsync(instanceId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListInstancesAsync"/>.</summary>
    public static ListConnectInstancesResult ListInstances(RegionEndpoint? region = null)
        => ListInstancesAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateInstanceAttributeAsync"/>.</summary>
    public static void UpdateInstanceAttribute(UpdateInstanceAttributeRequest request, RegionEndpoint? region = null)
        => UpdateInstanceAttributeAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeInstanceAttributeAsync"/>.</summary>
    public static DescribeInstanceAttributeResult DescribeInstanceAttribute(string instanceId, string attributeType, RegionEndpoint? region = null)
        => DescribeInstanceAttributeAsync(instanceId, attributeType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListInstanceAttributesAsync"/>.</summary>
    public static ListInstanceAttributesResult ListInstanceAttributes(string instanceId, RegionEndpoint? region = null)
        => ListInstanceAttributesAsync(instanceId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateUserAsync"/>.</summary>
    public static CreateConnectUserResult CreateUser(CreateUserRequest request, RegionEndpoint? region = null)
        => CreateUserAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteUserAsync"/>.</summary>
    public static void DeleteUser(string instanceId, string userId, RegionEndpoint? region = null)
        => DeleteUserAsync(instanceId, userId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeUserAsync"/>.</summary>
    public static DescribeConnectUserResult DescribeUser(string instanceId, string userId, RegionEndpoint? region = null)
        => DescribeUserAsync(instanceId, userId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListUsersAsync"/>.</summary>
    public static ListConnectUsersResult ListUsers(string instanceId, RegionEndpoint? region = null)
        => ListUsersAsync(instanceId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateUserIdentityInfoAsync"/>.</summary>
    public static void UpdateUserIdentityInfo(UpdateUserIdentityInfoRequest request, RegionEndpoint? region = null)
        => UpdateUserIdentityInfoAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateUserPhoneConfigAsync"/>.</summary>
    public static void UpdateUserPhoneConfig(UpdateUserPhoneConfigRequest request, RegionEndpoint? region = null)
        => UpdateUserPhoneConfigAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateUserRoutingProfileAsync"/>.</summary>
    public static void UpdateUserRoutingProfile(UpdateUserRoutingProfileRequest request, RegionEndpoint? region = null)
        => UpdateUserRoutingProfileAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateContactFlowAsync"/>.</summary>
    public static CreateContactFlowResult CreateContactFlow(CreateContactFlowRequest request, RegionEndpoint? region = null)
        => CreateContactFlowAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteContactFlowAsync"/>.</summary>
    public static void DeleteContactFlow(string instanceId, string contactFlowId, RegionEndpoint? region = null)
        => DeleteContactFlowAsync(instanceId, contactFlowId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeContactFlowAsync"/>.</summary>
    public static DescribeContactFlowResult DescribeContactFlow(string instanceId, string contactFlowId, RegionEndpoint? region = null)
        => DescribeContactFlowAsync(instanceId, contactFlowId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListContactFlowsAsync"/>.</summary>
    public static ListContactFlowsResult ListContactFlows(string instanceId, RegionEndpoint? region = null)
        => ListContactFlowsAsync(instanceId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateContactFlowContentAsync"/>.</summary>
    public static void UpdateContactFlowContent(UpdateContactFlowContentRequest request, RegionEndpoint? region = null)
        => UpdateContactFlowContentAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateContactFlowNameAsync"/>.</summary>
    public static void UpdateContactFlowName(UpdateContactFlowNameRequest request, RegionEndpoint? region = null)
        => UpdateContactFlowNameAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateQueueAsync"/>.</summary>
    public static CreateConnectQueueResult CreateQueue(CreateQueueRequest request, RegionEndpoint? region = null)
        => CreateQueueAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteQueueAsync"/>.</summary>
    public static void DeleteQueue(string instanceId, string queueId, RegionEndpoint? region = null)
        => DeleteQueueAsync(instanceId, queueId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeQueueAsync"/>.</summary>
    public static DescribeConnectQueueResult DescribeQueue(string instanceId, string queueId, RegionEndpoint? region = null)
        => DescribeQueueAsync(instanceId, queueId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListQueuesAsync"/>.</summary>
    public static ListConnectQueuesResult ListQueues(string instanceId, RegionEndpoint? region = null)
        => ListQueuesAsync(instanceId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateQueueNameAsync"/>.</summary>
    public static void UpdateQueueName(UpdateQueueNameRequest request, RegionEndpoint? region = null)
        => UpdateQueueNameAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateRoutingProfileAsync"/>.</summary>
    public static CreateRoutingProfileResult CreateRoutingProfile(CreateRoutingProfileRequest request, RegionEndpoint? region = null)
        => CreateRoutingProfileAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteRoutingProfileAsync"/>.</summary>
    public static void DeleteRoutingProfile(string instanceId, string routingProfileId, RegionEndpoint? region = null)
        => DeleteRoutingProfileAsync(instanceId, routingProfileId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeRoutingProfileAsync"/>.</summary>
    public static DescribeRoutingProfileResult DescribeRoutingProfile(string instanceId, string routingProfileId, RegionEndpoint? region = null)
        => DescribeRoutingProfileAsync(instanceId, routingProfileId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListRoutingProfilesAsync"/>.</summary>
    public static ListRoutingProfilesResult ListRoutingProfiles(string instanceId, RegionEndpoint? region = null)
        => ListRoutingProfilesAsync(instanceId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateRoutingProfileNameAsync"/>.</summary>
    public static void UpdateRoutingProfileName(UpdateRoutingProfileNameRequest request, RegionEndpoint? region = null)
        => UpdateRoutingProfileNameAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartOutboundVoiceContactAsync"/>.</summary>
    public static StartOutboundVoiceContactResult StartOutboundVoiceContact(StartOutboundVoiceContactRequest request, RegionEndpoint? region = null)
        => StartOutboundVoiceContactAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopContactAsync"/>.</summary>
    public static void StopContact(string instanceId, string contactId, RegionEndpoint? region = null)
        => StopContactAsync(instanceId, contactId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetCurrentMetricDataAsync"/>.</summary>
    public static GetCurrentMetricDataResult GetCurrentMetricData(GetCurrentMetricDataRequest request, RegionEndpoint? region = null)
        => GetCurrentMetricDataAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetMetricDataAsync"/>.</summary>
    public static GetMetricDataResult GetMetricData(GetMetricDataRequest request, RegionEndpoint? region = null)
        => GetMetricDataAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeContactAsync"/>.</summary>
    public static DescribeContactResult DescribeContact(string instanceId, string contactId, RegionEndpoint? region = null)
        => DescribeContactAsync(instanceId, contactId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SearchContactsAsync"/>.</summary>
    public static SearchContactsResult SearchContacts(SearchContactsRequest request, RegionEndpoint? region = null)
        => SearchContactsAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static ConnectTagResourceResult TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static ConnectUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static ListConnectTagsResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}

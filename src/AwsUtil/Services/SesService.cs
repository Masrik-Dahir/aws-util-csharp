using Amazon;
using Amazon.SimpleEmail;
using Amazon.SimpleEmail.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for SES operations.
/// </summary>
public sealed record SendEmailResult(string? MessageId = null);

public sealed record SendRawEmailResult(string? MessageId = null);

public sealed record ListIdentitiesResult(List<string>? Identities = null, string? NextToken = null);

public sealed record VerificationAttributesResult(
    Dictionary<string, IdentityVerificationAttributes>? Attributes = null);

public sealed record SendQuotaResult(
    double Max24HourSend,
    double MaxSendRate,
    double SentLast24Hours);

public sealed record SendStatisticsResult(List<SendDataPoint>? SendDataPoints = null);

public sealed record CreateReceiptRuleSetResult(bool Success);

public sealed record SendTemplatedEmailResult(string? MessageId = null);

public sealed record SendBulkTemplatedEmailResult(List<BulkEmailDestinationStatus>? Statuses = null);

/// <summary>
/// Utility helpers for Amazon Simple Email Service (SES v1).
/// </summary>
public static class SesService
{
    private static AmazonSimpleEmailServiceClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSimpleEmailServiceClient>(region);

    /// <summary>
    /// Send a formatted email via SES.
    /// </summary>
    public static async Task<SendEmailResult> SendEmailAsync(
        string source,
        Destination destination,
        Message message,
        string? replyToAddress = null,
        string? returnPath = null,
        string? configurationSetName = null,
        List<MessageTag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SendEmailRequest
        {
            Source = source,
            Destination = destination,
            Message = message
        };
        if (replyToAddress != null) request.ReplyToAddresses = [replyToAddress];
        if (returnPath != null) request.ReturnPath = returnPath;
        if (configurationSetName != null) request.ConfigurationSetName = configurationSetName;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.SendEmailAsync(request);
            return new SendEmailResult(MessageId: resp.MessageId);
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to send email");
        }
    }

    /// <summary>
    /// Send a raw email via SES.
    /// </summary>
    public static async Task<SendRawEmailResult> SendRawEmailAsync(
        RawMessage rawMessage,
        string? source = null,
        List<string>? destinations = null,
        string? configurationSetName = null,
        List<MessageTag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SendRawEmailRequest { RawMessage = rawMessage };
        if (source != null) request.Source = source;
        if (destinations != null) request.Destinations = destinations;
        if (configurationSetName != null) request.ConfigurationSetName = configurationSetName;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.SendRawEmailAsync(request);
            return new SendRawEmailResult(MessageId: resp.MessageId);
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to send raw email");
        }
    }

    /// <summary>
    /// Start verification of an email identity.
    /// </summary>
    public static async Task VerifyEmailIdentityAsync(
        string emailAddress, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.VerifyEmailIdentityAsync(
                new VerifyEmailIdentityRequest { EmailAddress = emailAddress });
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to verify email identity '{emailAddress}'");
        }
    }

    /// <summary>
    /// Delete a verified email identity.
    /// </summary>
    public static async Task DeleteIdentityAsync(
        string identity, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteIdentityAsync(
                new DeleteIdentityRequest { Identity = identity });
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete identity '{identity}'");
        }
    }

    /// <summary>
    /// List verified identities, automatically paginating.
    /// </summary>
    public static async Task<ListIdentitiesResult> ListIdentitiesAsync(
        string? identityType = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var identities = new List<string>();
        var request = new ListIdentitiesRequest();
        if (identityType != null) request.IdentityType = identityType;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListIdentitiesAsync(request);
                identities.AddRange(resp.Identities);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list identities");
        }

        return new ListIdentitiesResult(Identities: identities);
    }

    /// <summary>
    /// Get verification attributes for a list of identities.
    /// </summary>
    public static async Task<VerificationAttributesResult> GetIdentityVerificationAttributesAsync(
        List<string> identities, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetIdentityVerificationAttributesAsync(
                new GetIdentityVerificationAttributesRequest { Identities = identities });
            return new VerificationAttributesResult(Attributes: resp.VerificationAttributes);
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get identity verification attributes");
        }
    }

    /// <summary>
    /// Set the SNS topic for bounce, complaint, or delivery notifications on an identity.
    /// </summary>
    public static async Task SetIdentityNotificationTopicAsync(
        string identity,
        string notificationType,
        string? snsTopic = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SetIdentityNotificationTopicRequest
        {
            Identity = identity,
            NotificationType = notificationType
        };
        if (snsTopic != null) request.SnsTopic = snsTopic;

        try
        {
            await client.SetIdentityNotificationTopicAsync(request);
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to set notification topic for identity '{identity}'");
        }
    }

    /// <summary>
    /// Get the sending quota for the SES account.
    /// </summary>
    public static async Task<SendQuotaResult> GetSendQuotaAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetSendQuotaAsync(new GetSendQuotaRequest());
            return new SendQuotaResult(
                Max24HourSend: resp.Max24HourSend ?? 0.0,
                MaxSendRate: resp.MaxSendRate ?? 0.0,
                SentLast24Hours: resp.SentLast24Hours ?? 0.0);
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get send quota");
        }
    }

    /// <summary>
    /// Get sending statistics for the SES account.
    /// </summary>
    public static async Task<SendStatisticsResult> GetSendStatisticsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetSendStatisticsAsync(new GetSendStatisticsRequest());
            return new SendStatisticsResult(SendDataPoints: resp.SendDataPoints);
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get send statistics");
        }
    }

    /// <summary>
    /// Create a receipt rule within a rule set.
    /// </summary>
    public static async Task CreateReceiptRuleAsync(
        string ruleSetName,
        ReceiptRule rule,
        string? after = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateReceiptRuleRequest
        {
            RuleSetName = ruleSetName,
            Rule = rule
        };
        if (after != null) request.After = after;

        try
        {
            await client.CreateReceiptRuleAsync(request);
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create receipt rule in set '{ruleSetName}'");
        }
    }

    /// <summary>
    /// Delete a receipt rule from a rule set.
    /// </summary>
    public static async Task DeleteReceiptRuleAsync(
        string ruleSetName,
        string ruleName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteReceiptRuleAsync(new DeleteReceiptRuleRequest
            {
                RuleSetName = ruleSetName,
                RuleName = ruleName
            });
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete receipt rule '{ruleName}' from set '{ruleSetName}'");
        }
    }

    /// <summary>
    /// Create a receipt rule set.
    /// </summary>
    public static async Task CreateReceiptRuleSetAsync(
        string ruleSetName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateReceiptRuleSetAsync(
                new CreateReceiptRuleSetRequest { RuleSetName = ruleSetName });
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create receipt rule set '{ruleSetName}'");
        }
    }

    /// <summary>
    /// Set the active receipt rule set.
    /// </summary>
    public static async Task SetActiveReceiptRuleSetAsync(
        string? ruleSetName = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SetActiveReceiptRuleSetRequest();
        if (ruleSetName != null) request.RuleSetName = ruleSetName;

        try
        {
            await client.SetActiveReceiptRuleSetAsync(request);
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to set active receipt rule set");
        }
    }

    /// <summary>
    /// Create an email template.
    /// </summary>
    public static async Task CreateTemplateAsync(
        Template template, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateTemplateAsync(
                new CreateTemplateRequest { Template = template });
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create template '{template.TemplateName}'");
        }
    }

    /// <summary>
    /// Delete an email template.
    /// </summary>
    public static async Task DeleteTemplateAsync(
        string templateName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTemplateAsync(
                new DeleteTemplateRequest { TemplateName = templateName });
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete template '{templateName}'");
        }
    }

    /// <summary>
    /// Get an email template by name.
    /// </summary>
    public static async Task<Template> GetTemplateAsync(
        string templateName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetTemplateAsync(
                new GetTemplateRequest { TemplateName = templateName });
            return resp.Template;
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get template '{templateName}'");
        }
    }

    /// <summary>
    /// Update an existing email template.
    /// </summary>
    public static async Task UpdateTemplateAsync(
        Template template, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateTemplateAsync(
                new UpdateTemplateRequest { Template = template });
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update template '{template.TemplateName}'");
        }
    }

    /// <summary>
    /// Send an email using a template.
    /// </summary>
    public static async Task<SendTemplatedEmailResult> SendTemplatedEmailAsync(
        string source,
        Destination destination,
        string template,
        string templateData,
        string? replyToAddress = null,
        string? returnPath = null,
        string? configurationSetName = null,
        List<MessageTag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SendTemplatedEmailRequest
        {
            Source = source,
            Destination = destination,
            Template = template,
            TemplateData = templateData
        };
        if (replyToAddress != null) request.ReplyToAddresses = [replyToAddress];
        if (returnPath != null) request.ReturnPath = returnPath;
        if (configurationSetName != null) request.ConfigurationSetName = configurationSetName;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.SendTemplatedEmailAsync(request);
            return new SendTemplatedEmailResult(MessageId: resp.MessageId);
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to send templated email");
        }
    }

    /// <summary>
    /// Send bulk templated emails.
    /// </summary>
    public static async Task<SendBulkTemplatedEmailResult> SendBulkTemplatedEmailAsync(
        string source,
        string template,
        string defaultTemplateData,
        List<BulkEmailDestination> destinations,
        string? replyToAddress = null,
        string? returnPath = null,
        string? configurationSetName = null,
        List<MessageTag>? defaultTags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SendBulkTemplatedEmailRequest
        {
            Source = source,
            Template = template,
            DefaultTemplateData = defaultTemplateData,
            Destinations = destinations
        };
        if (replyToAddress != null) request.ReplyToAddresses = [replyToAddress];
        if (returnPath != null) request.ReturnPath = returnPath;
        if (configurationSetName != null) request.ConfigurationSetName = configurationSetName;
        if (defaultTags != null) request.DefaultTags = defaultTags;

        try
        {
            var resp = await client.SendBulkTemplatedEmailAsync(request);
            return new SendBulkTemplatedEmailResult(Statuses: resp.Status);
        }
        catch (AmazonSimpleEmailServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to send bulk templated email");
        }
    }
}

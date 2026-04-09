using Amazon;
using Amazon.SimpleEmailV2;
using Amazon.SimpleEmailV2.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for SES v2 operations.
/// </summary>
public sealed record SesV2SendEmailResult(string? MessageId = null);

public sealed record CreateEmailIdentityResult(
    string? IdentityType = null,
    bool? VerifiedForSendingStatus = null,
    DkimAttributes? DkimAttributes = null);

public sealed record GetEmailIdentityResult(
    string? IdentityType = null,
    bool? VerifiedForSendingStatus = null,
    DkimAttributes? DkimAttributes = null,
    MailFromAttributes? MailFromAttributes = null,
    Dictionary<string, string>? Policies = null,
    List<Amazon.SimpleEmailV2.Model.Tag>? Tags = null,
    string? ConfigurationSetName = null);

public sealed record ListEmailIdentitiesResult(
    List<IdentityInfo>? EmailIdentities = null,
    string? NextToken = null);

public sealed record GetContactResult(
    string? ContactListName = null,
    string? EmailAddress = null,
    bool? UnsubscribeAll = null,
    List<TopicPreference>? TopicPreferences = null,
    string? AttributesData = null,
    DateTime? CreatedTimestamp = null,
    DateTime? LastUpdatedTimestamp = null);

public sealed record SesV2ListContactsResult(
    List<Contact>? Contacts = null,
    string? NextToken = null);

public sealed record GetEmailTemplateResult(
    string? TemplateName = null,
    EmailTemplateContent? TemplateContent = null);

public sealed record ListEmailTemplatesResult(
    List<EmailTemplateMetadata>? Templates = null,
    string? NextToken = null);

public sealed record GetAccountResult(
    bool? DedicatedIpAutoWarmupEnabled = null,
    string? EnforcementStatus = null,
    bool? ProductionAccessEnabled = null,
    SendQuota? SendQuota = null,
    bool? SendingEnabled = null,
    SuppressionAttributes? SuppressionAttributes = null,
    AccountDetails? Details = null);

public sealed record GetConfigurationSetResult(
    string? ConfigurationSetName = null,
    TrackingOptions? TrackingOptions = null,
    DeliveryOptions? DeliveryOptions = null,
    ReputationOptions? ReputationOptions = null,
    SendingOptions? SendingOptions = null,
    List<Amazon.SimpleEmailV2.Model.Tag>? Tags = null,
    SuppressionOptions? SuppressionOptions = null,
    VdmOptions? VdmOptions = null);

public sealed record ListConfigurationSetsResult(
    List<string>? ConfigurationSets = null,
    string? NextToken = null);

/// <summary>
/// Utility helpers for Amazon Simple Email Service v2 (SES v2).
/// </summary>
public static class SesV2Service
{
    private static AmazonSimpleEmailServiceV2Client GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSimpleEmailServiceV2Client>(region);

    /// <summary>
    /// Send an email via SES v2.
    /// </summary>
    public static async Task<SesV2SendEmailResult> SendEmailAsync(
        EmailContent content,
        string? fromEmailAddress = null,
        Destination? destination = null,
        List<string>? replyToAddresses = null,
        string? feedbackForwardingEmailAddress = null,
        string? configurationSetName = null,
        List<MessageTag>? emailTags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SendEmailRequest { Content = content };
        if (fromEmailAddress != null) request.FromEmailAddress = fromEmailAddress;
        if (destination != null) request.Destination = destination;
        if (replyToAddresses != null) request.ReplyToAddresses = replyToAddresses;
        if (feedbackForwardingEmailAddress != null)
            request.FeedbackForwardingEmailAddress = feedbackForwardingEmailAddress;
        if (configurationSetName != null) request.ConfigurationSetName = configurationSetName;
        if (emailTags != null) request.EmailTags = emailTags;

        try
        {
            var resp = await client.SendEmailAsync(request);
            return new SesV2SendEmailResult(MessageId: resp.MessageId);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to send email via SES v2");
        }
    }

    /// <summary>
    /// Create an email identity (email address or domain) for verification.
    /// </summary>
    public static async Task<CreateEmailIdentityResult> CreateEmailIdentityAsync(
        string emailIdentity,
        List<Amazon.SimpleEmailV2.Model.Tag>? tags = null,
        DkimSigningAttributes? dkimSigningAttributes = null,
        string? configurationSetName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateEmailIdentityRequest { EmailIdentity = emailIdentity };
        if (tags != null) request.Tags = tags;
        if (dkimSigningAttributes != null) request.DkimSigningAttributes = dkimSigningAttributes;
        if (configurationSetName != null) request.ConfigurationSetName = configurationSetName;

        try
        {
            var resp = await client.CreateEmailIdentityAsync(request);
            return new CreateEmailIdentityResult(
                IdentityType: resp.IdentityType?.Value,
                VerifiedForSendingStatus: resp.VerifiedForSendingStatus,
                DkimAttributes: resp.DkimAttributes);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create email identity '{emailIdentity}'");
        }
    }

    /// <summary>
    /// Delete an email identity.
    /// </summary>
    public static async Task DeleteEmailIdentityAsync(
        string emailIdentity, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteEmailIdentityAsync(
                new DeleteEmailIdentityRequest { EmailIdentity = emailIdentity });
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete email identity '{emailIdentity}'");
        }
    }

    /// <summary>
    /// Get details about an email identity.
    /// </summary>
    public static async Task<GetEmailIdentityResult> GetEmailIdentityAsync(
        string emailIdentity, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetEmailIdentityAsync(
                new GetEmailIdentityRequest { EmailIdentity = emailIdentity });
            return new GetEmailIdentityResult(
                IdentityType: resp.IdentityType?.Value,
                VerifiedForSendingStatus: resp.VerifiedForSendingStatus,
                DkimAttributes: resp.DkimAttributes,
                MailFromAttributes: resp.MailFromAttributes,
                Policies: resp.Policies,
                Tags: resp.Tags,
                ConfigurationSetName: resp.ConfigurationSetName);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get email identity '{emailIdentity}'");
        }
    }

    /// <summary>
    /// List email identities, automatically paginating.
    /// </summary>
    public static async Task<ListEmailIdentitiesResult> ListEmailIdentitiesAsync(
        int? pageSize = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var identities = new List<IdentityInfo>();
        var request = new ListEmailIdentitiesRequest();
        if (pageSize.HasValue) request.PageSize = pageSize.Value;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListEmailIdentitiesAsync(request);
                identities.AddRange(resp.EmailIdentities);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list email identities");
        }

        return new ListEmailIdentitiesResult(EmailIdentities: identities);
    }

    /// <summary>
    /// Create a contact list.
    /// </summary>
    public static async Task CreateContactListAsync(
        string contactListName,
        string? description = null,
        List<Topic>? topics = null,
        List<Amazon.SimpleEmailV2.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateContactListRequest { ContactListName = contactListName };
        if (description != null) request.Description = description;
        if (topics != null) request.Topics = topics;
        if (tags != null) request.Tags = tags;

        try
        {
            await client.CreateContactListAsync(request);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create contact list '{contactListName}'");
        }
    }

    /// <summary>
    /// Delete a contact list.
    /// </summary>
    public static async Task DeleteContactListAsync(
        string contactListName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteContactListAsync(
                new DeleteContactListRequest { ContactListName = contactListName });
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete contact list '{contactListName}'");
        }
    }

    /// <summary>
    /// Get a contact from a contact list.
    /// </summary>
    public static async Task<GetContactResult> GetContactAsync(
        string contactListName,
        string emailAddress,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetContactAsync(new GetContactRequest
            {
                ContactListName = contactListName,
                EmailAddress = emailAddress
            });
            return new GetContactResult(
                ContactListName: resp.ContactListName,
                EmailAddress: resp.EmailAddress,
                UnsubscribeAll: resp.UnsubscribeAll,
                TopicPreferences: resp.TopicPreferences,
                AttributesData: resp.AttributesData,
                CreatedTimestamp: resp.CreatedTimestamp,
                LastUpdatedTimestamp: resp.LastUpdatedTimestamp);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get contact '{emailAddress}' from list '{contactListName}'");
        }
    }

    /// <summary>
    /// Create a contact in a contact list.
    /// </summary>
    public static async Task CreateContactAsync(
        string contactListName,
        string emailAddress,
        List<TopicPreference>? topicPreferences = null,
        bool? unsubscribeAll = null,
        string? attributesData = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateContactRequest
        {
            ContactListName = contactListName,
            EmailAddress = emailAddress
        };
        if (topicPreferences != null) request.TopicPreferences = topicPreferences;
        if (unsubscribeAll.HasValue) request.UnsubscribeAll = unsubscribeAll.Value;
        if (attributesData != null) request.AttributesData = attributesData;

        try
        {
            await client.CreateContactAsync(request);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create contact '{emailAddress}' in list '{contactListName}'");
        }
    }

    /// <summary>
    /// Delete a contact from a contact list.
    /// </summary>
    public static async Task DeleteContactAsync(
        string contactListName,
        string emailAddress,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteContactAsync(new DeleteContactRequest
            {
                ContactListName = contactListName,
                EmailAddress = emailAddress
            });
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete contact '{emailAddress}' from list '{contactListName}'");
        }
    }

    /// <summary>
    /// List contacts in a contact list, automatically paginating.
    /// </summary>
    public static async Task<SesV2ListContactsResult> ListContactsAsync(
        string contactListName,
        ListContactsFilter? filter = null,
        int? pageSize = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var contacts = new List<Contact>();
        var request = new ListContactsRequest { ContactListName = contactListName };
        if (filter != null) request.Filter = filter;
        if (pageSize.HasValue) request.PageSize = pageSize.Value;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListContactsAsync(request);
                contacts.AddRange(resp.Contacts);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list contacts in list '{contactListName}'");
        }

        return new SesV2ListContactsResult(Contacts: contacts);
    }

    /// <summary>
    /// Create an email template.
    /// </summary>
    public static async Task CreateEmailTemplateAsync(
        string templateName,
        EmailTemplateContent templateContent,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateEmailTemplateAsync(new CreateEmailTemplateRequest
            {
                TemplateName = templateName,
                TemplateContent = templateContent
            });
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create email template '{templateName}'");
        }
    }

    /// <summary>
    /// Delete an email template.
    /// </summary>
    public static async Task DeleteEmailTemplateAsync(
        string templateName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteEmailTemplateAsync(
                new DeleteEmailTemplateRequest { TemplateName = templateName });
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete email template '{templateName}'");
        }
    }

    /// <summary>
    /// Get an email template by name.
    /// </summary>
    public static async Task<GetEmailTemplateResult> GetEmailTemplateAsync(
        string templateName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetEmailTemplateAsync(
                new GetEmailTemplateRequest { TemplateName = templateName });
            return new GetEmailTemplateResult(
                TemplateName: resp.TemplateName,
                TemplateContent: resp.TemplateContent);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get email template '{templateName}'");
        }
    }

    /// <summary>
    /// Update an existing email template.
    /// </summary>
    public static async Task UpdateEmailTemplateAsync(
        string templateName,
        EmailTemplateContent templateContent,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateEmailTemplateAsync(new UpdateEmailTemplateRequest
            {
                TemplateName = templateName,
                TemplateContent = templateContent
            });
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update email template '{templateName}'");
        }
    }

    /// <summary>
    /// List email templates, automatically paginating.
    /// </summary>
    public static async Task<ListEmailTemplatesResult> ListEmailTemplatesAsync(
        int? pageSize = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var templates = new List<EmailTemplateMetadata>();
        var request = new ListEmailTemplatesRequest();
        if (pageSize.HasValue) request.PageSize = pageSize.Value;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListEmailTemplatesAsync(request);
                templates.AddRange(resp.TemplatesMetadata);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list email templates");
        }

        return new ListEmailTemplatesResult(Templates: templates);
    }

    /// <summary>
    /// Get SES v2 account details.
    /// </summary>
    public static async Task<GetAccountResult> GetAccountAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAccountAsync(new GetAccountRequest());
            return new GetAccountResult(
                DedicatedIpAutoWarmupEnabled: resp.DedicatedIpAutoWarmupEnabled,
                EnforcementStatus: resp.EnforcementStatus,
                ProductionAccessEnabled: resp.ProductionAccessEnabled,
                SendQuota: resp.SendQuota,
                SendingEnabled: resp.SendingEnabled,
                SuppressionAttributes: resp.SuppressionAttributes,
                Details: resp.Details);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get SES v2 account");
        }
    }

    /// <summary>
    /// Update SES v2 account details.
    /// </summary>
    public static async Task PutAccountDetailsAsync(
        string mailType,
        string websiteUrl,
        string useCaseDescription,
        string? contactLanguage = null,
        string? additionalContactEmailAddresses = null,
        bool? productionAccessEnabled = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutAccountDetailsRequest
        {
            MailType = mailType,
            WebsiteURL = websiteUrl,
            UseCaseDescription = useCaseDescription
        };
        if (contactLanguage != null) request.ContactLanguage = contactLanguage;
        if (additionalContactEmailAddresses != null)
            request.AdditionalContactEmailAddresses = [additionalContactEmailAddresses];
        if (productionAccessEnabled.HasValue)
            request.ProductionAccessEnabled = productionAccessEnabled.Value;

        try
        {
            await client.PutAccountDetailsAsync(request);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put account details");
        }
    }

    /// <summary>
    /// Create a configuration set.
    /// </summary>
    public static async Task CreateConfigurationSetAsync(
        string configurationSetName,
        TrackingOptions? trackingOptions = null,
        DeliveryOptions? deliveryOptions = null,
        ReputationOptions? reputationOptions = null,
        SendingOptions? sendingOptions = null,
        List<Amazon.SimpleEmailV2.Model.Tag>? tags = null,
        SuppressionOptions? suppressionOptions = null,
        VdmOptions? vdmOptions = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateConfigurationSetRequest
        {
            ConfigurationSetName = configurationSetName
        };
        if (trackingOptions != null) request.TrackingOptions = trackingOptions;
        if (deliveryOptions != null) request.DeliveryOptions = deliveryOptions;
        if (reputationOptions != null) request.ReputationOptions = reputationOptions;
        if (sendingOptions != null) request.SendingOptions = sendingOptions;
        if (tags != null) request.Tags = tags;
        if (suppressionOptions != null) request.SuppressionOptions = suppressionOptions;
        if (vdmOptions != null) request.VdmOptions = vdmOptions;

        try
        {
            await client.CreateConfigurationSetAsync(request);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create configuration set '{configurationSetName}'");
        }
    }

    /// <summary>
    /// Delete a configuration set.
    /// </summary>
    public static async Task DeleteConfigurationSetAsync(
        string configurationSetName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteConfigurationSetAsync(
                new DeleteConfigurationSetRequest
                {
                    ConfigurationSetName = configurationSetName
                });
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete configuration set '{configurationSetName}'");
        }
    }

    /// <summary>
    /// Get details about a configuration set.
    /// </summary>
    public static async Task<GetConfigurationSetResult> GetConfigurationSetAsync(
        string configurationSetName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetConfigurationSetAsync(
                new GetConfigurationSetRequest
                {
                    ConfigurationSetName = configurationSetName
                });
            return new GetConfigurationSetResult(
                ConfigurationSetName: resp.ConfigurationSetName,
                TrackingOptions: resp.TrackingOptions,
                DeliveryOptions: resp.DeliveryOptions,
                ReputationOptions: resp.ReputationOptions,
                SendingOptions: resp.SendingOptions,
                Tags: resp.Tags,
                SuppressionOptions: resp.SuppressionOptions,
                VdmOptions: resp.VdmOptions);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get configuration set '{configurationSetName}'");
        }
    }

    /// <summary>
    /// List configuration sets, automatically paginating.
    /// </summary>
    public static async Task<ListConfigurationSetsResult> ListConfigurationSetsAsync(
        int? pageSize = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var configSets = new List<string>();
        var request = new ListConfigurationSetsRequest();
        if (pageSize.HasValue) request.PageSize = pageSize.Value;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListConfigurationSetsAsync(request);
                configSets.AddRange(resp.ConfigurationSets);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSimpleEmailServiceV2Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list configuration sets");
        }

        return new ListConfigurationSetsResult(ConfigurationSets: configSets);
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="SendEmailAsync"/>.</summary>
    public static SesV2SendEmailResult SendEmail(EmailContent content, string? fromEmailAddress = null, Destination? destination = null, List<string>? replyToAddresses = null, string? feedbackForwardingEmailAddress = null, string? configurationSetName = null, List<MessageTag>? emailTags = null, RegionEndpoint? region = null)
        => SendEmailAsync(content, fromEmailAddress, destination, replyToAddresses, feedbackForwardingEmailAddress, configurationSetName, emailTags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateEmailIdentityAsync"/>.</summary>
    public static CreateEmailIdentityResult CreateEmailIdentity(string emailIdentity, List<Amazon.SimpleEmailV2.Model.Tag>? tags = null, DkimSigningAttributes? dkimSigningAttributes = null, string? configurationSetName = null, RegionEndpoint? region = null)
        => CreateEmailIdentityAsync(emailIdentity, tags, dkimSigningAttributes, configurationSetName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteEmailIdentityAsync"/>.</summary>
    public static void DeleteEmailIdentity(string emailIdentity, RegionEndpoint? region = null)
        => DeleteEmailIdentityAsync(emailIdentity, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetEmailIdentityAsync"/>.</summary>
    public static GetEmailIdentityResult GetEmailIdentity(string emailIdentity, RegionEndpoint? region = null)
        => GetEmailIdentityAsync(emailIdentity, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListEmailIdentitiesAsync"/>.</summary>
    public static ListEmailIdentitiesResult ListEmailIdentities(int? pageSize = null, RegionEndpoint? region = null)
        => ListEmailIdentitiesAsync(pageSize, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateContactListAsync"/>.</summary>
    public static void CreateContactList(string contactListName, string? description = null, List<Topic>? topics = null, List<Amazon.SimpleEmailV2.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateContactListAsync(contactListName, description, topics, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteContactListAsync"/>.</summary>
    public static void DeleteContactList(string contactListName, RegionEndpoint? region = null)
        => DeleteContactListAsync(contactListName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetContactAsync"/>.</summary>
    public static GetContactResult GetContact(string contactListName, string emailAddress, RegionEndpoint? region = null)
        => GetContactAsync(contactListName, emailAddress, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateContactAsync"/>.</summary>
    public static void CreateContact(string contactListName, string emailAddress, List<TopicPreference>? topicPreferences = null, bool? unsubscribeAll = null, string? attributesData = null, RegionEndpoint? region = null)
        => CreateContactAsync(contactListName, emailAddress, topicPreferences, unsubscribeAll, attributesData, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteContactAsync"/>.</summary>
    public static void DeleteContact(string contactListName, string emailAddress, RegionEndpoint? region = null)
        => DeleteContactAsync(contactListName, emailAddress, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListContactsAsync"/>.</summary>
    public static SesV2ListContactsResult ListContacts(string contactListName, ListContactsFilter? filter = null, int? pageSize = null, RegionEndpoint? region = null)
        => ListContactsAsync(contactListName, filter, pageSize, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateEmailTemplateAsync"/>.</summary>
    public static void CreateEmailTemplate(string templateName, EmailTemplateContent templateContent, RegionEndpoint? region = null)
        => CreateEmailTemplateAsync(templateName, templateContent, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteEmailTemplateAsync"/>.</summary>
    public static void DeleteEmailTemplate(string templateName, RegionEndpoint? region = null)
        => DeleteEmailTemplateAsync(templateName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetEmailTemplateAsync"/>.</summary>
    public static GetEmailTemplateResult GetEmailTemplate(string templateName, RegionEndpoint? region = null)
        => GetEmailTemplateAsync(templateName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateEmailTemplateAsync"/>.</summary>
    public static void UpdateEmailTemplate(string templateName, EmailTemplateContent templateContent, RegionEndpoint? region = null)
        => UpdateEmailTemplateAsync(templateName, templateContent, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListEmailTemplatesAsync"/>.</summary>
    public static ListEmailTemplatesResult ListEmailTemplates(int? pageSize = null, RegionEndpoint? region = null)
        => ListEmailTemplatesAsync(pageSize, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAccountAsync"/>.</summary>
    public static GetAccountResult GetAccount(RegionEndpoint? region = null)
        => GetAccountAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutAccountDetailsAsync"/>.</summary>
    public static void PutAccountDetails(string mailType, string websiteUrl, string useCaseDescription, string? contactLanguage = null, string? additionalContactEmailAddresses = null, bool? productionAccessEnabled = null, RegionEndpoint? region = null)
        => PutAccountDetailsAsync(mailType, websiteUrl, useCaseDescription, contactLanguage, additionalContactEmailAddresses, productionAccessEnabled, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateConfigurationSetAsync"/>.</summary>
    public static void CreateConfigurationSet(string configurationSetName, TrackingOptions? trackingOptions = null, DeliveryOptions? deliveryOptions = null, ReputationOptions? reputationOptions = null, SendingOptions? sendingOptions = null, List<Amazon.SimpleEmailV2.Model.Tag>? tags = null, SuppressionOptions? suppressionOptions = null, VdmOptions? vdmOptions = null, RegionEndpoint? region = null)
        => CreateConfigurationSetAsync(configurationSetName, trackingOptions, deliveryOptions, reputationOptions, sendingOptions, tags, suppressionOptions, vdmOptions, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteConfigurationSetAsync"/>.</summary>
    public static void DeleteConfigurationSet(string configurationSetName, RegionEndpoint? region = null)
        => DeleteConfigurationSetAsync(configurationSetName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetConfigurationSetAsync"/>.</summary>
    public static GetConfigurationSetResult GetConfigurationSet(string configurationSetName, RegionEndpoint? region = null)
        => GetConfigurationSetAsync(configurationSetName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListConfigurationSetsAsync"/>.</summary>
    public static ListConfigurationSetsResult ListConfigurationSets(int? pageSize = null, RegionEndpoint? region = null)
        => ListConfigurationSetsAsync(pageSize, region).GetAwaiter().GetResult();

}

using Amazon;
using Amazon.AWSHealth;
using Amazon.AWSHealth.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record HealthDescribeEventsResult(
    List<Event>? Events = null,
    string? NextToken = null);

public sealed record HealthDescribeEventDetailsResult(
    List<EventDetails>? SuccessfulSet = null,
    List<EventDetailsErrorItem>? FailedSet = null);

public sealed record HealthDescribeAffectedEntitiesResult(
    List<AffectedEntity>? Entities = null,
    string? NextToken = null);

public sealed record HealthDescribeEventTypesResult(
    List<EventType>? EventTypes = null,
    string? NextToken = null);

public sealed record HealthDescribeEventAggregatesResult(
    List<EventAggregate>? EventAggregates = null,
    string? NextToken = null);

public sealed record HealthDescribeAffectedAccountsForOrganizationResult(
    List<string>? AffectedAccounts = null,
    string? NextToken = null);

public sealed record HealthDescribeAffectedEntitiesForOrganizationResult(
    List<AffectedEntity>? Entities = null,
    List<OrganizationAffectedEntitiesErrorItem>? FailedSet = null,
    string? NextToken = null);

public sealed record HealthDescribeEventsForOrganizationResult(
    List<OrganizationEvent>? Events = null,
    string? NextToken = null);

public sealed record HealthDescribeEventDetailsForOrganizationResult(
    List<OrganizationEventDetails>? SuccessfulSet = null,
    List<OrganizationEventDetailsErrorItem>? FailedSet = null);

public sealed record HealthEnableServiceAccessForOrganizationResult(bool Success = true);

public sealed record HealthDisableServiceAccessForOrganizationResult(bool Success = true);

public sealed record HealthDescribeServiceStatusForOrganizationResult(
    string? HealthServiceAccessStatusForOrganization = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS Health.
/// </summary>
public static class HealthService
{
    private static AmazonAWSHealthClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonAWSHealthClient>(region);

    /// <summary>
    /// Describe AWS Health events matching the supplied filter.
    /// </summary>
    public static async Task<HealthDescribeEventsResult> DescribeEventsAsync(
        EventFilter? filter = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEventsRequest();
        if (filter != null) request.Filter = filter;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeEventsAsync(request);
            return new HealthDescribeEventsResult(
                Events: resp.Events,
                NextToken: resp.NextToken);
        }
        catch (AmazonAWSHealthException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe Health events");
        }
    }

    /// <summary>
    /// Describe details for one or more AWS Health events.
    /// </summary>
    public static async Task<HealthDescribeEventDetailsResult> DescribeEventDetailsAsync(
        List<string> eventArns,
        string? locale = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEventDetailsRequest { EventArns = eventArns };
        if (locale != null) request.Locale = locale;

        try
        {
            var resp = await client.DescribeEventDetailsAsync(request);
            return new HealthDescribeEventDetailsResult(
                SuccessfulSet: resp.SuccessfulSet,
                FailedSet: resp.FailedSet);
        }
        catch (AmazonAWSHealthException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe Health event details");
        }
    }

    /// <summary>
    /// Describe entities affected by one or more AWS Health events.
    /// </summary>
    public static async Task<HealthDescribeAffectedEntitiesResult> DescribeAffectedEntitiesAsync(
        EntityFilter filter,
        string? locale = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeAffectedEntitiesRequest { Filter = filter };
        if (locale != null) request.Locale = locale;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeAffectedEntitiesAsync(request);
            return new HealthDescribeAffectedEntitiesResult(
                Entities: resp.Entities,
                NextToken: resp.NextToken);
        }
        catch (AmazonAWSHealthException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe affected entities");
        }
    }

    /// <summary>
    /// Describe available AWS Health event types.
    /// </summary>
    public static async Task<HealthDescribeEventTypesResult> DescribeEventTypesAsync(
        EventTypeFilter? filter = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEventTypesRequest();
        if (filter != null) request.Filter = filter;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeEventTypesAsync(request);
            return new HealthDescribeEventTypesResult(
                EventTypes: resp.EventTypes,
                NextToken: resp.NextToken);
        }
        catch (AmazonAWSHealthException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe event types");
        }
    }

    /// <summary>
    /// Describe aggregated counts of AWS Health events.
    /// </summary>
    public static async Task<HealthDescribeEventAggregatesResult> DescribeEventAggregatesAsync(
        EventAggregateField aggregateField,
        EventFilter? filter = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEventAggregatesRequest
        {
            AggregateField = aggregateField
        };
        if (filter != null) request.Filter = filter;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeEventAggregatesAsync(request);
            return new HealthDescribeEventAggregatesResult(
                EventAggregates: resp.EventAggregates,
                NextToken: resp.NextToken);
        }
        catch (AmazonAWSHealthException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe event aggregates");
        }
    }

    /// <summary>
    /// Describe AWS accounts affected by an organizational Health event.
    /// </summary>
    public static async Task<HealthDescribeAffectedAccountsForOrganizationResult>
        DescribeAffectedAccountsForOrganizationAsync(
            string eventArn,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeAffectedAccountsForOrganizationRequest
        {
            EventArn = eventArn
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeAffectedAccountsForOrganizationAsync(request);
            return new HealthDescribeAffectedAccountsForOrganizationResult(
                AffectedAccounts: resp.AffectedAccounts,
                NextToken: resp.NextToken);
        }
        catch (AmazonAWSHealthException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe affected accounts for organization");
        }
    }

    /// <summary>
    /// Describe entities affected by organizational Health events.
    /// </summary>
    public static async Task<HealthDescribeAffectedEntitiesForOrganizationResult>
        DescribeAffectedEntitiesForOrganizationAsync(
            List<EventAccountFilter> organizationEntityFilters,
            string? locale = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeAffectedEntitiesForOrganizationRequest
        {
            OrganizationEntityFilters = organizationEntityFilters
        };
        if (locale != null) request.Locale = locale;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeAffectedEntitiesForOrganizationAsync(request);
            return new HealthDescribeAffectedEntitiesForOrganizationResult(
                Entities: resp.Entities,
                FailedSet: resp.FailedSet,
                NextToken: resp.NextToken);
        }
        catch (AmazonAWSHealthException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe affected entities for organization");
        }
    }

    /// <summary>
    /// Describe organizational Health events.
    /// </summary>
    public static async Task<HealthDescribeEventsForOrganizationResult>
        DescribeEventsForOrganizationAsync(
            OrganizationEventFilter? filter = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEventsForOrganizationRequest();
        if (filter != null) request.Filter = filter;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeEventsForOrganizationAsync(request);
            return new HealthDescribeEventsForOrganizationResult(
                Events: resp.Events,
                NextToken: resp.NextToken);
        }
        catch (AmazonAWSHealthException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe events for organization");
        }
    }

    /// <summary>
    /// Describe details for organizational Health events.
    /// </summary>
    public static async Task<HealthDescribeEventDetailsForOrganizationResult>
        DescribeEventDetailsForOrganizationAsync(
            List<EventAccountFilter> organizationEventDetailFilters,
            string? locale = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEventDetailsForOrganizationRequest
        {
            OrganizationEventDetailFilters = organizationEventDetailFilters
        };
        if (locale != null) request.Locale = locale;

        try
        {
            var resp = await client.DescribeEventDetailsForOrganizationAsync(request);
            return new HealthDescribeEventDetailsForOrganizationResult(
                SuccessfulSet: resp.SuccessfulSet,
                FailedSet: resp.FailedSet);
        }
        catch (AmazonAWSHealthException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe event details for organization");
        }
    }

    /// <summary>
    /// Enable AWS Health organizational view.
    /// </summary>
    public static async Task<HealthEnableServiceAccessForOrganizationResult>
        EnableHealthServiceAccessForOrganizationAsync(
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.EnableHealthServiceAccessForOrganizationAsync(
                new EnableHealthServiceAccessForOrganizationRequest());
            return new HealthEnableServiceAccessForOrganizationResult();
        }
        catch (AmazonAWSHealthException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to enable Health service access for organization");
        }
    }

    /// <summary>
    /// Disable AWS Health organizational view.
    /// </summary>
    public static async Task<HealthDisableServiceAccessForOrganizationResult>
        DisableHealthServiceAccessForOrganizationAsync(
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisableHealthServiceAccessForOrganizationAsync(
                new DisableHealthServiceAccessForOrganizationRequest());
            return new HealthDisableServiceAccessForOrganizationResult();
        }
        catch (AmazonAWSHealthException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to disable Health service access for organization");
        }
    }

    /// <summary>
    /// Describe the status of Health organizational view.
    /// </summary>
    public static async Task<HealthDescribeServiceStatusForOrganizationResult>
        DescribeHealthServiceStatusForOrganizationAsync(
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeHealthServiceStatusForOrganizationAsync(
                new DescribeHealthServiceStatusForOrganizationRequest());
            return new HealthDescribeServiceStatusForOrganizationResult(
                HealthServiceAccessStatusForOrganization:
                    resp.HealthServiceAccessStatusForOrganization);
        }
        catch (AmazonAWSHealthException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Health service status for organization");
        }
    }
}

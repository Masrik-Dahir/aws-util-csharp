using Amazon;
using Amazon.IoT;
using Amazon.IoT.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CreateThingResult(
    string? ThingName = null,
    string? ThingArn = null,
    string? ThingId = null);

public sealed record DescribeThingResult(
    string? ThingName = null,
    string? ThingArn = null,
    string? ThingId = null,
    string? ThingTypeName = null,
    long? Version = null,
    Dictionary<string, string>? Attributes = null);

public sealed record ListThingsResult(
    List<ThingAttribute>? Things = null,
    string? NextToken = null);

public sealed record CreateThingGroupResult(
    string? ThingGroupName = null,
    string? ThingGroupArn = null,
    string? ThingGroupId = null);

public sealed record DescribeThingGroupResult(
    string? ThingGroupName = null,
    string? ThingGroupArn = null,
    string? ThingGroupId = null,
    long? Version = null,
    ThingGroupProperties? ThingGroupProperties = null);

public sealed record ListThingGroupsResult(
    List<GroupNameAndArn>? ThingGroups = null,
    string? NextToken = null);

public sealed record ListThingsInThingGroupResult(
    List<string>? Things = null,
    string? NextToken = null);

public sealed record CreateThingTypeResult(
    string? ThingTypeName = null,
    string? ThingTypeArn = null,
    string? ThingTypeId = null);

public sealed record DescribeThingTypeResult(
    string? ThingTypeName = null,
    string? ThingTypeArn = null,
    string? ThingTypeId = null,
    ThingTypeProperties? ThingTypeProperties = null,
    ThingTypeMetadata? ThingTypeMetadata = null);

public sealed record ListThingTypesResult(
    List<ThingTypeDefinition>? ThingTypes = null,
    string? NextToken = null);

public sealed record CreateIoTPolicyResult(
    string? PolicyName = null,
    string? PolicyArn = null,
    string? PolicyDocument = null,
    string? PolicyVersionId = null);

public sealed record GetIoTPolicyResult(
    string? PolicyName = null,
    string? PolicyArn = null,
    string? PolicyDocument = null,
    string? DefaultVersionId = null);

public sealed record ListIoTPoliciesResult(
    List<Policy>? Policies = null,
    string? NextToken = null);

public sealed record CreatePolicyVersionResult(
    string? PolicyArn = null,
    string? PolicyDocument = null,
    string? PolicyVersionId = null,
    bool? IsDefaultVersion = null);

public sealed record ListPolicyVersionsResult(
    List<PolicyVersion>? PolicyVersions = null);

public sealed record ListTargetsForPolicyResult(
    List<string>? Targets = null,
    string? NextToken = null);

public sealed record CreateCertificateFromCsrResult(
    string? CertificateArn = null,
    string? CertificateId = null,
    string? CertificatePem = null);

public sealed record DescribeIoTCertificateResult(
    CertificateDescription? CertificateDescription = null);

public sealed record ListIoTCertificatesResult(
    List<Certificate>? Certificates = null,
    string? NextToken = null);

public sealed record ListThingPrincipalsResult(
    List<string>? Principals = null,
    string? NextToken = null);

public sealed record GetTopicRuleResult(
    string? RuleArn = null,
    TopicRule? Rule = null);

public sealed record ListTopicRulesResult(
    List<TopicRuleListItem>? Rules = null,
    string? NextToken = null);

public sealed record CreateIoTJobResult(
    string? JobArn = null,
    string? JobId = null,
    string? Description = null);

public sealed record DescribeIoTJobResult(
    Job? Job = null);

public sealed record ListIoTJobsResult(
    List<JobSummary>? Jobs = null,
    string? NextToken = null);

public sealed record CancelIoTJobResult(
    string? JobArn = null,
    string? JobId = null,
    string? Description = null);

public sealed record CreateIoTJobTemplateResult(
    string? JobTemplateArn = null,
    string? JobTemplateId = null);

public sealed record DescribeIoTJobTemplateResult(
    string? JobTemplateArn = null,
    string? JobTemplateId = null,
    string? Description = null,
    string? Document = null);

public sealed record ListIoTJobTemplatesResult(
    List<JobTemplateSummary>? JobTemplates = null,
    string? NextToken = null);

public sealed record IoTListTagsResult(
    List<Amazon.IoT.Model.Tag>? Tags = null,
    string? NextToken = null);

public sealed record SearchIndexResult(
    List<ThingDocument>? Things = null,
    List<ThingGroupDocument>? ThingGroups = null,
    string? NextToken = null);

public sealed record DescribeIndexResult(
    string? IndexName = null,
    string? IndexStatus = null,
    string? Schema = null);

public sealed record GetIndexingConfigurationResult(
    ThingIndexingConfiguration? ThingIndexingConfiguration = null,
    ThingGroupIndexingConfiguration? ThingGroupIndexingConfiguration = null);

/// <summary>
/// Utility helpers for AWS IoT Core.
/// </summary>
public static class IoTService
{
    private static AmazonIoTClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonIoTClient>(region);

    // ── Thing operations ────────────────────────────────────────────

    /// <summary>
    /// Create a new IoT thing.
    /// </summary>
    public static async Task<CreateThingResult> CreateThingAsync(
        string thingName,
        string? thingTypeName = null,
        Dictionary<string, string>? attributes = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateThingRequest { ThingName = thingName };
        if (thingTypeName != null) request.ThingTypeName = thingTypeName;
        if (attributes != null)
            request.AttributePayload = new AttributePayload { Attributes = attributes };

        try
        {
            var resp = await client.CreateThingAsync(request);
            return new CreateThingResult(
                ThingName: resp.ThingName,
                ThingArn: resp.ThingArn,
                ThingId: resp.ThingId);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create thing '{thingName}'");
        }
    }

    /// <summary>
    /// Delete an IoT thing.
    /// </summary>
    public static async Task DeleteThingAsync(
        string thingName,
        long? expectedVersion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteThingRequest { ThingName = thingName };
        if (expectedVersion.HasValue) request.ExpectedVersion = expectedVersion.Value;

        try
        {
            await client.DeleteThingAsync(request);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete thing '{thingName}'");
        }
    }

    /// <summary>
    /// Describe an IoT thing.
    /// </summary>
    public static async Task<DescribeThingResult> DescribeThingAsync(
        string thingName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeThingAsync(
                new DescribeThingRequest { ThingName = thingName });
            return new DescribeThingResult(
                ThingName: resp.ThingName,
                ThingArn: resp.ThingArn,
                ThingId: resp.ThingId,
                ThingTypeName: resp.ThingTypeName,
                Version: resp.Version,
                Attributes: resp.Attributes);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe thing '{thingName}'");
        }
    }

    /// <summary>
    /// List IoT things with optional pagination.
    /// </summary>
    public static async Task<ListThingsResult> ListThingsAsync(
        string? thingTypeName = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListThingsRequest();
        if (thingTypeName != null) request.ThingTypeName = thingTypeName;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListThingsAsync(request);
            return new ListThingsResult(
                Things: resp.Things,
                NextToken: null);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list things");
        }
    }

    /// <summary>
    /// Update attributes of an IoT thing.
    /// </summary>
    public static async Task UpdateThingAsync(
        string thingName,
        string? thingTypeName = null,
        Dictionary<string, string>? attributes = null,
        bool? removeThingType = null,
        long? expectedVersion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateThingRequest { ThingName = thingName };
        if (thingTypeName != null) request.ThingTypeName = thingTypeName;
        if (attributes != null)
            request.AttributePayload = new AttributePayload { Attributes = attributes };
        if (removeThingType.HasValue) request.RemoveThingType = removeThingType.Value;
        if (expectedVersion.HasValue) request.ExpectedVersion = expectedVersion.Value;

        try
        {
            await client.UpdateThingAsync(request);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to update thing '{thingName}'");
        }
    }

    // ── Thing Group operations ──────────────────────────────────────

    /// <summary>
    /// Create a new IoT thing group.
    /// </summary>
    public static async Task<CreateThingGroupResult> CreateThingGroupAsync(
        string thingGroupName,
        string? parentGroupName = null,
        ThingGroupProperties? thingGroupProperties = null,
        List<Amazon.IoT.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateThingGroupRequest { ThingGroupName = thingGroupName };
        if (parentGroupName != null) request.ParentGroupName = parentGroupName;
        if (thingGroupProperties != null) request.ThingGroupProperties = thingGroupProperties;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateThingGroupAsync(request);
            return new CreateThingGroupResult(
                ThingGroupName: resp.ThingGroupName,
                ThingGroupArn: resp.ThingGroupArn,
                ThingGroupId: resp.ThingGroupId);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create thing group '{thingGroupName}'");
        }
    }

    /// <summary>
    /// Delete an IoT thing group.
    /// </summary>
    public static async Task DeleteThingGroupAsync(
        string thingGroupName,
        long? expectedVersion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteThingGroupRequest { ThingGroupName = thingGroupName };
        if (expectedVersion.HasValue) request.ExpectedVersion = expectedVersion.Value;

        try
        {
            await client.DeleteThingGroupAsync(request);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete thing group '{thingGroupName}'");
        }
    }

    /// <summary>
    /// Describe an IoT thing group.
    /// </summary>
    public static async Task<DescribeThingGroupResult> DescribeThingGroupAsync(
        string thingGroupName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeThingGroupAsync(
                new DescribeThingGroupRequest { ThingGroupName = thingGroupName });
            return new DescribeThingGroupResult(
                ThingGroupName: resp.ThingGroupName,
                ThingGroupArn: resp.ThingGroupArn,
                ThingGroupId: resp.ThingGroupId,
                Version: resp.Version,
                ThingGroupProperties: resp.ThingGroupProperties);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe thing group '{thingGroupName}'");
        }
    }

    /// <summary>
    /// List IoT thing groups with optional pagination.
    /// </summary>
    public static async Task<ListThingGroupsResult> ListThingGroupsAsync(
        string? parentGroup = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListThingGroupsRequest();
        if (parentGroup != null) request.ParentGroup = parentGroup;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListThingGroupsAsync(request);
            return new ListThingGroupsResult(
                ThingGroups: resp.ThingGroups,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list thing groups");
        }
    }

    /// <summary>
    /// Add a thing to a thing group.
    /// </summary>
    public static async Task AddThingToThingGroupAsync(
        string? thingGroupName = null,
        string? thingGroupArn = null,
        string? thingName = null,
        string? thingArn = null,
        bool? overrideDynamicGroups = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AddThingToThingGroupRequest();
        if (thingGroupName != null) request.ThingGroupName = thingGroupName;
        if (thingGroupArn != null) request.ThingGroupArn = thingGroupArn;
        if (thingName != null) request.ThingName = thingName;
        if (thingArn != null) request.ThingArn = thingArn;
        if (overrideDynamicGroups.HasValue)
            request.OverrideDynamicGroups = overrideDynamicGroups.Value;

        try
        {
            await client.AddThingToThingGroupAsync(request);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to add thing to thing group");
        }
    }

    /// <summary>
    /// Remove a thing from a thing group.
    /// </summary>
    public static async Task RemoveThingFromThingGroupAsync(
        string? thingGroupName = null,
        string? thingGroupArn = null,
        string? thingName = null,
        string? thingArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new RemoveThingFromThingGroupRequest();
        if (thingGroupName != null) request.ThingGroupName = thingGroupName;
        if (thingGroupArn != null) request.ThingGroupArn = thingGroupArn;
        if (thingName != null) request.ThingName = thingName;
        if (thingArn != null) request.ThingArn = thingArn;

        try
        {
            await client.RemoveThingFromThingGroupAsync(request);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to remove thing from thing group");
        }
    }

    /// <summary>
    /// List things in a thing group with optional pagination.
    /// </summary>
    public static async Task<ListThingsInThingGroupResult> ListThingsInThingGroupAsync(
        string thingGroupName,
        bool? recursive = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListThingsInThingGroupRequest { ThingGroupName = thingGroupName };
        if (recursive.HasValue) request.Recursive = recursive.Value;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListThingsInThingGroupAsync(request);
            return new ListThingsInThingGroupResult(
                Things: resp.Things,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list things in group '{thingGroupName}'");
        }
    }

    // ── Thing Type operations ───────────────────────────────────────

    /// <summary>
    /// Create a new IoT thing type.
    /// </summary>
    public static async Task<CreateThingTypeResult> CreateThingTypeAsync(
        string thingTypeName,
        ThingTypeProperties? thingTypeProperties = null,
        List<Amazon.IoT.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateThingTypeRequest { ThingTypeName = thingTypeName };
        if (thingTypeProperties != null) request.ThingTypeProperties = thingTypeProperties;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateThingTypeAsync(request);
            return new CreateThingTypeResult(
                ThingTypeName: resp.ThingTypeName,
                ThingTypeArn: resp.ThingTypeArn,
                ThingTypeId: resp.ThingTypeId);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create thing type '{thingTypeName}'");
        }
    }

    /// <summary>
    /// Delete an IoT thing type.
    /// </summary>
    public static async Task DeleteThingTypeAsync(
        string thingTypeName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteThingTypeAsync(
                new DeleteThingTypeRequest { ThingTypeName = thingTypeName });
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete thing type '{thingTypeName}'");
        }
    }

    /// <summary>
    /// Describe an IoT thing type.
    /// </summary>
    public static async Task<DescribeThingTypeResult> DescribeThingTypeAsync(
        string thingTypeName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeThingTypeAsync(
                new DescribeThingTypeRequest { ThingTypeName = thingTypeName });
            return new DescribeThingTypeResult(
                ThingTypeName: resp.ThingTypeName,
                ThingTypeArn: resp.ThingTypeArn,
                ThingTypeId: resp.ThingTypeId,
                ThingTypeProperties: resp.ThingTypeProperties,
                ThingTypeMetadata: resp.ThingTypeMetadata);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe thing type '{thingTypeName}'");
        }
    }

    /// <summary>
    /// List IoT thing types with optional pagination.
    /// </summary>
    public static async Task<ListThingTypesResult> ListThingTypesAsync(
        string? thingTypeName = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListThingTypesRequest();
        if (thingTypeName != null) request.ThingTypeName = thingTypeName;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListThingTypesAsync(request);
            return new ListThingTypesResult(
                ThingTypes: resp.ThingTypes,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list thing types");
        }
    }

    // ── Policy operations ───────────────────────────────────────────

    /// <summary>
    /// Create an IoT policy.
    /// </summary>
    public static async Task<CreateIoTPolicyResult> CreatePolicyAsync(
        string policyName,
        string policyDocument,
        List<Amazon.IoT.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreatePolicyRequest
        {
            PolicyName = policyName,
            PolicyDocument = policyDocument
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreatePolicyAsync(request);
            return new CreateIoTPolicyResult(
                PolicyName: resp.PolicyName,
                PolicyArn: resp.PolicyArn,
                PolicyDocument: resp.PolicyDocument,
                PolicyVersionId: resp.PolicyVersionId);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create policy '{policyName}'");
        }
    }

    /// <summary>
    /// Delete an IoT policy.
    /// </summary>
    public static async Task DeletePolicyAsync(
        string policyName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePolicyAsync(
                new DeletePolicyRequest { PolicyName = policyName });
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete policy '{policyName}'");
        }
    }

    /// <summary>
    /// Get an IoT policy by name.
    /// </summary>
    public static async Task<GetIoTPolicyResult> GetPolicyAsync(
        string policyName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetPolicyAsync(
                new GetPolicyRequest { PolicyName = policyName });
            return new GetIoTPolicyResult(
                PolicyName: resp.PolicyName,
                PolicyArn: resp.PolicyArn,
                PolicyDocument: resp.PolicyDocument,
                DefaultVersionId: resp.DefaultVersionId);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get policy '{policyName}'");
        }
    }

    /// <summary>
    /// List IoT policies with optional pagination.
    /// </summary>
    public static async Task<ListIoTPoliciesResult> ListPoliciesAsync(
        bool? ascendingOrder = null,
        int? pageSize = null,
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListPoliciesRequest();
        if (ascendingOrder.HasValue) request.AscendingOrder = ascendingOrder.Value;
        if (pageSize.HasValue) request.PageSize = pageSize.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.ListPoliciesAsync(request);
            return new ListIoTPoliciesResult(
                Policies: resp.Policies,
                NextToken: resp.NextMarker);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list policies");
        }
    }

    /// <summary>
    /// Create a new version of an IoT policy.
    /// </summary>
    public static async Task<CreatePolicyVersionResult> CreatePolicyVersionAsync(
        string policyName,
        string policyDocument,
        bool? setAsDefault = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreatePolicyVersionRequest
        {
            PolicyName = policyName,
            PolicyDocument = policyDocument
        };
        if (setAsDefault.HasValue) request.SetAsDefault = setAsDefault.Value;

        try
        {
            var resp = await client.CreatePolicyVersionAsync(request);
            return new CreatePolicyVersionResult(
                PolicyArn: resp.PolicyArn,
                PolicyDocument: resp.PolicyDocument,
                PolicyVersionId: resp.PolicyVersionId,
                IsDefaultVersion: resp.IsDefaultVersion);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create policy version for '{policyName}'");
        }
    }

    /// <summary>
    /// Delete a version of an IoT policy.
    /// </summary>
    public static async Task DeletePolicyVersionAsync(
        string policyName,
        string policyVersionId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePolicyVersionAsync(new DeletePolicyVersionRequest
            {
                PolicyName = policyName,
                PolicyVersionId = policyVersionId
            });
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete policy version '{policyVersionId}' for '{policyName}'");
        }
    }

    /// <summary>
    /// List versions of an IoT policy.
    /// </summary>
    public static async Task<ListPolicyVersionsResult> ListPolicyVersionsAsync(
        string policyName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListPolicyVersionsAsync(
                new ListPolicyVersionsRequest { PolicyName = policyName });
            return new ListPolicyVersionsResult(PolicyVersions: resp.PolicyVersions);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list policy versions for '{policyName}'");
        }
    }

    /// <summary>
    /// Attach an IoT policy to a target (certificate or Cognito identity).
    /// </summary>
    public static async Task AttachPolicyAsync(
        string policyName,
        string target,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AttachPolicyAsync(new AttachPolicyRequest
            {
                PolicyName = policyName,
                Target = target
            });
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to attach policy '{policyName}' to target");
        }
    }

    /// <summary>
    /// Detach an IoT policy from a target.
    /// </summary>
    public static async Task DetachPolicyAsync(
        string policyName,
        string target,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DetachPolicyAsync(new DetachPolicyRequest
            {
                PolicyName = policyName,
                Target = target
            });
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to detach policy '{policyName}' from target");
        }
    }

    /// <summary>
    /// List targets for an IoT policy.
    /// </summary>
    public static async Task<ListTargetsForPolicyResult> ListTargetsForPolicyAsync(
        string policyName,
        int? pageSize = null,
        string? marker = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTargetsForPolicyRequest { PolicyName = policyName };
        if (pageSize.HasValue) request.PageSize = pageSize.Value;
        if (marker != null) request.Marker = marker;

        try
        {
            var resp = await client.ListTargetsForPolicyAsync(request);
            return new ListTargetsForPolicyResult(
                Targets: resp.Targets,
                NextToken: resp.NextMarker);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list targets for policy '{policyName}'");
        }
    }

    // ── Certificate operations ──────────────────────────────────────

    /// <summary>
    /// Create a certificate from a certificate signing request (CSR).
    /// </summary>
    public static async Task<CreateCertificateFromCsrResult> CreateCertificateFromCsrAsync(
        string certificateSigningRequest,
        bool? setAsActive = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateCertificateFromCsrRequest
        {
            CertificateSigningRequest = certificateSigningRequest
        };
        if (setAsActive.HasValue) request.SetAsActive = setAsActive.Value;

        try
        {
            var resp = await client.CreateCertificateFromCsrAsync(request);
            return new CreateCertificateFromCsrResult(
                CertificateArn: resp.CertificateArn,
                CertificateId: resp.CertificateId,
                CertificatePem: resp.CertificatePem);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create certificate from CSR");
        }
    }

    /// <summary>
    /// Delete an IoT certificate.
    /// </summary>
    public static async Task DeleteCertificateAsync(
        string certificateId,
        bool? forceDelete = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteCertificateRequest { CertificateId = certificateId };
        if (forceDelete.HasValue) request.ForceDelete = forceDelete.Value;

        try
        {
            await client.DeleteCertificateAsync(request);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete certificate '{certificateId}'");
        }
    }

    /// <summary>
    /// Describe an IoT certificate.
    /// </summary>
    public static async Task<DescribeIoTCertificateResult> DescribeCertificateAsync(
        string certificateId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeCertificateAsync(
                new DescribeCertificateRequest { CertificateId = certificateId });
            return new DescribeIoTCertificateResult(
                CertificateDescription: resp.CertificateDescription);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe certificate '{certificateId}'");
        }
    }

    /// <summary>
    /// List IoT certificates with optional pagination.
    /// </summary>
    public static async Task<ListIoTCertificatesResult> ListCertificatesAsync(
        int? pageSize = null,
        string? marker = null,
        bool? ascendingOrder = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListCertificatesRequest();
        if (pageSize.HasValue) request.PageSize = pageSize.Value;
        if (marker != null) request.Marker = marker;
        if (ascendingOrder.HasValue) request.AscendingOrder = ascendingOrder.Value;

        try
        {
            var resp = await client.ListCertificatesAsync(request);
            return new ListIoTCertificatesResult(
                Certificates: resp.Certificates,
                NextToken: resp.NextMarker);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list certificates");
        }
    }

    /// <summary>
    /// Update the status of an IoT certificate.
    /// </summary>
    public static async Task UpdateCertificateAsync(
        string certificateId,
        CertificateStatus newStatus,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateCertificateAsync(new UpdateCertificateRequest
            {
                CertificateId = certificateId,
                NewStatus = newStatus
            });
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update certificate '{certificateId}'");
        }
    }

    // ── Thing Principal operations ──────────────────────────────────

    /// <summary>
    /// Attach a principal (certificate) to an IoT thing.
    /// </summary>
    public static async Task AttachThingPrincipalAsync(
        string thingName,
        string principal,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AttachThingPrincipalAsync(new AttachThingPrincipalRequest
            {
                ThingName = thingName,
                Principal = principal
            });
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to attach principal to thing '{thingName}'");
        }
    }

    /// <summary>
    /// Detach a principal from an IoT thing.
    /// </summary>
    public static async Task DetachThingPrincipalAsync(
        string thingName,
        string principal,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DetachThingPrincipalAsync(new DetachThingPrincipalRequest
            {
                ThingName = thingName,
                Principal = principal
            });
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to detach principal from thing '{thingName}'");
        }
    }

    /// <summary>
    /// List principals (certificates) associated with an IoT thing.
    /// </summary>
    public static async Task<ListThingPrincipalsResult> ListThingPrincipalsAsync(
        string thingName,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListThingPrincipalsRequest { ThingName = thingName };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListThingPrincipalsAsync(request);
            return new ListThingPrincipalsResult(
                Principals: resp.Principals,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list principals for thing '{thingName}'");
        }
    }

    // ── Topic Rule operations ───────────────────────────────────────

    /// <summary>
    /// Create an IoT topic rule.
    /// </summary>
    public static async Task CreateTopicRuleAsync(
        string ruleName,
        TopicRulePayload topicRulePayload,
        string? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateTopicRuleRequest
        {
            RuleName = ruleName,
            TopicRulePayload = topicRulePayload
        };
        if (tags != null) request.Tags = tags;

        try
        {
            await client.CreateTopicRuleAsync(request);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create topic rule '{ruleName}'");
        }
    }

    /// <summary>
    /// Delete an IoT topic rule.
    /// </summary>
    public static async Task DeleteTopicRuleAsync(
        string ruleName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTopicRuleAsync(
                new DeleteTopicRuleRequest { RuleName = ruleName });
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete topic rule '{ruleName}'");
        }
    }

    /// <summary>
    /// Get an IoT topic rule.
    /// </summary>
    public static async Task<GetTopicRuleResult> GetTopicRuleAsync(
        string ruleName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetTopicRuleAsync(
                new GetTopicRuleRequest { RuleName = ruleName });
            return new GetTopicRuleResult(
                RuleArn: resp.RuleArn,
                Rule: resp.Rule);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get topic rule '{ruleName}'");
        }
    }

    /// <summary>
    /// List IoT topic rules with optional pagination.
    /// </summary>
    public static async Task<ListTopicRulesResult> ListTopicRulesAsync(
        string? topic = null,
        int? maxResults = null,
        string? nextToken = null,
        bool? ruleDisabled = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTopicRulesRequest();
        if (topic != null) request.Topic = topic;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (ruleDisabled.HasValue) request.RuleDisabled = ruleDisabled.Value;

        try
        {
            var resp = await client.ListTopicRulesAsync(request);
            return new ListTopicRulesResult(
                Rules: resp.Rules,
                NextToken: null);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list topic rules");
        }
    }

    /// <summary>
    /// Enable an IoT topic rule.
    /// </summary>
    public static async Task EnableTopicRuleAsync(
        string ruleName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.EnableTopicRuleAsync(
                new EnableTopicRuleRequest { RuleName = ruleName });
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to enable topic rule '{ruleName}'");
        }
    }

    /// <summary>
    /// Disable an IoT topic rule.
    /// </summary>
    public static async Task DisableTopicRuleAsync(
        string ruleName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisableTopicRuleAsync(
                new DisableTopicRuleRequest { RuleName = ruleName });
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disable topic rule '{ruleName}'");
        }
    }

    /// <summary>
    /// Replace an IoT topic rule.
    /// </summary>
    public static async Task ReplaceTopicRuleAsync(
        string ruleName,
        TopicRulePayload topicRulePayload,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ReplaceTopicRuleAsync(new ReplaceTopicRuleRequest
            {
                RuleName = ruleName,
                TopicRulePayload = topicRulePayload
            });
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to replace topic rule '{ruleName}'");
        }
    }

    // ── Job operations ──────────────────────────────────────────────

    /// <summary>
    /// Create an IoT job.
    /// </summary>
    public static async Task<CreateIoTJobResult> CreateJobAsync(
        string jobId,
        List<string> targets,
        string? document = null,
        string? documentSource = null,
        string? description = null,
        JobExecutionsRolloutConfig? jobExecutionsRolloutConfig = null,
        AbortConfig? abortConfig = null,
        TimeoutConfig? timeoutConfig = null,
        List<Amazon.IoT.Model.Tag>? tags = null,
        TargetSelection? targetSelection = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateJobRequest
        {
            JobId = jobId,
            Targets = targets
        };
        if (document != null) request.Document = document;
        if (documentSource != null) request.DocumentSource = documentSource;
        if (description != null) request.Description = description;
        if (jobExecutionsRolloutConfig != null)
            request.JobExecutionsRolloutConfig = jobExecutionsRolloutConfig;
        if (abortConfig != null) request.AbortConfig = abortConfig;
        if (timeoutConfig != null) request.TimeoutConfig = timeoutConfig;
        if (tags != null) request.Tags = tags;
        if (targetSelection != null) request.TargetSelection = targetSelection;

        try
        {
            var resp = await client.CreateJobAsync(request);
            return new CreateIoTJobResult(
                JobArn: resp.JobArn,
                JobId: resp.JobId,
                Description: resp.Description);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create job '{jobId}'");
        }
    }

    /// <summary>
    /// Delete an IoT job.
    /// </summary>
    public static async Task DeleteJobAsync(
        string jobId,
        bool? force = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteJobRequest { JobId = jobId };
        if (force.HasValue) request.Force = force.Value;

        try
        {
            await client.DeleteJobAsync(request);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete job '{jobId}'");
        }
    }

    /// <summary>
    /// Describe an IoT job.
    /// </summary>
    public static async Task<DescribeIoTJobResult> DescribeJobAsync(
        string jobId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeJobAsync(
                new DescribeJobRequest { JobId = jobId });
            return new DescribeIoTJobResult(Job: resp.Job);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe job '{jobId}'");
        }
    }

    /// <summary>
    /// List IoT jobs with optional pagination.
    /// </summary>
    public static async Task<ListIoTJobsResult> ListJobsAsync(
        JobStatus? status = null,
        TargetSelection? targetSelection = null,
        int? maxResults = null,
        string? nextToken = null,
        string? thingGroupName = null,
        string? thingGroupId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListJobsRequest();
        if (status != null) request.Status = status;
        if (targetSelection != null) request.TargetSelection = targetSelection;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;
        if (thingGroupName != null) request.ThingGroupName = thingGroupName;
        if (thingGroupId != null) request.ThingGroupId = thingGroupId;

        try
        {
            var resp = await client.ListJobsAsync(request);
            return new ListIoTJobsResult(
                Jobs: resp.Jobs,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list jobs");
        }
    }

    /// <summary>
    /// Cancel an IoT job.
    /// </summary>
    public static async Task<CancelIoTJobResult> CancelJobAsync(
        string jobId,
        string? reasonCode = null,
        string? comment = null,
        bool? force = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CancelJobRequest { JobId = jobId };
        if (reasonCode != null) request.ReasonCode = reasonCode;
        if (comment != null) request.Comment = comment;
        if (force.HasValue) request.Force = force.Value;

        try
        {
            var resp = await client.CancelJobAsync(request);
            return new CancelIoTJobResult(
                JobArn: resp.JobArn,
                JobId: resp.JobId,
                Description: resp.Description);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to cancel job '{jobId}'");
        }
    }

    // ── Job Template operations ─────────────────────────────────────

    /// <summary>
    /// Create an IoT job template.
    /// </summary>
    public static async Task<CreateIoTJobTemplateResult> CreateJobTemplateAsync(
        string jobTemplateId,
        string description,
        string? jobArn = null,
        string? document = null,
        string? documentSource = null,
        JobExecutionsRolloutConfig? jobExecutionsRolloutConfig = null,
        AbortConfig? abortConfig = null,
        TimeoutConfig? timeoutConfig = null,
        List<Amazon.IoT.Model.Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateJobTemplateRequest
        {
            JobTemplateId = jobTemplateId,
            Description = description
        };
        if (jobArn != null) request.JobArn = jobArn;
        if (document != null) request.Document = document;
        if (documentSource != null) request.DocumentSource = documentSource;
        if (jobExecutionsRolloutConfig != null)
            request.JobExecutionsRolloutConfig = jobExecutionsRolloutConfig;
        if (abortConfig != null) request.AbortConfig = abortConfig;
        if (timeoutConfig != null) request.TimeoutConfig = timeoutConfig;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateJobTemplateAsync(request);
            return new CreateIoTJobTemplateResult(
                JobTemplateArn: resp.JobTemplateArn,
                JobTemplateId: resp.JobTemplateId);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create job template '{jobTemplateId}'");
        }
    }

    /// <summary>
    /// Describe an IoT job template.
    /// </summary>
    public static async Task<DescribeIoTJobTemplateResult> DescribeJobTemplateAsync(
        string jobTemplateId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeJobTemplateAsync(
                new DescribeJobTemplateRequest { JobTemplateId = jobTemplateId });
            return new DescribeIoTJobTemplateResult(
                JobTemplateArn: resp.JobTemplateArn,
                JobTemplateId: resp.JobTemplateId,
                Description: resp.Description,
                Document: resp.Document);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe job template '{jobTemplateId}'");
        }
    }

    /// <summary>
    /// List IoT job templates with optional pagination.
    /// </summary>
    public static async Task<ListIoTJobTemplatesResult> ListJobTemplatesAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListJobTemplatesRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListJobTemplatesAsync(request);
            return new ListIoTJobTemplatesResult(
                JobTemplates: resp.JobTemplates,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list job templates");
        }
    }

    // ── Tagging operations ──────────────────────────────────────────

    /// <summary>
    /// Add tags to an IoT resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceArn,
        List<Amazon.IoT.Model.Tag> tags,
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
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from an IoT resource.
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
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for an IoT resource.
    /// </summary>
    public static async Task<IoTListTagsResult> ListTagsForResourceAsync(
        string resourceArn,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForResourceRequest { ResourceArn = resourceArn };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListTagsForResourceAsync(request);
            return new IoTListTagsResult(
                Tags: resp.Tags,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }

    // ── Fleet Indexing operations ────────────────────────────────────

    /// <summary>
    /// Search the IoT fleet index.
    /// </summary>
    public static async Task<SearchIndexResult> SearchIndexAsync(
        string queryString,
        string? indexName = null,
        int? maxResults = null,
        string? nextToken = null,
        string? queryVersion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SearchIndexRequest { QueryString = queryString };
        if (indexName != null) request.IndexName = indexName;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;
        if (queryVersion != null) request.QueryVersion = queryVersion;

        try
        {
            var resp = await client.SearchIndexAsync(request);
            return new SearchIndexResult(
                Things: resp.Things,
                ThingGroups: resp.ThingGroups,
                NextToken: resp.NextToken);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to search index");
        }
    }

    /// <summary>
    /// Describe an IoT fleet index.
    /// </summary>
    public static async Task<DescribeIndexResult> DescribeIndexAsync(
        string indexName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeIndexAsync(
                new DescribeIndexRequest { IndexName = indexName });
            return new DescribeIndexResult(
                IndexName: resp.IndexName,
                IndexStatus: resp.IndexStatus?.Value,
                Schema: resp.Schema);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe index '{indexName}'");
        }
    }

    /// <summary>
    /// Update the fleet indexing configuration.
    /// </summary>
    public static async Task UpdateIndexingConfigurationAsync(
        ThingIndexingConfiguration? thingIndexingConfiguration = null,
        ThingGroupIndexingConfiguration? thingGroupIndexingConfiguration = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateIndexingConfigurationRequest();
        if (thingIndexingConfiguration != null)
            request.ThingIndexingConfiguration = thingIndexingConfiguration;
        if (thingGroupIndexingConfiguration != null)
            request.ThingGroupIndexingConfiguration = thingGroupIndexingConfiguration;

        try
        {
            await client.UpdateIndexingConfigurationAsync(request);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update indexing configuration");
        }
    }

    /// <summary>
    /// Get the fleet indexing configuration.
    /// </summary>
    public static async Task<GetIndexingConfigurationResult>
        GetIndexingConfigurationAsync(RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetIndexingConfigurationAsync(
                new GetIndexingConfigurationRequest());
            return new GetIndexingConfigurationResult(
                ThingIndexingConfiguration: resp.ThingIndexingConfiguration,
                ThingGroupIndexingConfiguration: resp.ThingGroupIndexingConfiguration);
        }
        catch (AmazonIoTException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get indexing configuration");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateThingAsync"/>.</summary>
    public static CreateThingResult CreateThing(string thingName, string? thingTypeName = null, Dictionary<string, string>? attributes = null, RegionEndpoint? region = null)
        => CreateThingAsync(thingName, thingTypeName, attributes, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteThingAsync"/>.</summary>
    public static void DeleteThing(string thingName, long? expectedVersion = null, RegionEndpoint? region = null)
        => DeleteThingAsync(thingName, expectedVersion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeThingAsync"/>.</summary>
    public static DescribeThingResult DescribeThing(string thingName, RegionEndpoint? region = null)
        => DescribeThingAsync(thingName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListThingsAsync"/>.</summary>
    public static ListThingsResult ListThings(string? thingTypeName = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListThingsAsync(thingTypeName, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateThingAsync"/>.</summary>
    public static void UpdateThing(string thingName, string? thingTypeName = null, Dictionary<string, string>? attributes = null, bool? removeThingType = null, long? expectedVersion = null, RegionEndpoint? region = null)
        => UpdateThingAsync(thingName, thingTypeName, attributes, removeThingType, expectedVersion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateThingGroupAsync"/>.</summary>
    public static CreateThingGroupResult CreateThingGroup(string thingGroupName, string? parentGroupName = null, ThingGroupProperties? thingGroupProperties = null, List<Amazon.IoT.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateThingGroupAsync(thingGroupName, parentGroupName, thingGroupProperties, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteThingGroupAsync"/>.</summary>
    public static void DeleteThingGroup(string thingGroupName, long? expectedVersion = null, RegionEndpoint? region = null)
        => DeleteThingGroupAsync(thingGroupName, expectedVersion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeThingGroupAsync"/>.</summary>
    public static DescribeThingGroupResult DescribeThingGroup(string thingGroupName, RegionEndpoint? region = null)
        => DescribeThingGroupAsync(thingGroupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListThingGroupsAsync"/>.</summary>
    public static ListThingGroupsResult ListThingGroups(string? parentGroup = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListThingGroupsAsync(parentGroup, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddThingToThingGroupAsync"/>.</summary>
    public static void AddThingToThingGroup(string? thingGroupName = null, string? thingGroupArn = null, string? thingName = null, string? thingArn = null, bool? overrideDynamicGroups = null, RegionEndpoint? region = null)
        => AddThingToThingGroupAsync(thingGroupName, thingGroupArn, thingName, thingArn, overrideDynamicGroups, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RemoveThingFromThingGroupAsync"/>.</summary>
    public static void RemoveThingFromThingGroup(string? thingGroupName = null, string? thingGroupArn = null, string? thingName = null, string? thingArn = null, RegionEndpoint? region = null)
        => RemoveThingFromThingGroupAsync(thingGroupName, thingGroupArn, thingName, thingArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListThingsInThingGroupAsync"/>.</summary>
    public static ListThingsInThingGroupResult ListThingsInThingGroup(string thingGroupName, bool? recursive = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListThingsInThingGroupAsync(thingGroupName, recursive, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateThingTypeAsync"/>.</summary>
    public static CreateThingTypeResult CreateThingType(string thingTypeName, ThingTypeProperties? thingTypeProperties = null, List<Amazon.IoT.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateThingTypeAsync(thingTypeName, thingTypeProperties, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteThingTypeAsync"/>.</summary>
    public static void DeleteThingType(string thingTypeName, RegionEndpoint? region = null)
        => DeleteThingTypeAsync(thingTypeName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeThingTypeAsync"/>.</summary>
    public static DescribeThingTypeResult DescribeThingType(string thingTypeName, RegionEndpoint? region = null)
        => DescribeThingTypeAsync(thingTypeName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListThingTypesAsync"/>.</summary>
    public static ListThingTypesResult ListThingTypes(string? thingTypeName = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListThingTypesAsync(thingTypeName, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreatePolicyAsync"/>.</summary>
    public static CreateIoTPolicyResult CreatePolicy(string policyName, string policyDocument, List<Amazon.IoT.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreatePolicyAsync(policyName, policyDocument, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeletePolicyAsync"/>.</summary>
    public static void DeletePolicy(string policyName, RegionEndpoint? region = null)
        => DeletePolicyAsync(policyName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetPolicyAsync"/>.</summary>
    public static GetIoTPolicyResult GetPolicy(string policyName, RegionEndpoint? region = null)
        => GetPolicyAsync(policyName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListPoliciesAsync"/>.</summary>
    public static ListIoTPoliciesResult ListPolicies(bool? ascendingOrder = null, int? pageSize = null, string? marker = null, RegionEndpoint? region = null)
        => ListPoliciesAsync(ascendingOrder, pageSize, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreatePolicyVersionAsync"/>.</summary>
    public static CreatePolicyVersionResult CreatePolicyVersion(string policyName, string policyDocument, bool? setAsDefault = null, RegionEndpoint? region = null)
        => CreatePolicyVersionAsync(policyName, policyDocument, setAsDefault, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeletePolicyVersionAsync"/>.</summary>
    public static void DeletePolicyVersion(string policyName, string policyVersionId, RegionEndpoint? region = null)
        => DeletePolicyVersionAsync(policyName, policyVersionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListPolicyVersionsAsync"/>.</summary>
    public static ListPolicyVersionsResult ListPolicyVersions(string policyName, RegionEndpoint? region = null)
        => ListPolicyVersionsAsync(policyName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AttachPolicyAsync"/>.</summary>
    public static void AttachPolicy(string policyName, string target, RegionEndpoint? region = null)
        => AttachPolicyAsync(policyName, target, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DetachPolicyAsync"/>.</summary>
    public static void DetachPolicy(string policyName, string target, RegionEndpoint? region = null)
        => DetachPolicyAsync(policyName, target, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTargetsForPolicyAsync"/>.</summary>
    public static ListTargetsForPolicyResult ListTargetsForPolicy(string policyName, int? pageSize = null, string? marker = null, RegionEndpoint? region = null)
        => ListTargetsForPolicyAsync(policyName, pageSize, marker, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateCertificateFromCsrAsync"/>.</summary>
    public static CreateCertificateFromCsrResult CreateCertificateFromCsr(string certificateSigningRequest, bool? setAsActive = null, RegionEndpoint? region = null)
        => CreateCertificateFromCsrAsync(certificateSigningRequest, setAsActive, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteCertificateAsync"/>.</summary>
    public static void DeleteCertificate(string certificateId, bool? forceDelete = null, RegionEndpoint? region = null)
        => DeleteCertificateAsync(certificateId, forceDelete, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeCertificateAsync"/>.</summary>
    public static DescribeIoTCertificateResult DescribeCertificate(string certificateId, RegionEndpoint? region = null)
        => DescribeCertificateAsync(certificateId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListCertificatesAsync"/>.</summary>
    public static ListIoTCertificatesResult ListCertificates(int? pageSize = null, string? marker = null, bool? ascendingOrder = null, RegionEndpoint? region = null)
        => ListCertificatesAsync(pageSize, marker, ascendingOrder, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateCertificateAsync"/>.</summary>
    public static void UpdateCertificate(string certificateId, CertificateStatus newStatus, RegionEndpoint? region = null)
        => UpdateCertificateAsync(certificateId, newStatus, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AttachThingPrincipalAsync"/>.</summary>
    public static void AttachThingPrincipal(string thingName, string principal, RegionEndpoint? region = null)
        => AttachThingPrincipalAsync(thingName, principal, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DetachThingPrincipalAsync"/>.</summary>
    public static void DetachThingPrincipal(string thingName, string principal, RegionEndpoint? region = null)
        => DetachThingPrincipalAsync(thingName, principal, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListThingPrincipalsAsync"/>.</summary>
    public static ListThingPrincipalsResult ListThingPrincipals(string thingName, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListThingPrincipalsAsync(thingName, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateTopicRuleAsync"/>.</summary>
    public static void CreateTopicRule(string ruleName, TopicRulePayload topicRulePayload, string? tags = null, RegionEndpoint? region = null)
        => CreateTopicRuleAsync(ruleName, topicRulePayload, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteTopicRuleAsync"/>.</summary>
    public static void DeleteTopicRule(string ruleName, RegionEndpoint? region = null)
        => DeleteTopicRuleAsync(ruleName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetTopicRuleAsync"/>.</summary>
    public static GetTopicRuleResult GetTopicRule(string ruleName, RegionEndpoint? region = null)
        => GetTopicRuleAsync(ruleName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTopicRulesAsync"/>.</summary>
    public static ListTopicRulesResult ListTopicRules(string? topic = null, int? maxResults = null, string? nextToken = null, bool? ruleDisabled = null, RegionEndpoint? region = null)
        => ListTopicRulesAsync(topic, maxResults, nextToken, ruleDisabled, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EnableTopicRuleAsync"/>.</summary>
    public static void EnableTopicRule(string ruleName, RegionEndpoint? region = null)
        => EnableTopicRuleAsync(ruleName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisableTopicRuleAsync"/>.</summary>
    public static void DisableTopicRule(string ruleName, RegionEndpoint? region = null)
        => DisableTopicRuleAsync(ruleName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ReplaceTopicRuleAsync"/>.</summary>
    public static void ReplaceTopicRule(string ruleName, TopicRulePayload topicRulePayload, RegionEndpoint? region = null)
        => ReplaceTopicRuleAsync(ruleName, topicRulePayload, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateJobAsync"/>.</summary>
    public static CreateIoTJobResult CreateJob(string jobId, List<string> targets, string? document = null, string? documentSource = null, string? description = null, JobExecutionsRolloutConfig? jobExecutionsRolloutConfig = null, AbortConfig? abortConfig = null, TimeoutConfig? timeoutConfig = null, List<Amazon.IoT.Model.Tag>? tags = null, TargetSelection? targetSelection = null, RegionEndpoint? region = null)
        => CreateJobAsync(jobId, targets, document, documentSource, description, jobExecutionsRolloutConfig, abortConfig, timeoutConfig, tags, targetSelection, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteJobAsync"/>.</summary>
    public static void DeleteJob(string jobId, bool? force = null, RegionEndpoint? region = null)
        => DeleteJobAsync(jobId, force, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeJobAsync"/>.</summary>
    public static DescribeIoTJobResult DescribeJob(string jobId, RegionEndpoint? region = null)
        => DescribeJobAsync(jobId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListJobsAsync"/>.</summary>
    public static ListIoTJobsResult ListJobs(JobStatus? status = null, TargetSelection? targetSelection = null, int? maxResults = null, string? nextToken = null, string? thingGroupName = null, string? thingGroupId = null, RegionEndpoint? region = null)
        => ListJobsAsync(status, targetSelection, maxResults, nextToken, thingGroupName, thingGroupId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CancelJobAsync"/>.</summary>
    public static CancelIoTJobResult CancelJob(string jobId, string? reasonCode = null, string? comment = null, bool? force = null, RegionEndpoint? region = null)
        => CancelJobAsync(jobId, reasonCode, comment, force, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateJobTemplateAsync"/>.</summary>
    public static CreateIoTJobTemplateResult CreateJobTemplate(string jobTemplateId, string description, string? jobArn = null, string? document = null, string? documentSource = null, JobExecutionsRolloutConfig? jobExecutionsRolloutConfig = null, AbortConfig? abortConfig = null, TimeoutConfig? timeoutConfig = null, List<Amazon.IoT.Model.Tag>? tags = null, RegionEndpoint? region = null)
        => CreateJobTemplateAsync(jobTemplateId, description, jobArn, document, documentSource, jobExecutionsRolloutConfig, abortConfig, timeoutConfig, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeJobTemplateAsync"/>.</summary>
    public static DescribeIoTJobTemplateResult DescribeJobTemplate(string jobTemplateId, RegionEndpoint? region = null)
        => DescribeJobTemplateAsync(jobTemplateId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListJobTemplatesAsync"/>.</summary>
    public static ListIoTJobTemplatesResult ListJobTemplates(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListJobTemplatesAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, List<Amazon.IoT.Model.Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static IoTListTagsResult ListTagsForResource(string resourceArn, string? nextToken = null, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SearchIndexAsync"/>.</summary>
    public static SearchIndexResult SearchIndex(string queryString, string? indexName = null, int? maxResults = null, string? nextToken = null, string? queryVersion = null, RegionEndpoint? region = null)
        => SearchIndexAsync(queryString, indexName, maxResults, nextToken, queryVersion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeIndexAsync"/>.</summary>
    public static DescribeIndexResult DescribeIndex(string indexName, RegionEndpoint? region = null)
        => DescribeIndexAsync(indexName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateIndexingConfigurationAsync"/>.</summary>
    public static void UpdateIndexingConfiguration(ThingIndexingConfiguration? thingIndexingConfiguration = null, ThingGroupIndexingConfiguration? thingGroupIndexingConfiguration = null, RegionEndpoint? region = null)
        => UpdateIndexingConfigurationAsync(thingIndexingConfiguration, thingGroupIndexingConfiguration, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetIndexingConfigurationAsync"/>.</summary>
    public static GetIndexingConfigurationResult GetIndexingConfiguration(RegionEndpoint? region = null)
        => GetIndexingConfigurationAsync(region).GetAwaiter().GetResult();

}

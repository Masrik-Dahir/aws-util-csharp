using Amazon;
using Amazon.CloudFormation;
using Amazon.CloudFormation.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CreateStackResult(string? StackId = null);

public sealed record UpdateStackResult(string? StackId = null);

public sealed record DeleteStackResult(bool Success = true);

public sealed record DescribeStacksResult(
    List<Stack>? Stacks = null,
    string? NextToken = null);

public sealed record ListStacksResult(
    List<StackSummary>? StackSummaries = null,
    string? NextToken = null);

public sealed record DescribeStackResourcesResult(
    List<StackResource>? StackResources = null);

public sealed record DescribeStackEventsResult(
    List<StackEvent>? StackEvents = null,
    string? NextToken = null);

public sealed record GetTemplateResult(
    string? TemplateBody = null,
    List<string>? StagesAvailable = null);

public sealed record ValidateTemplateResult(
    List<TemplateParameter>? Parameters = null,
    string? Description = null,
    List<string>? Capabilities = null,
    string? CapabilitiesReason = null,
    List<string>? DeclaredTransforms = null);

public sealed record CreateChangeSetResult(
    string? Id = null,
    string? StackId = null);

public sealed record ExecuteChangeSetResult(bool Success = true);

public sealed record DeleteChangeSetResult(bool Success = true);

public sealed record DescribeChangeSetResult(
    string? ChangeSetName = null,
    string? ChangeSetId = null,
    string? StackId = null,
    string? StackName = null,
    string? Description = null,
    string? Status = null,
    string? StatusReason = null,
    List<Change>? Changes = null,
    string? NextToken = null,
    string? ExecutionStatus = null);

public sealed record ListChangeSetsResult(
    List<ChangeSetSummary>? Summaries = null,
    string? NextToken = null);

public sealed record ListStackResourcesResult(
    List<StackResourceSummary>? StackResourceSummaries = null,
    string? NextToken = null);

public sealed record GetTemplateSummaryResult(
    List<ParameterDeclaration>? Parameters = null,
    string? Description = null,
    List<string>? Capabilities = null,
    string? CapabilitiesReason = null,
    List<string>? ResourceTypes = null,
    string? Version = null,
    string? Metadata = null,
    List<string>? DeclaredTransforms = null);

public sealed record ListExportsResult(
    List<Export>? Exports = null,
    string? NextToken = null);

public sealed record ListImportsResult(
    List<string>? Imports = null,
    string? NextToken = null);

public sealed record DetectStackDriftResult(string? StackDriftDetectionId = null);

public sealed record DescribeStackDriftDetectionStatusResult(
    string? StackId = null,
    string? StackDriftDetectionId = null,
    string? StackDriftStatus = null,
    string? DetectionStatus = null,
    string? DetectionStatusReason = null,
    int? DriftedStackResourceCount = null,
    DateTime? Timestamp = null);

public sealed record ContinueUpdateRollbackResult(bool Success = true);

public sealed record SignalResourceResult(bool Success = true);

public sealed record EstimateTemplateCostResult(string? Url = null);

public sealed record SetStackPolicyResult(bool Success = true);

public sealed record GetStackPolicyResult(string? StackPolicyBody = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS CloudFormation.
/// </summary>
public static class CloudFormationService
{
    private static AmazonCloudFormationClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCloudFormationClient>(region);

    /// <summary>
    /// Create a new CloudFormation stack.
    /// </summary>
    public static async Task<CreateStackResult> CreateStackAsync(
        CreateStackRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateStackAsync(request);
            return new CreateStackResult(StackId: resp.StackId);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create stack");
        }
    }

    /// <summary>
    /// Update an existing CloudFormation stack.
    /// </summary>
    public static async Task<UpdateStackResult> UpdateStackAsync(
        UpdateStackRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateStackAsync(request);
            return new UpdateStackResult(StackId: resp.StackId);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update stack");
        }
    }

    /// <summary>
    /// Delete a CloudFormation stack.
    /// </summary>
    public static async Task<DeleteStackResult> DeleteStackAsync(
        string stackName,
        List<string>? retainResources = null,
        string? roleArn = null,
        string? clientRequestToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteStackRequest { StackName = stackName };
        if (retainResources != null) request.RetainResources = retainResources;
        if (roleArn != null) request.RoleARN = roleArn;
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;

        try
        {
            await client.DeleteStackAsync(request);
            return new DeleteStackResult();
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete stack '{stackName}'");
        }
    }

    /// <summary>
    /// Describe one or more CloudFormation stacks.
    /// </summary>
    public static async Task<DescribeStacksResult> DescribeStacksAsync(
        string? stackName = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeStacksRequest();
        if (stackName != null) request.StackName = stackName;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeStacksAsync(request);
            return new DescribeStacksResult(
                Stacks: resp.Stacks,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe stacks");
        }
    }

    /// <summary>
    /// List CloudFormation stacks with optional status filter.
    /// </summary>
    public static async Task<ListStacksResult> ListStacksAsync(
        List<string>? stackStatusFilter = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStacksRequest();
        if (stackStatusFilter != null)
            request.StackStatusFilter = stackStatusFilter;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListStacksAsync(request);
            return new ListStacksResult(
                StackSummaries: resp.StackSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list stacks");
        }
    }

    /// <summary>
    /// Describe resources belonging to a stack.
    /// </summary>
    public static async Task<DescribeStackResourcesResult> DescribeStackResourcesAsync(
        string? stackName = null,
        string? logicalResourceId = null,
        string? physicalResourceId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeStackResourcesRequest();
        if (stackName != null) request.StackName = stackName;
        if (logicalResourceId != null) request.LogicalResourceId = logicalResourceId;
        if (physicalResourceId != null) request.PhysicalResourceId = physicalResourceId;

        try
        {
            var resp = await client.DescribeStackResourcesAsync(request);
            return new DescribeStackResourcesResult(StackResources: resp.StackResources);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe stack resources");
        }
    }

    /// <summary>
    /// Describe events for a CloudFormation stack.
    /// </summary>
    public static async Task<DescribeStackEventsResult> DescribeStackEventsAsync(
        string? stackName = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeStackEventsRequest();
        if (stackName != null) request.StackName = stackName;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeStackEventsAsync(request);
            return new DescribeStackEventsResult(
                StackEvents: resp.StackEvents,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe stack events");
        }
    }

    /// <summary>
    /// Retrieve the template body for a stack.
    /// </summary>
    public static async Task<GetTemplateResult> GetTemplateAsync(
        string? stackName = null,
        string? changeSetName = null,
        string? templateStage = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetTemplateRequest();
        if (stackName != null) request.StackName = stackName;
        if (changeSetName != null) request.ChangeSetName = changeSetName;
        if (templateStage != null) request.TemplateStage = new TemplateStage(templateStage);

        try
        {
            var resp = await client.GetTemplateAsync(request);
            return new GetTemplateResult(
                TemplateBody: resp.TemplateBody,
                StagesAvailable: resp.StagesAvailable?.ToList());
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get template");
        }
    }

    /// <summary>
    /// Validate a CloudFormation template.
    /// </summary>
    public static async Task<ValidateTemplateResult> ValidateTemplateAsync(
        string? templateBody = null,
        string? templateUrl = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ValidateTemplateRequest();
        if (templateBody != null) request.TemplateBody = templateBody;
        if (templateUrl != null) request.TemplateURL = templateUrl;

        try
        {
            var resp = await client.ValidateTemplateAsync(request);
            return new ValidateTemplateResult(
                Parameters: resp.Parameters,
                Description: resp.Description,
                Capabilities: resp.Capabilities?.ToList(),
                CapabilitiesReason: resp.CapabilitiesReason,
                DeclaredTransforms: resp.DeclaredTransforms);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to validate template");
        }
    }

    /// <summary>
    /// Create a change set for a CloudFormation stack.
    /// </summary>
    public static async Task<CreateChangeSetResult> CreateChangeSetAsync(
        CreateChangeSetRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateChangeSetAsync(request);
            return new CreateChangeSetResult(Id: resp.Id, StackId: resp.StackId);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create change set");
        }
    }

    /// <summary>
    /// Execute a change set to apply changes to a stack.
    /// </summary>
    public static async Task<ExecuteChangeSetResult> ExecuteChangeSetAsync(
        string changeSetName,
        string? stackName = null,
        string? clientRequestToken = null,
        bool? disableRollback = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ExecuteChangeSetRequest { ChangeSetName = changeSetName };
        if (stackName != null) request.StackName = stackName;
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (disableRollback.HasValue) request.DisableRollback = disableRollback.Value;

        try
        {
            await client.ExecuteChangeSetAsync(request);
            return new ExecuteChangeSetResult();
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to execute change set '{changeSetName}'");
        }
    }

    /// <summary>
    /// Delete a change set.
    /// </summary>
    public static async Task<DeleteChangeSetResult> DeleteChangeSetAsync(
        string changeSetName,
        string? stackName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteChangeSetRequest { ChangeSetName = changeSetName };
        if (stackName != null) request.StackName = stackName;

        try
        {
            await client.DeleteChangeSetAsync(request);
            return new DeleteChangeSetResult();
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete change set '{changeSetName}'");
        }
    }

    /// <summary>
    /// Describe a change set.
    /// </summary>
    public static async Task<DescribeChangeSetResult> DescribeChangeSetAsync(
        string changeSetName,
        string? stackName = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeChangeSetRequest { ChangeSetName = changeSetName };
        if (stackName != null) request.StackName = stackName;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeChangeSetAsync(request);
            return new DescribeChangeSetResult(
                ChangeSetName: resp.ChangeSetName,
                ChangeSetId: resp.ChangeSetId,
                StackId: resp.StackId,
                StackName: resp.StackName,
                Description: resp.Description,
                Status: resp.Status?.Value,
                StatusReason: resp.StatusReason,
                Changes: resp.Changes,
                NextToken: resp.NextToken,
                ExecutionStatus: resp.ExecutionStatus?.Value);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe change set '{changeSetName}'");
        }
    }

    /// <summary>
    /// List change sets for a stack.
    /// </summary>
    public static async Task<ListChangeSetsResult> ListChangeSetAsync(
        string stackName,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListChangeSetsRequest { StackName = stackName };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListChangeSetsAsync(request);
            return new ListChangeSetsResult(
                Summaries: resp.Summaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list change sets");
        }
    }

    /// <summary>
    /// List resources in a CloudFormation stack.
    /// </summary>
    public static async Task<ListStackResourcesResult> ListStackResourcesAsync(
        string stackName,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStackResourcesRequest { StackName = stackName };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListStackResourcesAsync(request);
            return new ListStackResourcesResult(
                StackResourceSummaries: resp.StackResourceSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list stack resources");
        }
    }

    /// <summary>
    /// Get a summary of a CloudFormation template.
    /// </summary>
    public static async Task<GetTemplateSummaryResult> GetTemplateSummaryAsync(
        string? templateBody = null,
        string? templateUrl = null,
        string? stackName = null,
        string? stackSetName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetTemplateSummaryRequest();
        if (templateBody != null) request.TemplateBody = templateBody;
        if (templateUrl != null) request.TemplateURL = templateUrl;
        if (stackName != null) request.StackName = stackName;
        if (stackSetName != null) request.StackSetName = stackSetName;

        try
        {
            var resp = await client.GetTemplateSummaryAsync(request);
            return new GetTemplateSummaryResult(
                Parameters: resp.Parameters,
                Description: resp.Description,
                Capabilities: resp.Capabilities?.ToList(),
                CapabilitiesReason: resp.CapabilitiesReason,
                ResourceTypes: resp.ResourceTypes,
                Version: resp.Version,
                Metadata: resp.Metadata,
                DeclaredTransforms: resp.DeclaredTransforms);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get template summary");
        }
    }

    /// <summary>
    /// List CloudFormation exports.
    /// </summary>
    public static async Task<ListExportsResult> ListExportsAsync(
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListExportsRequest();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListExportsAsync(request);
            return new ListExportsResult(
                Exports: resp.Exports,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list exports");
        }
    }

    /// <summary>
    /// List stacks importing a specific export.
    /// </summary>
    public static async Task<ListImportsResult> ListImportsAsync(
        string exportName,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListImportsRequest { ExportName = exportName };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListImportsAsync(request);
            return new ListImportsResult(
                Imports: resp.Imports,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list imports");
        }
    }

    /// <summary>
    /// Initiate drift detection on a stack.
    /// </summary>
    public static async Task<DetectStackDriftResult> DetectStackDriftAsync(
        string stackName,
        List<string>? logicalResourceIds = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectStackDriftRequest { StackName = stackName };
        if (logicalResourceIds != null) request.LogicalResourceIds = logicalResourceIds;

        try
        {
            var resp = await client.DetectStackDriftAsync(request);
            return new DetectStackDriftResult(
                StackDriftDetectionId: resp.StackDriftDetectionId);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to detect stack drift for '{stackName}'");
        }
    }

    /// <summary>
    /// Describe the status of a drift detection operation.
    /// </summary>
    public static async Task<DescribeStackDriftDetectionStatusResult>
        DescribeStackDriftDetectionStatusAsync(
            string stackDriftDetectionId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeStackDriftDetectionStatusAsync(
                new DescribeStackDriftDetectionStatusRequest
                {
                    StackDriftDetectionId = stackDriftDetectionId
                });
            return new DescribeStackDriftDetectionStatusResult(
                StackId: resp.StackId,
                StackDriftDetectionId: resp.StackDriftDetectionId,
                StackDriftStatus: resp.StackDriftStatus?.Value,
                DetectionStatus: resp.DetectionStatus?.Value,
                DetectionStatusReason: resp.DetectionStatusReason,
                DriftedStackResourceCount: resp.DriftedStackResourceCount,
                Timestamp: resp.Timestamp);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe stack drift detection status");
        }
    }

    /// <summary>
    /// Continue an update rollback for a stack.
    /// </summary>
    public static async Task<ContinueUpdateRollbackResult> ContinueUpdateRollbackAsync(
        string stackName,
        string? roleArn = null,
        List<string>? resourcesToSkip = null,
        string? clientRequestToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ContinueUpdateRollbackRequest { StackName = stackName };
        if (roleArn != null) request.RoleARN = roleArn;
        if (resourcesToSkip != null) request.ResourcesToSkip = resourcesToSkip;
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;

        try
        {
            await client.ContinueUpdateRollbackAsync(request);
            return new ContinueUpdateRollbackResult();
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to continue update rollback for '{stackName}'");
        }
    }

    /// <summary>
    /// Send a signal to a CloudFormation resource.
    /// </summary>
    public static async Task<SignalResourceResult> SignalResourceAsync(
        string stackName,
        string logicalResourceId,
        string uniqueId,
        string status,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.SignalResourceAsync(new SignalResourceRequest
            {
                StackName = stackName,
                LogicalResourceId = logicalResourceId,
                UniqueId = uniqueId,
                Status = new ResourceSignalStatus(status)
            });
            return new SignalResourceResult();
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to signal resource");
        }
    }

    /// <summary>
    /// Estimate the cost of a CloudFormation template.
    /// </summary>
    public static async Task<EstimateTemplateCostResult> EstimateTemplateCostAsync(
        string? templateBody = null,
        string? templateUrl = null,
        List<Parameter>? parameters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new EstimateTemplateCostRequest();
        if (templateBody != null) request.TemplateBody = templateBody;
        if (templateUrl != null) request.TemplateURL = templateUrl;
        if (parameters != null) request.Parameters = parameters;

        try
        {
            var resp = await client.EstimateTemplateCostAsync(request);
            return new EstimateTemplateCostResult(Url: resp.Url);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to estimate template cost");
        }
    }

    /// <summary>
    /// Set the stack policy for a CloudFormation stack.
    /// </summary>
    public static async Task<SetStackPolicyResult> SetStackPolicyAsync(
        string stackName,
        string? stackPolicyBody = null,
        string? stackPolicyUrl = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SetStackPolicyRequest { StackName = stackName };
        if (stackPolicyBody != null) request.StackPolicyBody = stackPolicyBody;
        if (stackPolicyUrl != null) request.StackPolicyURL = stackPolicyUrl;

        try
        {
            await client.SetStackPolicyAsync(request);
            return new SetStackPolicyResult();
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to set stack policy for '{stackName}'");
        }
    }

    /// <summary>
    /// Get the stack policy for a CloudFormation stack.
    /// </summary>
    public static async Task<GetStackPolicyResult> GetStackPolicyAsync(
        string stackName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetStackPolicyAsync(new GetStackPolicyRequest
            {
                StackName = stackName
            });
            return new GetStackPolicyResult(StackPolicyBody: resp.StackPolicyBody);
        }
        catch (AmazonCloudFormationException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get stack policy for '{stackName}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateStackAsync"/>.</summary>
    public static CreateStackResult CreateStack(CreateStackRequest request, RegionEndpoint? region = null)
        => CreateStackAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateStackAsync"/>.</summary>
    public static UpdateStackResult UpdateStack(UpdateStackRequest request, RegionEndpoint? region = null)
        => UpdateStackAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteStackAsync"/>.</summary>
    public static DeleteStackResult DeleteStack(string stackName, List<string>? retainResources = null, string? roleArn = null, string? clientRequestToken = null, RegionEndpoint? region = null)
        => DeleteStackAsync(stackName, retainResources, roleArn, clientRequestToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeStacksAsync"/>.</summary>
    public static DescribeStacksResult DescribeStacks(string? stackName = null, string? nextToken = null, RegionEndpoint? region = null)
        => DescribeStacksAsync(stackName, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListStacksAsync"/>.</summary>
    public static ListStacksResult ListStacks(List<string>? stackStatusFilter = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListStacksAsync(stackStatusFilter, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeStackResourcesAsync"/>.</summary>
    public static DescribeStackResourcesResult DescribeStackResources(string? stackName = null, string? logicalResourceId = null, string? physicalResourceId = null, RegionEndpoint? region = null)
        => DescribeStackResourcesAsync(stackName, logicalResourceId, physicalResourceId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeStackEventsAsync"/>.</summary>
    public static DescribeStackEventsResult DescribeStackEvents(string? stackName = null, string? nextToken = null, RegionEndpoint? region = null)
        => DescribeStackEventsAsync(stackName, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetTemplateAsync"/>.</summary>
    public static GetTemplateResult GetTemplate(string? stackName = null, string? changeSetName = null, string? templateStage = null, RegionEndpoint? region = null)
        => GetTemplateAsync(stackName, changeSetName, templateStage, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ValidateTemplateAsync"/>.</summary>
    public static ValidateTemplateResult ValidateTemplate(string? templateBody = null, string? templateUrl = null, RegionEndpoint? region = null)
        => ValidateTemplateAsync(templateBody, templateUrl, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateChangeSetAsync"/>.</summary>
    public static CreateChangeSetResult CreateChangeSet(CreateChangeSetRequest request, RegionEndpoint? region = null)
        => CreateChangeSetAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ExecuteChangeSetAsync"/>.</summary>
    public static ExecuteChangeSetResult ExecuteChangeSet(string changeSetName, string? stackName = null, string? clientRequestToken = null, bool? disableRollback = null, RegionEndpoint? region = null)
        => ExecuteChangeSetAsync(changeSetName, stackName, clientRequestToken, disableRollback, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteChangeSetAsync"/>.</summary>
    public static DeleteChangeSetResult DeleteChangeSet(string changeSetName, string? stackName = null, RegionEndpoint? region = null)
        => DeleteChangeSetAsync(changeSetName, stackName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeChangeSetAsync"/>.</summary>
    public static DescribeChangeSetResult DescribeChangeSet(string changeSetName, string? stackName = null, string? nextToken = null, RegionEndpoint? region = null)
        => DescribeChangeSetAsync(changeSetName, stackName, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListChangeSetAsync"/>.</summary>
    public static ListChangeSetsResult ListChangeSet(string stackName, string? nextToken = null, RegionEndpoint? region = null)
        => ListChangeSetAsync(stackName, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListStackResourcesAsync"/>.</summary>
    public static ListStackResourcesResult ListStackResources(string stackName, string? nextToken = null, RegionEndpoint? region = null)
        => ListStackResourcesAsync(stackName, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetTemplateSummaryAsync"/>.</summary>
    public static GetTemplateSummaryResult GetTemplateSummary(string? templateBody = null, string? templateUrl = null, string? stackName = null, string? stackSetName = null, RegionEndpoint? region = null)
        => GetTemplateSummaryAsync(templateBody, templateUrl, stackName, stackSetName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListExportsAsync"/>.</summary>
    public static ListExportsResult ListExports(string? nextToken = null, RegionEndpoint? region = null)
        => ListExportsAsync(nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListImportsAsync"/>.</summary>
    public static ListImportsResult ListImports(string exportName, string? nextToken = null, RegionEndpoint? region = null)
        => ListImportsAsync(exportName, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DetectStackDriftAsync"/>.</summary>
    public static DetectStackDriftResult DetectStackDrift(string stackName, List<string>? logicalResourceIds = null, RegionEndpoint? region = null)
        => DetectStackDriftAsync(stackName, logicalResourceIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeStackDriftDetectionStatusAsync"/>.</summary>
    public static DescribeStackDriftDetectionStatusResult DescribeStackDriftDetectionStatus(string stackDriftDetectionId, RegionEndpoint? region = null)
        => DescribeStackDriftDetectionStatusAsync(stackDriftDetectionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ContinueUpdateRollbackAsync"/>.</summary>
    public static ContinueUpdateRollbackResult ContinueUpdateRollback(string stackName, string? roleArn = null, List<string>? resourcesToSkip = null, string? clientRequestToken = null, RegionEndpoint? region = null)
        => ContinueUpdateRollbackAsync(stackName, roleArn, resourcesToSkip, clientRequestToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SignalResourceAsync"/>.</summary>
    public static SignalResourceResult SignalResource(string stackName, string logicalResourceId, string uniqueId, string status, RegionEndpoint? region = null)
        => SignalResourceAsync(stackName, logicalResourceId, uniqueId, status, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EstimateTemplateCostAsync"/>.</summary>
    public static EstimateTemplateCostResult EstimateTemplateCost(string? templateBody = null, string? templateUrl = null, List<Parameter>? parameters = null, RegionEndpoint? region = null)
        => EstimateTemplateCostAsync(templateBody, templateUrl, parameters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SetStackPolicyAsync"/>.</summary>
    public static SetStackPolicyResult SetStackPolicy(string stackName, string? stackPolicyBody = null, string? stackPolicyUrl = null, RegionEndpoint? region = null)
        => SetStackPolicyAsync(stackName, stackPolicyBody, stackPolicyUrl, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetStackPolicyAsync"/>.</summary>
    public static GetStackPolicyResult GetStackPolicy(string stackName, RegionEndpoint? region = null)
        => GetStackPolicyAsync(stackName, region).GetAwaiter().GetResult();

}

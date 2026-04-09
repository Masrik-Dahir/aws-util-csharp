using Amazon;
using Amazon.CloudFront;
using Amazon.CloudFront.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────────────

public sealed record CreateDistributionResult(Distribution? Distribution = null, string? Location = null, string? ETag = null);
public sealed record GetDistributionResult(Distribution? Distribution = null, string? ETag = null);
public sealed record ListDistributionsResult(DistributionList? DistributionList = null);
public sealed record UpdateDistributionResult(Distribution? Distribution = null, string? ETag = null);
public sealed record GetDistributionConfigResult(DistributionConfig? DistributionConfig = null, string? ETag = null);

public sealed record CreateInvalidationResult(Invalidation? Invalidation = null, string? Location = null);
public sealed record GetInvalidationResult(Invalidation? Invalidation = null);
public sealed record ListInvalidationsResult(InvalidationList? InvalidationList = null);

public sealed record CreateOriginAccessControlResult(OriginAccessControl? OriginAccessControl = null, string? Location = null, string? ETag = null);
public sealed record GetOriginAccessControlResult(OriginAccessControl? OriginAccessControl = null, string? ETag = null);
public sealed record ListOriginAccessControlsResult(OriginAccessControlList? OriginAccessControlList = null);
public sealed record UpdateOriginAccessControlResult(OriginAccessControl? OriginAccessControl = null, string? ETag = null);

public sealed record CreateCachePolicyResult(CachePolicy? CachePolicy = null, string? Location = null, string? ETag = null);
public sealed record GetCachePolicyResult(CachePolicy? CachePolicy = null, string? ETag = null);
public sealed record ListCachePoliciesResult(CachePolicyList? CachePolicyList = null);
public sealed record UpdateCachePolicyResult(CachePolicy? CachePolicy = null, string? ETag = null);

public sealed record CreateFunctionResult(FunctionSummary? FunctionSummary = null, string? Location = null, string? ETag = null);
public sealed record DescribeFunctionResult(FunctionSummary? FunctionSummary = null, string? ETag = null);
public sealed record CloudFrontGetFunctionResult(MemoryStream? FunctionCode = null, string? ContentType = null, string? ETag = null);
public sealed record CloudFrontListFunctionsResult(FunctionList? FunctionList = null);
public sealed record PublishFunctionResult(FunctionSummary? FunctionSummary = null);
public sealed record TestFunctionResult(TestResult? TestResult = null);
public sealed record UpdateFunctionResult(FunctionSummary? FunctionSummary = null, string? ETag = null);

public sealed record CreateResponseHeadersPolicyResult(ResponseHeadersPolicy? ResponseHeadersPolicy = null, string? Location = null, string? ETag = null);
public sealed record GetResponseHeadersPolicyResult(ResponseHeadersPolicy? ResponseHeadersPolicy = null, string? ETag = null);
public sealed record ListResponseHeadersPoliciesResult(ResponseHeadersPolicyList? ResponseHeadersPolicyList = null);

public sealed record CloudFrontTagsResult(Tags? Tags = null);

// ── Service ─────────────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon CloudFront.
/// </summary>
public static class CloudFrontService
{
    private static AmazonCloudFrontClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCloudFrontClient>(region);

    // ── Distributions ───────────────────────────────────────────────────

    /// <summary>
    /// Create a CloudFront distribution.
    /// </summary>
    public static async Task<CreateDistributionResult> CreateDistributionAsync(
        CreateDistributionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDistributionAsync(request);
            return new CreateDistributionResult(
                Distribution: resp.Distribution,
                Location: resp.Location,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create distribution");
        }
    }

    /// <summary>
    /// Delete a CloudFront distribution.
    /// </summary>
    public static async Task DeleteDistributionAsync(
        string id,
        string ifMatch,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDistributionAsync(new DeleteDistributionRequest
            {
                Id = id,
                IfMatch = ifMatch
            });
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete distribution '{id}'");
        }
    }

    /// <summary>
    /// Get a CloudFront distribution.
    /// </summary>
    public static async Task<GetDistributionResult> GetDistributionAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDistributionAsync(new GetDistributionRequest
            {
                Id = id
            });
            return new GetDistributionResult(
                Distribution: resp.Distribution,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get distribution '{id}'");
        }
    }

    /// <summary>
    /// List CloudFront distributions.
    /// </summary>
    public static async Task<ListDistributionsResult> ListDistributionsAsync(
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDistributionsRequest();
        if (marker != null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value.ToString();

        try
        {
            var resp = await client.ListDistributionsAsync(request);
            return new ListDistributionsResult(DistributionList: resp.DistributionList);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list distributions");
        }
    }

    /// <summary>
    /// Update a CloudFront distribution.
    /// </summary>
    public static async Task<UpdateDistributionResult> UpdateDistributionAsync(
        UpdateDistributionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateDistributionAsync(request);
            return new UpdateDistributionResult(
                Distribution: resp.Distribution,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update distribution");
        }
    }

    /// <summary>
    /// Get a CloudFront distribution configuration.
    /// </summary>
    public static async Task<GetDistributionConfigResult> GetDistributionConfigAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDistributionConfigAsync(new GetDistributionConfigRequest
            {
                Id = id
            });
            return new GetDistributionConfigResult(
                DistributionConfig: resp.DistributionConfig,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get distribution config for '{id}'");
        }
    }

    // ── Invalidations ───────────────────────────────────────────────────

    /// <summary>
    /// Create an invalidation for a distribution.
    /// </summary>
    public static async Task<CreateInvalidationResult> CreateInvalidationAsync(
        CreateInvalidationRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateInvalidationAsync(request);
            return new CreateInvalidationResult(
                Invalidation: resp.Invalidation,
                Location: resp.Location);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create invalidation");
        }
    }

    /// <summary>
    /// Get an invalidation.
    /// </summary>
    public static async Task<GetInvalidationResult> GetInvalidationAsync(
        string distributionId,
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetInvalidationAsync(new GetInvalidationRequest
            {
                DistributionId = distributionId,
                Id = id
            });
            return new GetInvalidationResult(Invalidation: resp.Invalidation);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get invalidation '{id}' for distribution '{distributionId}'");
        }
    }

    /// <summary>
    /// List invalidations for a distribution.
    /// </summary>
    public static async Task<ListInvalidationsResult> ListInvalidationsAsync(
        string distributionId,
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListInvalidationsRequest
        {
            DistributionId = distributionId
        };
        if (marker != null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value.ToString();

        try
        {
            var resp = await client.ListInvalidationsAsync(request);
            return new ListInvalidationsResult(InvalidationList: resp.InvalidationList);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list invalidations for distribution '{distributionId}'");
        }
    }

    // ── Origin Access Controls ──────────────────────────────────────────

    /// <summary>
    /// Create an origin access control.
    /// </summary>
    public static async Task<CreateOriginAccessControlResult> CreateOriginAccessControlAsync(
        CreateOriginAccessControlRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateOriginAccessControlAsync(request);
            return new CreateOriginAccessControlResult(
                OriginAccessControl: resp.OriginAccessControl,
                Location: resp.Location,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create origin access control");
        }
    }

    /// <summary>
    /// Delete an origin access control.
    /// </summary>
    public static async Task DeleteOriginAccessControlAsync(
        string id,
        string ifMatch,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteOriginAccessControlAsync(new DeleteOriginAccessControlRequest
            {
                Id = id,
                IfMatch = ifMatch
            });
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete origin access control '{id}'");
        }
    }

    /// <summary>
    /// Get an origin access control.
    /// </summary>
    public static async Task<GetOriginAccessControlResult> GetOriginAccessControlAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetOriginAccessControlAsync(new GetOriginAccessControlRequest
            {
                Id = id
            });
            return new GetOriginAccessControlResult(
                OriginAccessControl: resp.OriginAccessControl,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get origin access control '{id}'");
        }
    }

    /// <summary>
    /// List origin access controls.
    /// </summary>
    public static async Task<ListOriginAccessControlsResult> ListOriginAccessControlsAsync(
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListOriginAccessControlsRequest();
        if (marker != null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value.ToString();

        try
        {
            var resp = await client.ListOriginAccessControlsAsync(request);
            return new ListOriginAccessControlsResult(
                OriginAccessControlList: resp.OriginAccessControlList);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list origin access controls");
        }
    }

    /// <summary>
    /// Update an origin access control.
    /// </summary>
    public static async Task<UpdateOriginAccessControlResult> UpdateOriginAccessControlAsync(
        UpdateOriginAccessControlRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateOriginAccessControlAsync(request);
            return new UpdateOriginAccessControlResult(
                OriginAccessControl: resp.OriginAccessControl,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update origin access control");
        }
    }

    // ── Cache Policies ──────────────────────────────────────────────────

    /// <summary>
    /// Create a cache policy.
    /// </summary>
    public static async Task<CreateCachePolicyResult> CreateCachePolicyAsync(
        CreateCachePolicyRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateCachePolicyAsync(request);
            return new CreateCachePolicyResult(
                CachePolicy: resp.CachePolicy,
                Location: resp.Location,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create cache policy");
        }
    }

    /// <summary>
    /// Delete a cache policy.
    /// </summary>
    public static async Task DeleteCachePolicyAsync(
        string id,
        string ifMatch,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteCachePolicyAsync(new DeleteCachePolicyRequest
            {
                Id = id,
                IfMatch = ifMatch
            });
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete cache policy '{id}'");
        }
    }

    /// <summary>
    /// Get a cache policy.
    /// </summary>
    public static async Task<GetCachePolicyResult> GetCachePolicyAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetCachePolicyAsync(new GetCachePolicyRequest
            {
                Id = id
            });
            return new GetCachePolicyResult(
                CachePolicy: resp.CachePolicy,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get cache policy '{id}'");
        }
    }

    /// <summary>
    /// List cache policies.
    /// </summary>
    public static async Task<ListCachePoliciesResult> ListCachePoliciesAsync(
        string? type = null,
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListCachePoliciesRequest();
        if (type != null) request.Type = type;
        if (marker != null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value.ToString();

        try
        {
            var resp = await client.ListCachePoliciesAsync(request);
            return new ListCachePoliciesResult(CachePolicyList: resp.CachePolicyList);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list cache policies");
        }
    }

    /// <summary>
    /// Update a cache policy.
    /// </summary>
    public static async Task<UpdateCachePolicyResult> UpdateCachePolicyAsync(
        UpdateCachePolicyRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateCachePolicyAsync(request);
            return new UpdateCachePolicyResult(
                CachePolicy: resp.CachePolicy,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update cache policy");
        }
    }

    // ── Functions ────────────────────────────────────────────────────────

    /// <summary>
    /// Create a CloudFront function.
    /// </summary>
    public static async Task<CreateFunctionResult> CreateFunctionAsync(
        CreateFunctionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateFunctionAsync(request);
            return new CreateFunctionResult(
                FunctionSummary: resp.FunctionSummary,
                Location: resp.Location,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create function");
        }
    }

    /// <summary>
    /// Delete a CloudFront function.
    /// </summary>
    public static async Task DeleteFunctionAsync(
        string name,
        string ifMatch,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteFunctionAsync(new DeleteFunctionRequest
            {
                Name = name,
                IfMatch = ifMatch
            });
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete function '{name}'");
        }
    }

    /// <summary>
    /// Describe a CloudFront function.
    /// </summary>
    public static async Task<DescribeFunctionResult> DescribeFunctionAsync(
        string name,
        string? stage = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeFunctionRequest { Name = name };
        if (stage != null) request.Stage = stage;

        try
        {
            var resp = await client.DescribeFunctionAsync(request);
            return new DescribeFunctionResult(
                FunctionSummary: resp.FunctionSummary,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to describe function '{name}'");
        }
    }

    /// <summary>
    /// Get the code of a CloudFront function.
    /// </summary>
    public static async Task<CloudFrontGetFunctionResult> GetFunctionAsync(
        string name,
        string? stage = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetFunctionRequest { Name = name };
        if (stage != null) request.Stage = stage;

        try
        {
            var resp = await client.GetFunctionAsync(request);
            return new CloudFrontGetFunctionResult(
                FunctionCode: resp.FunctionCode,
                ContentType: resp.ContentType,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get function '{name}'");
        }
    }

    /// <summary>
    /// List CloudFront functions.
    /// </summary>
    public static async Task<CloudFrontListFunctionsResult> ListFunctionsAsync(
        string? marker = null,
        int? maxItems = null,
        string? stage = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListFunctionsRequest();
        if (marker != null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value.ToString();
        if (stage != null) request.Stage = stage;

        try
        {
            var resp = await client.ListFunctionsAsync(request);
            return new CloudFrontListFunctionsResult(FunctionList: resp.FunctionList);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list functions");
        }
    }

    /// <summary>
    /// Publish a CloudFront function from DEVELOPMENT to LIVE stage.
    /// </summary>
    public static async Task<PublishFunctionResult> PublishFunctionAsync(
        string name,
        string ifMatch,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PublishFunctionAsync(new PublishFunctionRequest
            {
                Name = name,
                IfMatch = ifMatch
            });
            return new PublishFunctionResult(FunctionSummary: resp.FunctionSummary);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to publish function '{name}'");
        }
    }

    /// <summary>
    /// Test a CloudFront function.
    /// </summary>
    public static async Task<TestFunctionResult> TestFunctionAsync(
        TestFunctionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.TestFunctionAsync(request);
            return new TestFunctionResult(TestResult: resp.TestResult);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to test function");
        }
    }

    /// <summary>
    /// Update a CloudFront function.
    /// </summary>
    public static async Task<UpdateFunctionResult> UpdateFunctionAsync(
        UpdateFunctionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateFunctionAsync(request);
            return new UpdateFunctionResult(
                FunctionSummary: resp.FunctionSummary,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to update function");
        }
    }

    // ── Response Headers Policies ───────────────────────────────────────

    /// <summary>
    /// Create a response headers policy.
    /// </summary>
    public static async Task<CreateResponseHeadersPolicyResult> CreateResponseHeadersPolicyAsync(
        CreateResponseHeadersPolicyRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateResponseHeadersPolicyAsync(request);
            return new CreateResponseHeadersPolicyResult(
                ResponseHeadersPolicy: resp.ResponseHeadersPolicy,
                Location: resp.Location,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create response headers policy");
        }
    }

    /// <summary>
    /// Delete a response headers policy.
    /// </summary>
    public static async Task DeleteResponseHeadersPolicyAsync(
        string id,
        string ifMatch,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteResponseHeadersPolicyAsync(new DeleteResponseHeadersPolicyRequest
            {
                Id = id,
                IfMatch = ifMatch
            });
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete response headers policy '{id}'");
        }
    }

    /// <summary>
    /// Get a response headers policy.
    /// </summary>
    public static async Task<GetResponseHeadersPolicyResult> GetResponseHeadersPolicyAsync(
        string id,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetResponseHeadersPolicyAsync(new GetResponseHeadersPolicyRequest
            {
                Id = id
            });
            return new GetResponseHeadersPolicyResult(
                ResponseHeadersPolicy: resp.ResponseHeadersPolicy,
                ETag: resp.ETag);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get response headers policy '{id}'");
        }
    }

    /// <summary>
    /// List response headers policies.
    /// </summary>
    public static async Task<ListResponseHeadersPoliciesResult> ListResponseHeadersPoliciesAsync(
        string? type = null,
        string? marker = null,
        int? maxItems = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListResponseHeadersPoliciesRequest();
        if (type != null) request.Type = type;
        if (marker != null) request.Marker = marker;
        if (maxItems.HasValue) request.MaxItems = maxItems.Value.ToString();

        try
        {
            var resp = await client.ListResponseHeadersPoliciesAsync(request);
            return new ListResponseHeadersPoliciesResult(
                ResponseHeadersPolicyList: resp.ResponseHeadersPolicyList);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list response headers policies");
        }
    }

    // ── Tagging ─────────────────────────────────────────────────────────

    /// <summary>
    /// Tag a CloudFront resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resource,
        Tags tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                Resource = resource,
                Tags = tags
            });
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to tag resource '{resource}'");
        }
    }

    /// <summary>
    /// Remove tags from a CloudFront resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string resource,
        TagKeys tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                Resource = resource,
                TagKeys = tagKeys
            });
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to untag resource '{resource}'");
        }
    }

    /// <summary>
    /// List tags for a CloudFront resource.
    /// </summary>
    public static async Task<CloudFrontTagsResult> ListTagsForResourceAsync(
        string resource,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(new ListTagsForResourceRequest
            {
                Resource = resource
            });
            return new CloudFrontTagsResult(Tags: resp.Tags);
        }
        catch (AmazonCloudFrontException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resource}'");
        }
    }
}

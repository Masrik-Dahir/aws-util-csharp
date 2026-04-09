using Amazon;
using Amazon.ECR;
using Amazon.ECR.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for ECR operations.
/// </summary>
public sealed record CreateRepositoryResult(
    string? RepositoryArn = null, string? RepositoryName = null,
    string? RepositoryUri = null, string? RegistryId = null);

public sealed record DeleteRepositoryResult(
    string? RepositoryArn = null, string? RepositoryName = null,
    string? RegistryId = null);

public sealed record DescribeRepositoriesResult(
    List<Repository>? Repositories = null, string? NextToken = null);

public sealed record EcrListImagesResult(
    List<ImageIdentifier>? ImageIds = null, string? NextToken = null);

public sealed record EcrDescribeImagesResult(
    List<ImageDetail>? ImageDetails = null, string? NextToken = null);

public sealed record EcrBatchDeleteImageResult(
    List<ImageIdentifier>? ImageIds = null,
    List<ImageFailure>? Failures = null);

public sealed record EcrBatchGetImageResult(
    List<Image>? Images = null, List<ImageFailure>? Failures = null);

public sealed record EcrGetAuthorizationTokenResult(
    List<AuthorizationData>? AuthorizationData = null);

public sealed record EcrPutImageResult(
    string? ImageDigest = null, string? ImageTag = null,
    string? RegistryId = null, string? RepositoryName = null);

public sealed record EcrPutImageTagMutabilityResult(
    string? RegistryId = null, string? RepositoryName = null,
    string? ImageTagMutability = null);

public sealed record EcrPutImageScanningConfigurationResult(
    string? RegistryId = null, string? RepositoryName = null,
    ImageScanningConfiguration? ImageScanningConfiguration = null);

public sealed record EcrSetRepositoryPolicyResult(
    string? RegistryId = null, string? RepositoryName = null,
    string? PolicyText = null);

public sealed record EcrGetRepositoryPolicyResult(
    string? RegistryId = null, string? RepositoryName = null,
    string? PolicyText = null);

public sealed record EcrDeleteRepositoryPolicyResult(
    string? RegistryId = null, string? RepositoryName = null,
    string? PolicyText = null);

public sealed record EcrStartImageScanResult(
    string? RegistryId = null, string? RepositoryName = null,
    ImageIdentifier? ImageId = null, string? ImageScanStatus = null);

public sealed record EcrDescribeImageScanFindingsResult(
    string? RegistryId = null, string? RepositoryName = null,
    ImageIdentifier? ImageId = null,
    ImageScanFindings? ImageScanFindings = null, string? NextToken = null);

public sealed record EcrGetLifecyclePolicyResult(
    string? RegistryId = null, string? RepositoryName = null,
    string? LifecyclePolicyText = null, string? LastEvaluatedAt = null);

public sealed record EcrPutLifecyclePolicyResult(
    string? RegistryId = null, string? RepositoryName = null,
    string? LifecyclePolicyText = null);

public sealed record EcrDeleteLifecyclePolicyResult(
    string? RegistryId = null, string? RepositoryName = null,
    string? LifecyclePolicyText = null, string? LastEvaluatedAt = null);

public sealed record EcrGetLifecyclePolicyPreviewResult(
    string? RegistryId = null, string? RepositoryName = null,
    string? Status = null,
    List<LifecyclePolicyPreviewResult>? PreviewResults = null,
    string? NextToken = null);

public sealed record EcrStartLifecyclePolicyPreviewResult(
    string? RegistryId = null, string? RepositoryName = null,
    string? LifecyclePolicyText = null, string? Status = null);

public sealed record EcrTagResourceResult(bool Success = true);
public sealed record EcrUntagResourceResult(bool Success = true);

public sealed record EcrListTagsForResourceResult(
    List<Amazon.ECR.Model.Tag>? Tags = null);

public sealed record EcrPutRegistryScanningConfigurationResult(
    RegistryScanningConfiguration? ScanningConfiguration = null);

public sealed record EcrGetRegistryScanningConfigurationResult(
    RegistryScanningConfiguration? ScanningConfiguration = null);

public sealed record EcrBatchGetRepositoryScanningConfigurationResult(
    List<RepositoryScanningConfiguration>? ScanningConfigurations = null,
    List<RepositoryScanningConfigurationFailure>? Failures = null);

public sealed record EcrCreatePullThroughCacheRuleResult(
    string? EcrRepositoryPrefix = null, string? UpstreamRegistryUrl = null,
    string? RegistryId = null);

public sealed record EcrDeletePullThroughCacheRuleResult(
    string? EcrRepositoryPrefix = null, string? UpstreamRegistryUrl = null,
    string? RegistryId = null);

public sealed record EcrDescribePullThroughCacheRulesResult(
    List<PullThroughCacheRule>? PullThroughCacheRules = null,
    string? NextToken = null);

/// <summary>
/// Utility helpers for Amazon Elastic Container Registry (ECR).
/// </summary>
public static class EcrService
{
    private static AmazonECRClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonECRClient>(region);

    /// <summary>
    /// Create a new ECR repository.
    /// </summary>
    public static async Task<CreateRepositoryResult> CreateRepositoryAsync(
        CreateRepositoryRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateRepositoryAsync(request);
            return new CreateRepositoryResult(
                RepositoryArn: resp.Repository.RepositoryArn,
                RepositoryName: resp.Repository.RepositoryName,
                RepositoryUri: resp.Repository.RepositoryUri,
                RegistryId: resp.Repository.RegistryId);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create ECR repository");
        }
    }

    /// <summary>
    /// Delete an ECR repository.
    /// </summary>
    public static async Task<DeleteRepositoryResult> DeleteRepositoryAsync(
        string repositoryName, string? registryId = null,
        bool force = false, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteRepositoryRequest
        {
            RepositoryName = repositoryName,
            Force = force
        };
        if (registryId != null) request.RegistryId = registryId;

        try
        {
            var resp = await client.DeleteRepositoryAsync(request);
            return new DeleteRepositoryResult(
                RepositoryArn: resp.Repository.RepositoryArn,
                RepositoryName: resp.Repository.RepositoryName,
                RegistryId: resp.Repository.RegistryId);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete ECR repository '{repositoryName}'");
        }
    }

    /// <summary>
    /// Describe ECR repositories.
    /// </summary>
    public static async Task<DescribeRepositoriesResult> DescribeRepositoriesAsync(
        DescribeRepositoriesRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeRepositoriesAsync(request);
            return new DescribeRepositoriesResult(
                Repositories: resp.Repositories,
                NextToken: resp.NextToken);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe ECR repositories");
        }
    }

    /// <summary>
    /// List images in an ECR repository.
    /// </summary>
    public static async Task<EcrListImagesResult> ListImagesAsync(
        ListImagesRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListImagesAsync(request);
            return new EcrListImagesResult(
                ImageIds: resp.ImageIds,
                NextToken: resp.NextToken);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list ECR images");
        }
    }

    /// <summary>
    /// Describe images in an ECR repository.
    /// </summary>
    public static async Task<EcrDescribeImagesResult> DescribeImagesAsync(
        DescribeImagesRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeImagesAsync(request);
            return new EcrDescribeImagesResult(
                ImageDetails: resp.ImageDetails,
                NextToken: resp.NextToken);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe ECR images");
        }
    }

    /// <summary>
    /// Batch delete images from an ECR repository.
    /// </summary>
    public static async Task<EcrBatchDeleteImageResult> BatchDeleteImageAsync(
        BatchDeleteImageRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchDeleteImageAsync(request);
            return new EcrBatchDeleteImageResult(
                ImageIds: resp.ImageIds,
                Failures: resp.Failures);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to batch delete ECR images");
        }
    }

    /// <summary>
    /// Batch get images from an ECR repository.
    /// </summary>
    public static async Task<EcrBatchGetImageResult> BatchGetImageAsync(
        BatchGetImageRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetImageAsync(request);
            return new EcrBatchGetImageResult(
                Images: resp.Images,
                Failures: resp.Failures);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to batch get ECR images");
        }
    }

    /// <summary>
    /// Get an authorization token for ECR.
    /// </summary>
    public static async Task<EcrGetAuthorizationTokenResult>
        GetAuthorizationTokenAsync(RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAuthorizationTokenAsync(
                new GetAuthorizationTokenRequest());
            return new EcrGetAuthorizationTokenResult(
                AuthorizationData: resp.AuthorizationData);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get ECR authorization token");
        }
    }

    /// <summary>
    /// Put an image into an ECR repository.
    /// </summary>
    public static async Task<EcrPutImageResult> PutImageAsync(
        PutImageRequest request, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutImageAsync(request);
            return new EcrPutImageResult(
                ImageDigest: resp.Image.ImageId.ImageDigest,
                ImageTag: resp.Image.ImageId.ImageTag,
                RegistryId: resp.Image.RegistryId,
                RepositoryName: resp.Image.RepositoryName);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put ECR image");
        }
    }

    /// <summary>
    /// Set image tag mutability for an ECR repository.
    /// </summary>
    public static async Task<EcrPutImageTagMutabilityResult>
        PutImageTagMutabilityAsync(
            string repositoryName, string imageTagMutability,
            string? registryId = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutImageTagMutabilityRequest
        {
            RepositoryName = repositoryName,
            ImageTagMutability = new ImageTagMutability(imageTagMutability)
        };
        if (registryId != null) request.RegistryId = registryId;

        try
        {
            var resp = await client.PutImageTagMutabilityAsync(request);
            return new EcrPutImageTagMutabilityResult(
                RegistryId: resp.RegistryId,
                RepositoryName: resp.RepositoryName,
                ImageTagMutability: resp.ImageTagMutability?.Value);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put image tag mutability");
        }
    }

    /// <summary>
    /// Configure image scanning for an ECR repository.
    /// </summary>
    public static async Task<EcrPutImageScanningConfigurationResult>
        PutImageScanningConfigurationAsync(
            string repositoryName,
            ImageScanningConfiguration imageScanningConfiguration,
            string? registryId = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutImageScanningConfigurationRequest
        {
            RepositoryName = repositoryName,
            ImageScanningConfiguration = imageScanningConfiguration
        };
        if (registryId != null) request.RegistryId = registryId;

        try
        {
            var resp = await client.PutImageScanningConfigurationAsync(request);
            return new EcrPutImageScanningConfigurationResult(
                RegistryId: resp.RegistryId,
                RepositoryName: resp.RepositoryName,
                ImageScanningConfiguration: resp.ImageScanningConfiguration);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put image scanning configuration");
        }
    }

    /// <summary>
    /// Set a repository policy.
    /// </summary>
    public static async Task<EcrSetRepositoryPolicyResult>
        SetRepositoryPolicyAsync(
            string repositoryName, string policyText,
            bool? force = null, string? registryId = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SetRepositoryPolicyRequest
        {
            RepositoryName = repositoryName,
            PolicyText = policyText
        };
        if (force.HasValue) request.Force = force.Value;
        if (registryId != null) request.RegistryId = registryId;

        try
        {
            var resp = await client.SetRepositoryPolicyAsync(request);
            return new EcrSetRepositoryPolicyResult(
                RegistryId: resp.RegistryId,
                RepositoryName: resp.RepositoryName,
                PolicyText: resp.PolicyText);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to set repository policy");
        }
    }

    /// <summary>
    /// Get a repository policy.
    /// </summary>
    public static async Task<EcrGetRepositoryPolicyResult>
        GetRepositoryPolicyAsync(
            string repositoryName, string? registryId = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetRepositoryPolicyRequest
        {
            RepositoryName = repositoryName
        };
        if (registryId != null) request.RegistryId = registryId;

        try
        {
            var resp = await client.GetRepositoryPolicyAsync(request);
            return new EcrGetRepositoryPolicyResult(
                RegistryId: resp.RegistryId,
                RepositoryName: resp.RepositoryName,
                PolicyText: resp.PolicyText);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get repository policy");
        }
    }

    /// <summary>
    /// Delete a repository policy.
    /// </summary>
    public static async Task<EcrDeleteRepositoryPolicyResult>
        DeleteRepositoryPolicyAsync(
            string repositoryName, string? registryId = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteRepositoryPolicyRequest
        {
            RepositoryName = repositoryName
        };
        if (registryId != null) request.RegistryId = registryId;

        try
        {
            var resp = await client.DeleteRepositoryPolicyAsync(request);
            return new EcrDeleteRepositoryPolicyResult(
                RegistryId: resp.RegistryId,
                RepositoryName: resp.RepositoryName,
                PolicyText: resp.PolicyText);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete repository policy");
        }
    }

    /// <summary>
    /// Start an image scan.
    /// </summary>
    public static async Task<EcrStartImageScanResult> StartImageScanAsync(
        string repositoryName, ImageIdentifier imageId,
        string? registryId = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartImageScanRequest
        {
            RepositoryName = repositoryName,
            ImageId = imageId
        };
        if (registryId != null) request.RegistryId = registryId;

        try
        {
            var resp = await client.StartImageScanAsync(request);
            return new EcrStartImageScanResult(
                RegistryId: resp.RegistryId,
                RepositoryName: resp.RepositoryName,
                ImageId: resp.ImageId,
                ImageScanStatus: resp.ImageScanStatus?.Status?.Value);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start image scan");
        }
    }

    /// <summary>
    /// Describe image scan findings.
    /// </summary>
    public static async Task<EcrDescribeImageScanFindingsResult>
        DescribeImageScanFindingsAsync(
            DescribeImageScanFindingsRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeImageScanFindingsAsync(request);
            return new EcrDescribeImageScanFindingsResult(
                RegistryId: resp.RegistryId,
                RepositoryName: resp.RepositoryName,
                ImageId: resp.ImageId,
                ImageScanFindings: resp.ImageScanFindings,
                NextToken: resp.NextToken);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe image scan findings");
        }
    }

    /// <summary>
    /// Get lifecycle policy for a repository.
    /// </summary>
    public static async Task<EcrGetLifecyclePolicyResult>
        GetLifecyclePolicyAsync(
            string repositoryName, string? registryId = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetLifecyclePolicyRequest
        {
            RepositoryName = repositoryName
        };
        if (registryId != null) request.RegistryId = registryId;

        try
        {
            var resp = await client.GetLifecyclePolicyAsync(request);
            return new EcrGetLifecyclePolicyResult(
                RegistryId: resp.RegistryId,
                RepositoryName: resp.RepositoryName,
                LifecyclePolicyText: resp.LifecyclePolicyText,
                LastEvaluatedAt: resp.LastEvaluatedAt.ToString());
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get lifecycle policy");
        }
    }

    /// <summary>
    /// Put lifecycle policy for a repository.
    /// </summary>
    public static async Task<EcrPutLifecyclePolicyResult>
        PutLifecyclePolicyAsync(
            string repositoryName, string lifecyclePolicyText,
            string? registryId = null, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutLifecyclePolicyRequest
        {
            RepositoryName = repositoryName,
            LifecyclePolicyText = lifecyclePolicyText
        };
        if (registryId != null) request.RegistryId = registryId;

        try
        {
            var resp = await client.PutLifecyclePolicyAsync(request);
            return new EcrPutLifecyclePolicyResult(
                RegistryId: resp.RegistryId,
                RepositoryName: resp.RepositoryName,
                LifecyclePolicyText: resp.LifecyclePolicyText);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put lifecycle policy");
        }
    }

    /// <summary>
    /// Delete lifecycle policy for a repository.
    /// </summary>
    public static async Task<EcrDeleteLifecyclePolicyResult>
        DeleteLifecyclePolicyAsync(
            string repositoryName, string? registryId = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteLifecyclePolicyRequest
        {
            RepositoryName = repositoryName
        };
        if (registryId != null) request.RegistryId = registryId;

        try
        {
            var resp = await client.DeleteLifecyclePolicyAsync(request);
            return new EcrDeleteLifecyclePolicyResult(
                RegistryId: resp.RegistryId,
                RepositoryName: resp.RepositoryName,
                LifecyclePolicyText: resp.LifecyclePolicyText,
                LastEvaluatedAt: resp.LastEvaluatedAt.ToString());
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete lifecycle policy");
        }
    }

    /// <summary>
    /// Get lifecycle policy preview results.
    /// </summary>
    public static async Task<EcrGetLifecyclePolicyPreviewResult>
        GetLifecyclePolicyPreviewAsync(
            GetLifecyclePolicyPreviewRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetLifecyclePolicyPreviewAsync(request);
            return new EcrGetLifecyclePolicyPreviewResult(
                RegistryId: resp.RegistryId,
                RepositoryName: resp.RepositoryName,
                Status: resp.Status?.Value,
                PreviewResults: resp.PreviewResults,
                NextToken: resp.NextToken);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get lifecycle policy preview");
        }
    }

    /// <summary>
    /// Start a lifecycle policy preview.
    /// </summary>
    public static async Task<EcrStartLifecyclePolicyPreviewResult>
        StartLifecyclePolicyPreviewAsync(
            StartLifecyclePolicyPreviewRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartLifecyclePolicyPreviewAsync(request);
            return new EcrStartLifecyclePolicyPreviewResult(
                RegistryId: resp.RegistryId,
                RepositoryName: resp.RepositoryName,
                LifecyclePolicyText: resp.LifecyclePolicyText,
                Status: resp.Status?.Value);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start lifecycle policy preview");
        }
    }

    /// <summary>
    /// Tag an ECR resource.
    /// </summary>
    public static async Task<EcrTagResourceResult> TagResourceAsync(
        string resourceArn, List<Amazon.ECR.Model.Tag> tags,
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
            return new EcrTagResourceResult();
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to tag ECR resource");
        }
    }

    /// <summary>
    /// Remove tags from an ECR resource.
    /// </summary>
    public static async Task<EcrUntagResourceResult> UntagResourceAsync(
        string resourceArn, List<string> tagKeys,
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
            return new EcrUntagResourceResult();
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to untag ECR resource");
        }
    }

    /// <summary>
    /// List tags for an ECR resource.
    /// </summary>
    public static async Task<EcrListTagsForResourceResult>
        ListTagsForResourceAsync(
            string resourceArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new EcrListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for ECR resource");
        }
    }

    /// <summary>
    /// Configure registry-level scanning.
    /// </summary>
    public static async Task<EcrPutRegistryScanningConfigurationResult>
        PutRegistryScanningConfigurationAsync(
            PutRegistryScanningConfigurationRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutRegistryScanningConfigurationAsync(request);
            return new EcrPutRegistryScanningConfigurationResult(
                ScanningConfiguration: resp.RegistryScanningConfiguration);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put registry scanning configuration");
        }
    }

    /// <summary>
    /// Get registry-level scanning configuration.
    /// </summary>
    public static async Task<EcrGetRegistryScanningConfigurationResult>
        GetRegistryScanningConfigurationAsync(
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetRegistryScanningConfigurationAsync(
                new GetRegistryScanningConfigurationRequest());
            return new EcrGetRegistryScanningConfigurationResult(
                ScanningConfiguration: resp.ScanningConfiguration);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get registry scanning configuration");
        }
    }

    /// <summary>
    /// Batch get repository scanning configurations.
    /// </summary>
    public static async Task<EcrBatchGetRepositoryScanningConfigurationResult>
        BatchGetRepositoryScanningConfigurationAsync(
            List<string> repositoryNames, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetRepositoryScanningConfigurationAsync(
                new BatchGetRepositoryScanningConfigurationRequest
                {
                    RepositoryNames = repositoryNames
                });
            return new EcrBatchGetRepositoryScanningConfigurationResult(
                ScanningConfigurations: resp.ScanningConfigurations,
                Failures: resp.Failures);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch get repository scanning configuration");
        }
    }

    /// <summary>
    /// Create a pull-through cache rule.
    /// </summary>
    public static async Task<EcrCreatePullThroughCacheRuleResult>
        CreatePullThroughCacheRuleAsync(
            CreatePullThroughCacheRuleRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreatePullThroughCacheRuleAsync(request);
            return new EcrCreatePullThroughCacheRuleResult(
                EcrRepositoryPrefix: resp.EcrRepositoryPrefix,
                UpstreamRegistryUrl: resp.UpstreamRegistryUrl,
                RegistryId: resp.RegistryId);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create pull-through cache rule");
        }
    }

    /// <summary>
    /// Delete a pull-through cache rule.
    /// </summary>
    public static async Task<EcrDeletePullThroughCacheRuleResult>
        DeletePullThroughCacheRuleAsync(
            string ecrRepositoryPrefix, string? registryId = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeletePullThroughCacheRuleRequest
        {
            EcrRepositoryPrefix = ecrRepositoryPrefix
        };
        if (registryId != null) request.RegistryId = registryId;

        try
        {
            var resp = await client.DeletePullThroughCacheRuleAsync(request);
            return new EcrDeletePullThroughCacheRuleResult(
                EcrRepositoryPrefix: resp.EcrRepositoryPrefix,
                UpstreamRegistryUrl: resp.UpstreamRegistryUrl,
                RegistryId: resp.RegistryId);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete pull-through cache rule");
        }
    }

    /// <summary>
    /// Describe pull-through cache rules.
    /// </summary>
    public static async Task<EcrDescribePullThroughCacheRulesResult>
        DescribePullThroughCacheRulesAsync(
            DescribePullThroughCacheRulesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribePullThroughCacheRulesAsync(request);
            return new EcrDescribePullThroughCacheRulesResult(
                PullThroughCacheRules: resp.PullThroughCacheRules,
                NextToken: resp.NextToken);
        }
        catch (AmazonECRException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe pull-through cache rules");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateRepositoryAsync"/>.</summary>
    public static CreateRepositoryResult CreateRepository(CreateRepositoryRequest request, RegionEndpoint? region = null)
        => CreateRepositoryAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteRepositoryAsync"/>.</summary>
    public static DeleteRepositoryResult DeleteRepository(string repositoryName, string? registryId = null, bool force = false, RegionEndpoint? region = null)
        => DeleteRepositoryAsync(repositoryName, registryId, force, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeRepositoriesAsync"/>.</summary>
    public static DescribeRepositoriesResult DescribeRepositories(DescribeRepositoriesRequest request, RegionEndpoint? region = null)
        => DescribeRepositoriesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListImagesAsync"/>.</summary>
    public static EcrListImagesResult ListImages(ListImagesRequest request, RegionEndpoint? region = null)
        => ListImagesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeImagesAsync"/>.</summary>
    public static EcrDescribeImagesResult DescribeImages(DescribeImagesRequest request, RegionEndpoint? region = null)
        => DescribeImagesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchDeleteImageAsync"/>.</summary>
    public static EcrBatchDeleteImageResult BatchDeleteImage(BatchDeleteImageRequest request, RegionEndpoint? region = null)
        => BatchDeleteImageAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetImageAsync"/>.</summary>
    public static EcrBatchGetImageResult BatchGetImage(BatchGetImageRequest request, RegionEndpoint? region = null)
        => BatchGetImageAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAuthorizationTokenAsync"/>.</summary>
    public static EcrGetAuthorizationTokenResult GetAuthorizationToken(RegionEndpoint? region = null)
        => GetAuthorizationTokenAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutImageAsync"/>.</summary>
    public static EcrPutImageResult PutImage(PutImageRequest request, RegionEndpoint? region = null)
        => PutImageAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutImageTagMutabilityAsync"/>.</summary>
    public static EcrPutImageTagMutabilityResult PutImageTagMutability(string repositoryName, string imageTagMutability, string? registryId = null, RegionEndpoint? region = null)
        => PutImageTagMutabilityAsync(repositoryName, imageTagMutability, registryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutImageScanningConfigurationAsync"/>.</summary>
    public static EcrPutImageScanningConfigurationResult PutImageScanningConfiguration(string repositoryName, ImageScanningConfiguration imageScanningConfiguration, string? registryId = null, RegionEndpoint? region = null)
        => PutImageScanningConfigurationAsync(repositoryName, imageScanningConfiguration, registryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SetRepositoryPolicyAsync"/>.</summary>
    public static EcrSetRepositoryPolicyResult SetRepositoryPolicy(string repositoryName, string policyText, bool? force = null, string? registryId = null, RegionEndpoint? region = null)
        => SetRepositoryPolicyAsync(repositoryName, policyText, force, registryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetRepositoryPolicyAsync"/>.</summary>
    public static EcrGetRepositoryPolicyResult GetRepositoryPolicy(string repositoryName, string? registryId = null, RegionEndpoint? region = null)
        => GetRepositoryPolicyAsync(repositoryName, registryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteRepositoryPolicyAsync"/>.</summary>
    public static EcrDeleteRepositoryPolicyResult DeleteRepositoryPolicy(string repositoryName, string? registryId = null, RegionEndpoint? region = null)
        => DeleteRepositoryPolicyAsync(repositoryName, registryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartImageScanAsync"/>.</summary>
    public static EcrStartImageScanResult StartImageScan(string repositoryName, ImageIdentifier imageId, string? registryId = null, RegionEndpoint? region = null)
        => StartImageScanAsync(repositoryName, imageId, registryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeImageScanFindingsAsync"/>.</summary>
    public static EcrDescribeImageScanFindingsResult DescribeImageScanFindings(DescribeImageScanFindingsRequest request, RegionEndpoint? region = null)
        => DescribeImageScanFindingsAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetLifecyclePolicyAsync"/>.</summary>
    public static EcrGetLifecyclePolicyResult GetLifecyclePolicy(string repositoryName, string? registryId = null, RegionEndpoint? region = null)
        => GetLifecyclePolicyAsync(repositoryName, registryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutLifecyclePolicyAsync"/>.</summary>
    public static EcrPutLifecyclePolicyResult PutLifecyclePolicy(string repositoryName, string lifecyclePolicyText, string? registryId = null, RegionEndpoint? region = null)
        => PutLifecyclePolicyAsync(repositoryName, lifecyclePolicyText, registryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteLifecyclePolicyAsync"/>.</summary>
    public static EcrDeleteLifecyclePolicyResult DeleteLifecyclePolicy(string repositoryName, string? registryId = null, RegionEndpoint? region = null)
        => DeleteLifecyclePolicyAsync(repositoryName, registryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetLifecyclePolicyPreviewAsync"/>.</summary>
    public static EcrGetLifecyclePolicyPreviewResult GetLifecyclePolicyPreview(GetLifecyclePolicyPreviewRequest request, RegionEndpoint? region = null)
        => GetLifecyclePolicyPreviewAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartLifecyclePolicyPreviewAsync"/>.</summary>
    public static EcrStartLifecyclePolicyPreviewResult StartLifecyclePolicyPreview(StartLifecyclePolicyPreviewRequest request, RegionEndpoint? region = null)
        => StartLifecyclePolicyPreviewAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static EcrTagResourceResult TagResource(string resourceArn, List<Amazon.ECR.Model.Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static EcrUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static EcrListTagsForResourceResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutRegistryScanningConfigurationAsync"/>.</summary>
    public static EcrPutRegistryScanningConfigurationResult PutRegistryScanningConfiguration(PutRegistryScanningConfigurationRequest request, RegionEndpoint? region = null)
        => PutRegistryScanningConfigurationAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetRegistryScanningConfigurationAsync"/>.</summary>
    public static EcrGetRegistryScanningConfigurationResult GetRegistryScanningConfiguration(RegionEndpoint? region = null)
        => GetRegistryScanningConfigurationAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetRepositoryScanningConfigurationAsync"/>.</summary>
    public static EcrBatchGetRepositoryScanningConfigurationResult BatchGetRepositoryScanningConfiguration(List<string> repositoryNames, RegionEndpoint? region = null)
        => BatchGetRepositoryScanningConfigurationAsync(repositoryNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreatePullThroughCacheRuleAsync"/>.</summary>
    public static EcrCreatePullThroughCacheRuleResult CreatePullThroughCacheRule(CreatePullThroughCacheRuleRequest request, RegionEndpoint? region = null)
        => CreatePullThroughCacheRuleAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeletePullThroughCacheRuleAsync"/>.</summary>
    public static EcrDeletePullThroughCacheRuleResult DeletePullThroughCacheRule(string ecrRepositoryPrefix, string? registryId = null, RegionEndpoint? region = null)
        => DeletePullThroughCacheRuleAsync(ecrRepositoryPrefix, registryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribePullThroughCacheRulesAsync"/>.</summary>
    public static EcrDescribePullThroughCacheRulesResult DescribePullThroughCacheRules(DescribePullThroughCacheRulesRequest request, RegionEndpoint? region = null)
        => DescribePullThroughCacheRulesAsync(request, region).GetAwaiter().GetResult();

}

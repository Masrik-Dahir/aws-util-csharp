using Amazon;
using Amazon.CodeCommit;
using Amazon.CodeCommit.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record CcCreateRepositoryResult(
    string? RepositoryId = null,
    string? RepositoryName = null,
    string? Arn = null,
    string? CloneUrlHttp = null,
    string? CloneUrlSsh = null);

public sealed record CcDeleteRepositoryResult(
    string? RepositoryId = null);

public sealed record CcGetRepositoryResult(
    RepositoryMetadata? RepositoryMetadata = null);

public sealed record CcListRepositoriesResult(
    List<RepositoryNameIdPair>? Repositories = null,
    string? NextToken = null);

public sealed record CcUpdateRepositoryNameResult(bool Success = true);

public sealed record CcUpdateRepositoryDescriptionResult(bool Success = true);

public sealed record CcGetBranchResult(
    BranchInfo? Branch = null);

public sealed record CcCreateBranchResult(bool Success = true);

public sealed record CcDeleteBranchResult(
    BranchInfo? DeletedBranch = null);

public sealed record CcListBranchesResult(
    List<string>? Branches = null,
    string? NextToken = null);

public sealed record CcCreateCommitResult(
    string? CommitId = null,
    string? TreeId = null);

public sealed record CcGetCommitResult(
    Commit? Commit = null);

public sealed record CcGetDifferencesResult(
    List<Difference>? Differences = null,
    string? NextToken = null);

public sealed record CcMergeBranchesByFastForwardResult(
    string? CommitId = null,
    string? TreeId = null);

public sealed record CcMergeBranchesBySquashResult(
    string? CommitId = null,
    string? TreeId = null);

public sealed record CcMergeBranchesByThreeWayResult(
    string? CommitId = null,
    string? TreeId = null);

public sealed record CcCreatePullRequestResult(
    PullRequest? PullRequest = null);

public sealed record CcGetPullRequestResult(
    PullRequest? PullRequest = null);

public sealed record CcListPullRequestsResult(
    List<string>? PullRequestIds = null,
    string? NextToken = null);

public sealed record CcUpdatePullRequestStatusResult(
    PullRequest? PullRequest = null);

public sealed record CcMergePullRequestByFastForwardResult(
    PullRequest? PullRequest = null);

public sealed record CcMergePullRequestBySquashResult(
    PullRequest? PullRequest = null);

public sealed record CcMergePullRequestByThreeWayResult(
    PullRequest? PullRequest = null);

public sealed record CcCreatePullRequestApprovalRuleResult(
    ApprovalRule? ApprovalRule = null);

public sealed record CcPutFileResult(
    string? CommitId = null,
    string? BlobId = null,
    string? TreeId = null);

public sealed record CcGetFileResult(
    string? CommitId = null,
    string? BlobId = null,
    string? FilePath = null,
    long? FileSize = null,
    byte[]? FileContent = null);

public sealed record CcDeleteFileResult(
    string? CommitId = null,
    string? BlobId = null,
    string? TreeId = null,
    string? FilePath = null);

public sealed record CcGetFolderResult(
    string? CommitId = null,
    string? FolderPath = null,
    List<Folder>? SubFolders = null,
    List<Amazon.CodeCommit.Model.File>? Files = null,
    List<SubModule>? SubModules = null,
    List<SymbolicLink>? SymbolicLinks = null);

public sealed record CcTagResourceResult(bool Success = true);
public sealed record CcUntagResourceResult(bool Success = true);

public sealed record CcListTagsForResourceResult(
    Dictionary<string, string>? Tags = null,
    string? NextToken = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS CodeCommit.
/// </summary>
public static class CodeCommitService
{
    private static AmazonCodeCommitClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCodeCommitClient>(region);

    /// <summary>
    /// Create a new CodeCommit repository.
    /// </summary>
    public static async Task<CcCreateRepositoryResult> CreateRepositoryAsync(
        string repositoryName,
        string? repositoryDescription = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateRepositoryRequest
        {
            RepositoryName = repositoryName
        };
        if (repositoryDescription != null)
            request.RepositoryDescription = repositoryDescription;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateRepositoryAsync(request);
            var meta = resp.RepositoryMetadata;
            return new CcCreateRepositoryResult(
                RepositoryId: meta.RepositoryId,
                RepositoryName: meta.RepositoryName,
                Arn: meta.Arn,
                CloneUrlHttp: meta.CloneUrlHttp,
                CloneUrlSsh: meta.CloneUrlSsh);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create CodeCommit repository");
        }
    }

    /// <summary>
    /// Delete a CodeCommit repository.
    /// </summary>
    public static async Task<CcDeleteRepositoryResult> DeleteRepositoryAsync(
        string repositoryName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteRepositoryAsync(
                new DeleteRepositoryRequest
                {
                    RepositoryName = repositoryName
                });
            return new CcDeleteRepositoryResult(
                RepositoryId: resp.RepositoryId);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete CodeCommit repository '{repositoryName}'");
        }
    }

    /// <summary>
    /// Get information about a CodeCommit repository.
    /// </summary>
    public static async Task<CcGetRepositoryResult> GetRepositoryAsync(
        string repositoryName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetRepositoryAsync(
                new GetRepositoryRequest
                {
                    RepositoryName = repositoryName
                });
            return new CcGetRepositoryResult(
                RepositoryMetadata: resp.RepositoryMetadata);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get CodeCommit repository '{repositoryName}'");
        }
    }

    /// <summary>
    /// List CodeCommit repositories.
    /// </summary>
    public static async Task<CcListRepositoriesResult> ListRepositoriesAsync(
        string? sortBy = null,
        string? order = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRepositoriesRequest();
        if (sortBy != null) request.SortBy = new SortByEnum(sortBy);
        if (order != null) request.Order = new OrderEnum(order);
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListRepositoriesAsync(request);
            return new CcListRepositoriesResult(
                Repositories: resp.Repositories,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list CodeCommit repositories");
        }
    }

    /// <summary>
    /// Rename a CodeCommit repository.
    /// </summary>
    public static async Task<CcUpdateRepositoryNameResult>
        UpdateRepositoryNameAsync(
            string oldName,
            string newName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateRepositoryNameAsync(
                new UpdateRepositoryNameRequest
                {
                    OldName = oldName,
                    NewName = newName
                });
            return new CcUpdateRepositoryNameResult();
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to rename CodeCommit repository '{oldName}'");
        }
    }

    /// <summary>
    /// Update the description of a CodeCommit repository.
    /// </summary>
    public static async Task<CcUpdateRepositoryDescriptionResult>
        UpdateRepositoryDescriptionAsync(
            string repositoryName,
            string repositoryDescription,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateRepositoryDescriptionAsync(
                new UpdateRepositoryDescriptionRequest
                {
                    RepositoryName = repositoryName,
                    RepositoryDescription = repositoryDescription
                });
            return new CcUpdateRepositoryDescriptionResult();
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update description for '{repositoryName}'");
        }
    }

    /// <summary>
    /// Get information about a branch.
    /// </summary>
    public static async Task<CcGetBranchResult> GetBranchAsync(
        string repositoryName,
        string branchName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetBranchAsync(new GetBranchRequest
            {
                RepositoryName = repositoryName,
                BranchName = branchName
            });
            return new CcGetBranchResult(Branch: resp.Branch);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get branch '{branchName}'");
        }
    }

    /// <summary>
    /// Create a branch in a CodeCommit repository.
    /// </summary>
    public static async Task<CcCreateBranchResult> CreateBranchAsync(
        string repositoryName,
        string branchName,
        string commitId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateBranchAsync(new CreateBranchRequest
            {
                RepositoryName = repositoryName,
                BranchName = branchName,
                CommitId = commitId
            });
            return new CcCreateBranchResult();
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create branch '{branchName}'");
        }
    }

    /// <summary>
    /// Delete a branch from a CodeCommit repository.
    /// </summary>
    public static async Task<CcDeleteBranchResult> DeleteBranchAsync(
        string repositoryName,
        string branchName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteBranchAsync(new DeleteBranchRequest
            {
                RepositoryName = repositoryName,
                BranchName = branchName
            });
            return new CcDeleteBranchResult(DeletedBranch: resp.DeletedBranch);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete branch '{branchName}'");
        }
    }

    /// <summary>
    /// List branches in a CodeCommit repository.
    /// </summary>
    public static async Task<CcListBranchesResult> ListBranchesAsync(
        string repositoryName,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListBranchesRequest
        {
            RepositoryName = repositoryName
        };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListBranchesAsync(request);
            return new CcListBranchesResult(
                Branches: resp.Branches,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list branches");
        }
    }

    /// <summary>
    /// Create a commit in a CodeCommit repository.
    /// </summary>
    public static async Task<CcCreateCommitResult> CreateCommitAsync(
        CreateCommitRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateCommitAsync(request);
            return new CcCreateCommitResult(
                CommitId: resp.CommitId,
                TreeId: resp.TreeId);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create commit");
        }
    }

    /// <summary>
    /// Get information about a commit.
    /// </summary>
    public static async Task<CcGetCommitResult> GetCommitAsync(
        string repositoryName,
        string commitId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetCommitAsync(new GetCommitRequest
            {
                RepositoryName = repositoryName,
                CommitId = commitId
            });
            return new CcGetCommitResult(Commit: resp.Commit);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get commit '{commitId}'");
        }
    }

    /// <summary>
    /// Get differences between two commit specifiers.
    /// </summary>
    public static async Task<CcGetDifferencesResult> GetDifferencesAsync(
        GetDifferencesRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDifferencesAsync(request);
            return new CcGetDifferencesResult(
                Differences: resp.Differences,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get differences");
        }
    }

    /// <summary>
    /// Merge branches using fast-forward strategy.
    /// </summary>
    public static async Task<CcMergeBranchesByFastForwardResult>
        MergeBranchesByFastForwardAsync(
            MergeBranchesByFastForwardRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.MergeBranchesByFastForwardAsync(request);
            return new CcMergeBranchesByFastForwardResult(
                CommitId: resp.CommitId,
                TreeId: resp.TreeId);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to merge branches by fast-forward");
        }
    }

    /// <summary>
    /// Merge branches using squash strategy.
    /// </summary>
    public static async Task<CcMergeBranchesBySquashResult>
        MergeBranchesBySquashAsync(
            MergeBranchesBySquashRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.MergeBranchesBySquashAsync(request);
            return new CcMergeBranchesBySquashResult(
                CommitId: resp.CommitId,
                TreeId: resp.TreeId);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to merge branches by squash");
        }
    }

    /// <summary>
    /// Merge branches using three-way strategy.
    /// </summary>
    public static async Task<CcMergeBranchesByThreeWayResult>
        MergeBranchesByThreeWayAsync(
            MergeBranchesByThreeWayRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.MergeBranchesByThreeWayAsync(request);
            return new CcMergeBranchesByThreeWayResult(
                CommitId: resp.CommitId,
                TreeId: resp.TreeId);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to merge branches by three-way");
        }
    }

    /// <summary>
    /// Create a pull request.
    /// </summary>
    public static async Task<CcCreatePullRequestResult>
        CreatePullRequestAsync(
            CreatePullRequestRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreatePullRequestAsync(request);
            return new CcCreatePullRequestResult(
                PullRequest: resp.PullRequest);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create pull request");
        }
    }

    /// <summary>
    /// Get information about a pull request.
    /// </summary>
    public static async Task<CcGetPullRequestResult> GetPullRequestAsync(
        string pullRequestId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetPullRequestAsync(
                new GetPullRequestRequest
                {
                    PullRequestId = pullRequestId
                });
            return new CcGetPullRequestResult(
                PullRequest: resp.PullRequest);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get pull request '{pullRequestId}'");
        }
    }

    /// <summary>
    /// List pull requests for a repository.
    /// </summary>
    public static async Task<CcListPullRequestsResult>
        ListPullRequestsAsync(
            string repositoryName,
            string? pullRequestStatus = null,
            string? authorArn = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListPullRequestsRequest
        {
            RepositoryName = repositoryName
        };
        if (pullRequestStatus != null)
            request.PullRequestStatus =
                new PullRequestStatusEnum(pullRequestStatus);
        if (authorArn != null) request.AuthorArn = authorArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListPullRequestsAsync(request);
            return new CcListPullRequestsResult(
                PullRequestIds: resp.PullRequestIds,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list pull requests");
        }
    }

    /// <summary>
    /// Update the status of a pull request.
    /// </summary>
    public static async Task<CcUpdatePullRequestStatusResult>
        UpdatePullRequestStatusAsync(
            string pullRequestId,
            string pullRequestStatus,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdatePullRequestStatusAsync(
                new UpdatePullRequestStatusRequest
                {
                    PullRequestId = pullRequestId,
                    PullRequestStatus =
                        new PullRequestStatusEnum(pullRequestStatus)
                });
            return new CcUpdatePullRequestStatusResult(
                PullRequest: resp.PullRequest);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update pull request status '{pullRequestId}'");
        }
    }

    /// <summary>
    /// Merge a pull request using fast-forward strategy.
    /// </summary>
    public static async Task<CcMergePullRequestByFastForwardResult>
        MergePullRequestByFastForwardAsync(
            MergePullRequestByFastForwardRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.MergePullRequestByFastForwardAsync(request);
            return new CcMergePullRequestByFastForwardResult(
                PullRequest: resp.PullRequest);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to merge pull request by fast-forward");
        }
    }

    /// <summary>
    /// Merge a pull request using squash strategy.
    /// </summary>
    public static async Task<CcMergePullRequestBySquashResult>
        MergePullRequestBySquashAsync(
            MergePullRequestBySquashRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.MergePullRequestBySquashAsync(request);
            return new CcMergePullRequestBySquashResult(
                PullRequest: resp.PullRequest);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to merge pull request by squash");
        }
    }

    /// <summary>
    /// Merge a pull request using three-way strategy.
    /// </summary>
    public static async Task<CcMergePullRequestByThreeWayResult>
        MergePullRequestByThreeWayAsync(
            MergePullRequestByThreeWayRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.MergePullRequestByThreeWayAsync(request);
            return new CcMergePullRequestByThreeWayResult(
                PullRequest: resp.PullRequest);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to merge pull request by three-way");
        }
    }

    /// <summary>
    /// Create an approval rule for a pull request.
    /// </summary>
    public static async Task<CcCreatePullRequestApprovalRuleResult>
        CreatePullRequestApprovalRuleAsync(
            CreatePullRequestApprovalRuleRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp =
                await client.CreatePullRequestApprovalRuleAsync(request);
            return new CcCreatePullRequestApprovalRuleResult(
                ApprovalRule: resp.ApprovalRule);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create pull request approval rule");
        }
    }

    /// <summary>
    /// Put a file into a CodeCommit repository.
    /// </summary>
    public static async Task<CcPutFileResult> PutFileAsync(
        PutFileRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutFileAsync(request);
            return new CcPutFileResult(
                CommitId: resp.CommitId,
                BlobId: resp.BlobId,
                TreeId: resp.TreeId);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to put file");
        }
    }

    /// <summary>
    /// Get a file from a CodeCommit repository.
    /// </summary>
    public static async Task<CcGetFileResult> GetFileAsync(
        string repositoryName,
        string filePath,
        string? commitSpecifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetFileRequest
        {
            RepositoryName = repositoryName,
            FilePath = filePath
        };
        if (commitSpecifier != null)
            request.CommitSpecifier = commitSpecifier;

        try
        {
            var resp = await client.GetFileAsync(request);
            return new CcGetFileResult(
                CommitId: resp.CommitId,
                BlobId: resp.BlobId,
                FilePath: resp.FilePath,
                FileSize: resp.FileSize,
                FileContent: resp.FileContent?.ToArray());
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get file '{filePath}'");
        }
    }

    /// <summary>
    /// Delete a file from a CodeCommit repository.
    /// </summary>
    public static async Task<CcDeleteFileResult> DeleteFileAsync(
        DeleteFileRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteFileAsync(request);
            return new CcDeleteFileResult(
                CommitId: resp.CommitId,
                BlobId: resp.BlobId,
                TreeId: resp.TreeId,
                FilePath: resp.FilePath);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to delete file");
        }
    }

    /// <summary>
    /// Get the contents of a folder in a CodeCommit repository.
    /// </summary>
    public static async Task<CcGetFolderResult> GetFolderAsync(
        string repositoryName,
        string folderPath,
        string? commitSpecifier = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetFolderRequest
        {
            RepositoryName = repositoryName,
            FolderPath = folderPath
        };
        if (commitSpecifier != null)
            request.CommitSpecifier = commitSpecifier;

        try
        {
            var resp = await client.GetFolderAsync(request);
            return new CcGetFolderResult(
                CommitId: resp.CommitId,
                FolderPath: resp.FolderPath,
                SubFolders: resp.SubFolders,
                Files: resp.Files,
                SubModules: resp.SubModules,
                SymbolicLinks: resp.SymbolicLinks);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get folder '{folderPath}'");
        }
    }

    /// <summary>
    /// Tag a CodeCommit resource.
    /// </summary>
    public static async Task<CcTagResourceResult> TagResourceAsync(
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
            return new CcTagResourceResult();
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to tag CodeCommit resource");
        }
    }

    /// <summary>
    /// Remove tags from a CodeCommit resource.
    /// </summary>
    public static async Task<CcUntagResourceResult> UntagResourceAsync(
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
            return new CcUntagResourceResult();
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to untag CodeCommit resource");
        }
    }

    /// <summary>
    /// List tags for a CodeCommit resource.
    /// </summary>
    public static async Task<CcListTagsForResourceResult>
        ListTagsForResourceAsync(
            string resourceArn,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForResourceRequest
        {
            ResourceArn = resourceArn
        };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListTagsForResourceAsync(request);
            return new CcListTagsForResourceResult(
                Tags: resp.Tags,
                NextToken: resp.NextToken);
        }
        catch (AmazonCodeCommitException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for CodeCommit resource");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateRepositoryAsync"/>.</summary>
    public static CcCreateRepositoryResult CreateRepository(string repositoryName, string? repositoryDescription = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateRepositoryAsync(repositoryName, repositoryDescription, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteRepositoryAsync"/>.</summary>
    public static CcDeleteRepositoryResult DeleteRepository(string repositoryName, RegionEndpoint? region = null)
        => DeleteRepositoryAsync(repositoryName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetRepositoryAsync"/>.</summary>
    public static CcGetRepositoryResult GetRepository(string repositoryName, RegionEndpoint? region = null)
        => GetRepositoryAsync(repositoryName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListRepositoriesAsync"/>.</summary>
    public static CcListRepositoriesResult ListRepositories(string? sortBy = null, string? order = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListRepositoriesAsync(sortBy, order, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateRepositoryNameAsync"/>.</summary>
    public static CcUpdateRepositoryNameResult UpdateRepositoryName(string oldName, string newName, RegionEndpoint? region = null)
        => UpdateRepositoryNameAsync(oldName, newName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateRepositoryDescriptionAsync"/>.</summary>
    public static CcUpdateRepositoryDescriptionResult UpdateRepositoryDescription(string repositoryName, string repositoryDescription, RegionEndpoint? region = null)
        => UpdateRepositoryDescriptionAsync(repositoryName, repositoryDescription, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBranchAsync"/>.</summary>
    public static CcGetBranchResult GetBranch(string repositoryName, string branchName, RegionEndpoint? region = null)
        => GetBranchAsync(repositoryName, branchName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateBranchAsync"/>.</summary>
    public static CcCreateBranchResult CreateBranch(string repositoryName, string branchName, string commitId, RegionEndpoint? region = null)
        => CreateBranchAsync(repositoryName, branchName, commitId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteBranchAsync"/>.</summary>
    public static CcDeleteBranchResult DeleteBranch(string repositoryName, string branchName, RegionEndpoint? region = null)
        => DeleteBranchAsync(repositoryName, branchName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListBranchesAsync"/>.</summary>
    public static CcListBranchesResult ListBranches(string repositoryName, string? nextToken = null, RegionEndpoint? region = null)
        => ListBranchesAsync(repositoryName, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateCommitAsync"/>.</summary>
    public static CcCreateCommitResult CreateCommit(CreateCommitRequest request, RegionEndpoint? region = null)
        => CreateCommitAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetCommitAsync"/>.</summary>
    public static CcGetCommitResult GetCommit(string repositoryName, string commitId, RegionEndpoint? region = null)
        => GetCommitAsync(repositoryName, commitId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDifferencesAsync"/>.</summary>
    public static CcGetDifferencesResult GetDifferences(GetDifferencesRequest request, RegionEndpoint? region = null)
        => GetDifferencesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="MergeBranchesByFastForwardAsync"/>.</summary>
    public static CcMergeBranchesByFastForwardResult MergeBranchesByFastForward(MergeBranchesByFastForwardRequest request, RegionEndpoint? region = null)
        => MergeBranchesByFastForwardAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="MergeBranchesBySquashAsync"/>.</summary>
    public static CcMergeBranchesBySquashResult MergeBranchesBySquash(MergeBranchesBySquashRequest request, RegionEndpoint? region = null)
        => MergeBranchesBySquashAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="MergeBranchesByThreeWayAsync"/>.</summary>
    public static CcMergeBranchesByThreeWayResult MergeBranchesByThreeWay(MergeBranchesByThreeWayRequest request, RegionEndpoint? region = null)
        => MergeBranchesByThreeWayAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreatePullRequestAsync"/>.</summary>
    public static CcCreatePullRequestResult CreatePullRequest(CreatePullRequestRequest request, RegionEndpoint? region = null)
        => CreatePullRequestAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetPullRequestAsync"/>.</summary>
    public static CcGetPullRequestResult GetPullRequest(string pullRequestId, RegionEndpoint? region = null)
        => GetPullRequestAsync(pullRequestId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListPullRequestsAsync"/>.</summary>
    public static CcListPullRequestsResult ListPullRequests(string repositoryName, string? pullRequestStatus = null, string? authorArn = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListPullRequestsAsync(repositoryName, pullRequestStatus, authorArn, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdatePullRequestStatusAsync"/>.</summary>
    public static CcUpdatePullRequestStatusResult UpdatePullRequestStatus(string pullRequestId, string pullRequestStatus, RegionEndpoint? region = null)
        => UpdatePullRequestStatusAsync(pullRequestId, pullRequestStatus, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="MergePullRequestByFastForwardAsync"/>.</summary>
    public static CcMergePullRequestByFastForwardResult MergePullRequestByFastForward(MergePullRequestByFastForwardRequest request, RegionEndpoint? region = null)
        => MergePullRequestByFastForwardAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="MergePullRequestBySquashAsync"/>.</summary>
    public static CcMergePullRequestBySquashResult MergePullRequestBySquash(MergePullRequestBySquashRequest request, RegionEndpoint? region = null)
        => MergePullRequestBySquashAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="MergePullRequestByThreeWayAsync"/>.</summary>
    public static CcMergePullRequestByThreeWayResult MergePullRequestByThreeWay(MergePullRequestByThreeWayRequest request, RegionEndpoint? region = null)
        => MergePullRequestByThreeWayAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreatePullRequestApprovalRuleAsync"/>.</summary>
    public static CcCreatePullRequestApprovalRuleResult CreatePullRequestApprovalRule(CreatePullRequestApprovalRuleRequest request, RegionEndpoint? region = null)
        => CreatePullRequestApprovalRuleAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutFileAsync"/>.</summary>
    public static CcPutFileResult PutFile(PutFileRequest request, RegionEndpoint? region = null)
        => PutFileAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetFileAsync"/>.</summary>
    public static CcGetFileResult GetFile(string repositoryName, string filePath, string? commitSpecifier = null, RegionEndpoint? region = null)
        => GetFileAsync(repositoryName, filePath, commitSpecifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteFileAsync"/>.</summary>
    public static CcDeleteFileResult DeleteFile(DeleteFileRequest request, RegionEndpoint? region = null)
        => DeleteFileAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetFolderAsync"/>.</summary>
    public static CcGetFolderResult GetFolder(string repositoryName, string folderPath, string? commitSpecifier = null, RegionEndpoint? region = null)
        => GetFolderAsync(repositoryName, folderPath, commitSpecifier, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static CcTagResourceResult TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static CcUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static CcListTagsForResourceResult ListTagsForResource(string resourceArn, string? nextToken = null, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, nextToken, region).GetAwaiter().GetResult();

}

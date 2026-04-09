using Amazon;
using Amazon.Transfer;
using Amazon.Transfer.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record TransferServerInfo(
    string? ServerId = null,
    string? Arn = null,
    string? State = null,
    string? Domain = null,
    string? EndpointType = null,
    string? IdentityProviderType = null,
    int? UserCount = null);

public sealed record TransferUserInfo(
    string? ServerId = null,
    string? UserName = null,
    string? Arn = null,
    string? HomeDirectory = null,
    string? HomeDirectoryType = null,
    string? Role = null,
    int? SshPublicKeyCount = null);

public sealed record TransferSshPublicKeyInfo(
    string? ServerId = null,
    string? SshPublicKeyId = null,
    string? UserName = null);

public sealed record TransferWorkflowInfo(
    string? WorkflowId = null,
    string? Arn = null,
    string? Description = null);

public sealed record TransferAgreementInfo(
    string? AgreementId = null,
    string? Arn = null,
    string? ServerId = null,
    string? Status = null,
    string? Description = null,
    string? LocalProfileId = null,
    string? PartnerProfileId = null);

public sealed record TransferConnectorInfo(
    string? ConnectorId = null,
    string? Arn = null,
    string? Url = null);

public sealed record TransferProfileInfo(
    string? ProfileId = null,
    string? Arn = null,
    string? ProfileType = null,
    string? As2Id = null);

public sealed record TransferAccessInfo(
    string? ServerId = null,
    string? ExternalId = null,
    string? HomeDirectory = null,
    string? HomeDirectoryType = null,
    string? Role = null);

public sealed record TransferWorkflowStepResult(bool Success = true);

public sealed record TransferTestIdentityProviderResult(
    string? StatusCode = null,
    string? Message = null,
    string? Response = null,
    string? Url = null);

public sealed record TransferTagResult(bool Success = true);

public sealed record TransferListTagsResult(
    string? Arn = null,
    List<Tag>? Tags = null,
    string? NextToken = null);

/// <summary>
/// Utility helpers for AWS Transfer Family.
/// </summary>
public static class TransferService
{
    private static AmazonTransferClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonTransferClient>(region);

    // ── Server operations ────────────────────────────────────────────

    /// <summary>
    /// Create a Transfer server.
    /// </summary>
    public static async Task<string?> CreateServerAsync(
        CreateServerRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateServerAsync(request);
            return resp.ServerId;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Transfer server");
        }
    }

    /// <summary>
    /// Delete a Transfer server.
    /// </summary>
    public static async Task DeleteServerAsync(
        string serverId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteServerAsync(new DeleteServerRequest
            {
                ServerId = serverId
            });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Transfer server '{serverId}'");
        }
    }

    /// <summary>
    /// Describe a Transfer server.
    /// </summary>
    public static async Task<TransferServerInfo> DescribeServerAsync(
        string serverId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeServerAsync(
                new DescribeServerRequest { ServerId = serverId });
            var s = resp.Server;
            return new TransferServerInfo(
                ServerId: s.ServerId,
                Arn: s.Arn,
                State: s.State?.Value,
                Domain: s.Domain?.Value,
                EndpointType: s.EndpointType?.Value,
                IdentityProviderType: s.IdentityProviderType?.Value,
                UserCount: s.UserCount);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Transfer server '{serverId}'");
        }
    }

    /// <summary>
    /// List Transfer servers.
    /// </summary>
    public static async Task<List<TransferServerInfo>> ListServersAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListServersRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListServersAsync(request);
            return resp.Servers.Select(s => new TransferServerInfo(
                ServerId: s.ServerId,
                Arn: s.Arn,
                State: s.State?.Value,
                Domain: s.Domain?.Value,
                EndpointType: s.EndpointType?.Value,
                IdentityProviderType: s.IdentityProviderType?.Value,
                UserCount: s.UserCount)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Transfer servers");
        }
    }

    /// <summary>
    /// Update a Transfer server.
    /// </summary>
    public static async Task<string?> UpdateServerAsync(
        UpdateServerRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateServerAsync(request);
            return resp.ServerId;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update Transfer server");
        }
    }

    /// <summary>
    /// Start a Transfer server.
    /// </summary>
    public static async Task StartServerAsync(
        string serverId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StartServerAsync(new StartServerRequest
            {
                ServerId = serverId
            });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start Transfer server '{serverId}'");
        }
    }

    /// <summary>
    /// Stop a Transfer server.
    /// </summary>
    public static async Task StopServerAsync(
        string serverId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StopServerAsync(new StopServerRequest
            {
                ServerId = serverId
            });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop Transfer server '{serverId}'");
        }
    }

    // ── User operations ──────────────────────────────────────────────

    /// <summary>
    /// Create a user on a Transfer server.
    /// </summary>
    public static async Task<TransferUserInfo> CreateUserAsync(
        CreateUserRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateUserAsync(request);
            return new TransferUserInfo(
                ServerId: resp.ServerId,
                UserName: resp.UserName);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create Transfer user");
        }
    }

    /// <summary>
    /// Delete a user from a Transfer server.
    /// </summary>
    public static async Task DeleteUserAsync(
        string serverId,
        string userName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteUserAsync(new DeleteUserRequest
            {
                ServerId = serverId,
                UserName = userName
            });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete user '{userName}' from server '{serverId}'");
        }
    }

    /// <summary>
    /// Describe a user on a Transfer server.
    /// </summary>
    public static async Task<TransferUserInfo> DescribeUserAsync(
        string serverId,
        string userName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeUserAsync(new DescribeUserRequest
            {
                ServerId = serverId,
                UserName = userName
            });
            var u = resp.User;
            return new TransferUserInfo(
                ServerId: resp.ServerId,
                UserName: u.UserName,
                Arn: u.Arn,
                HomeDirectory: u.HomeDirectory,
                HomeDirectoryType: u.HomeDirectoryType?.Value,
                Role: u.Role,
                SshPublicKeyCount: u.SshPublicKeys?.Count);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe user '{userName}' on server '{serverId}'");
        }
    }

    /// <summary>
    /// List users on a Transfer server.
    /// </summary>
    public static async Task<List<TransferUserInfo>> ListUsersAsync(
        string serverId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListUsersRequest { ServerId = serverId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListUsersAsync(request);
            return resp.Users.Select(u => new TransferUserInfo(
                ServerId: resp.ServerId,
                UserName: u.UserName,
                Arn: u.Arn,
                HomeDirectory: u.HomeDirectory,
                HomeDirectoryType: u.HomeDirectoryType?.Value,
                Role: u.Role,
                SshPublicKeyCount: u.SshPublicKeyCount)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list users on server '{serverId}'");
        }
    }

    /// <summary>
    /// Update a user on a Transfer server.
    /// </summary>
    public static async Task<TransferUserInfo> UpdateUserAsync(
        UpdateUserRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateUserAsync(request);
            return new TransferUserInfo(
                ServerId: resp.ServerId,
                UserName: resp.UserName);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update Transfer user");
        }
    }

    // ── SSH Public Key operations ────────────────────────────────────

    /// <summary>
    /// Import an SSH public key for a user.
    /// </summary>
    public static async Task<TransferSshPublicKeyInfo>
        ImportSshPublicKeyAsync(
            string serverId,
            string userName,
            string sshPublicKeyBody,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ImportSshPublicKeyAsync(
                new ImportSshPublicKeyRequest
                {
                    ServerId = serverId,
                    UserName = userName,
                    SshPublicKeyBody = sshPublicKeyBody
                });
            return new TransferSshPublicKeyInfo(
                ServerId: resp.ServerId,
                SshPublicKeyId: resp.SshPublicKeyId,
                UserName: resp.UserName);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to import SSH public key for user '{userName}'");
        }
    }

    /// <summary>
    /// Delete an SSH public key for a user.
    /// </summary>
    public static async Task DeleteSshPublicKeyAsync(
        string serverId,
        string userName,
        string sshPublicKeyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteSshPublicKeyAsync(
                new DeleteSshPublicKeyRequest
                {
                    ServerId = serverId,
                    UserName = userName,
                    SshPublicKeyId = sshPublicKeyId
                });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete SSH public key '{sshPublicKeyId}' for user '{userName}'");
        }
    }

    // ── Workflow operations ──────────────────────────────────────────

    /// <summary>
    /// Create a workflow.
    /// </summary>
    public static async Task<string?> CreateWorkflowAsync(
        CreateWorkflowRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateWorkflowAsync(request);
            return resp.WorkflowId;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create workflow");
        }
    }

    /// <summary>
    /// Delete a workflow.
    /// </summary>
    public static async Task DeleteWorkflowAsync(
        string workflowId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteWorkflowAsync(new DeleteWorkflowRequest
            {
                WorkflowId = workflowId
            });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete workflow '{workflowId}'");
        }
    }

    /// <summary>
    /// Describe a workflow.
    /// </summary>
    public static async Task<TransferWorkflowInfo> DescribeWorkflowAsync(
        string workflowId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeWorkflowAsync(
                new DescribeWorkflowRequest { WorkflowId = workflowId });
            var w = resp.Workflow;
            return new TransferWorkflowInfo(
                WorkflowId: w.WorkflowId,
                Arn: w.Arn,
                Description: w.Description);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe workflow '{workflowId}'");
        }
    }

    /// <summary>
    /// List workflows.
    /// </summary>
    public static async Task<List<TransferWorkflowInfo>> ListWorkflowsAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListWorkflowsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListWorkflowsAsync(request);
            return resp.Workflows.Select(w => new TransferWorkflowInfo(
                WorkflowId: w.WorkflowId,
                Arn: w.Arn,
                Description: w.Description)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list workflows");
        }
    }

    // ── Agreement operations ─────────────────────────────────────────

    /// <summary>
    /// Create an agreement.
    /// </summary>
    public static async Task<string?> CreateAgreementAsync(
        CreateAgreementRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateAgreementAsync(request);
            return resp.AgreementId;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create agreement");
        }
    }

    /// <summary>
    /// Delete an agreement.
    /// </summary>
    public static async Task DeleteAgreementAsync(
        string agreementId,
        string serverId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAgreementAsync(new DeleteAgreementRequest
            {
                AgreementId = agreementId,
                ServerId = serverId
            });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete agreement '{agreementId}'");
        }
    }

    /// <summary>
    /// Describe an agreement.
    /// </summary>
    public static async Task<TransferAgreementInfo> DescribeAgreementAsync(
        string agreementId,
        string serverId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAgreementAsync(
                new DescribeAgreementRequest
                {
                    AgreementId = agreementId,
                    ServerId = serverId
                });
            var a = resp.Agreement;
            return new TransferAgreementInfo(
                AgreementId: a.AgreementId,
                Arn: a.Arn,
                ServerId: a.ServerId,
                Status: a.Status?.Value,
                Description: a.Description,
                LocalProfileId: a.LocalProfileId,
                PartnerProfileId: a.PartnerProfileId);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe agreement '{agreementId}'");
        }
    }

    /// <summary>
    /// List agreements for a server.
    /// </summary>
    public static async Task<List<TransferAgreementInfo>>
        ListAgreementsAsync(
            string serverId,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAgreementsRequest { ServerId = serverId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListAgreementsAsync(request);
            return resp.Agreements.Select(a => new TransferAgreementInfo(
                AgreementId: a.AgreementId,
                Arn: a.Arn,
                ServerId: a.ServerId,
                Status: a.Status?.Value,
                Description: a.Description,
                LocalProfileId: a.LocalProfileId,
                PartnerProfileId: a.PartnerProfileId)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list agreements for server '{serverId}'");
        }
    }

    /// <summary>
    /// Update an agreement.
    /// </summary>
    public static async Task<string?> UpdateAgreementAsync(
        UpdateAgreementRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateAgreementAsync(request);
            return resp.AgreementId;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update agreement");
        }
    }

    // ── Connector operations ─────────────────────────────────────────

    /// <summary>
    /// Create a connector.
    /// </summary>
    public static async Task<string?> CreateConnectorAsync(
        CreateConnectorRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateConnectorAsync(request);
            return resp.ConnectorId;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create connector");
        }
    }

    /// <summary>
    /// Delete a connector.
    /// </summary>
    public static async Task DeleteConnectorAsync(
        string connectorId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteConnectorAsync(new DeleteConnectorRequest
            {
                ConnectorId = connectorId
            });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete connector '{connectorId}'");
        }
    }

    /// <summary>
    /// Describe a connector.
    /// </summary>
    public static async Task<TransferConnectorInfo> DescribeConnectorAsync(
        string connectorId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeConnectorAsync(
                new DescribeConnectorRequest { ConnectorId = connectorId });
            var c = resp.Connector;
            return new TransferConnectorInfo(
                ConnectorId: c.ConnectorId,
                Arn: c.Arn,
                Url: c.Url);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe connector '{connectorId}'");
        }
    }

    /// <summary>
    /// List connectors.
    /// </summary>
    public static async Task<List<TransferConnectorInfo>>
        ListConnectorsAsync(
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListConnectorsRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListConnectorsAsync(request);
            return resp.Connectors.Select(c => new TransferConnectorInfo(
                ConnectorId: c.ConnectorId,
                Arn: c.Arn,
                Url: c.Url)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list connectors");
        }
    }

    /// <summary>
    /// Update a connector.
    /// </summary>
    public static async Task<string?> UpdateConnectorAsync(
        UpdateConnectorRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateConnectorAsync(request);
            return resp.ConnectorId;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update connector");
        }
    }

    // ── Profile operations ───────────────────────────────────────────

    /// <summary>
    /// Create a profile.
    /// </summary>
    public static async Task<string?> CreateProfileAsync(
        CreateProfileRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateProfileAsync(request);
            return resp.ProfileId;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create profile");
        }
    }

    /// <summary>
    /// Delete a profile.
    /// </summary>
    public static async Task DeleteProfileAsync(
        string profileId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteProfileAsync(new DeleteProfileRequest
            {
                ProfileId = profileId
            });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete profile '{profileId}'");
        }
    }

    /// <summary>
    /// Describe a profile.
    /// </summary>
    public static async Task<TransferProfileInfo> DescribeProfileAsync(
        string profileId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeProfileAsync(
                new DescribeProfileRequest { ProfileId = profileId });
            var p = resp.Profile;
            return new TransferProfileInfo(
                ProfileId: p.ProfileId,
                Arn: p.Arn,
                ProfileType: p.ProfileType?.Value,
                As2Id: p.As2Id);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe profile '{profileId}'");
        }
    }

    /// <summary>
    /// List profiles.
    /// </summary>
    public static async Task<List<TransferProfileInfo>> ListProfilesAsync(
        string? profileType = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListProfilesRequest();
        if (profileType != null) request.ProfileType = profileType;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListProfilesAsync(request);
            return resp.Profiles.Select(p => new TransferProfileInfo(
                ProfileId: p.ProfileId,
                Arn: p.Arn,
                ProfileType: p.ProfileType?.Value,
                As2Id: p.As2Id)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list profiles");
        }
    }

    /// <summary>
    /// Update a profile.
    /// </summary>
    public static async Task<string?> UpdateProfileAsync(
        UpdateProfileRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateProfileAsync(request);
            return resp.ProfileId;
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update profile");
        }
    }

    // ── Access operations ────────────────────────────────────────────

    /// <summary>
    /// Create an access configuration for an external identity.
    /// </summary>
    public static async Task<TransferAccessInfo> CreateAccessAsync(
        CreateAccessRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateAccessAsync(request);
            return new TransferAccessInfo(
                ServerId: resp.ServerId,
                ExternalId: resp.ExternalId);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create access");
        }
    }

    /// <summary>
    /// Delete an access configuration.
    /// </summary>
    public static async Task DeleteAccessAsync(
        string serverId,
        string externalId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAccessAsync(new DeleteAccessRequest
            {
                ServerId = serverId,
                ExternalId = externalId
            });
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete access for '{externalId}' on server '{serverId}'");
        }
    }

    /// <summary>
    /// Describe an access configuration.
    /// </summary>
    public static async Task<TransferAccessInfo> DescribeAccessAsync(
        string serverId,
        string externalId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeAccessAsync(
                new DescribeAccessRequest
                {
                    ServerId = serverId,
                    ExternalId = externalId
                });
            var a = resp.Access;
            return new TransferAccessInfo(
                ServerId: resp.ServerId,
                ExternalId: a.ExternalId,
                HomeDirectory: a.HomeDirectory,
                HomeDirectoryType: a.HomeDirectoryType?.Value,
                Role: a.Role);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe access for '{externalId}'");
        }
    }

    /// <summary>
    /// List accesses for a server.
    /// </summary>
    public static async Task<List<TransferAccessInfo>> ListAccessesAsync(
        string serverId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAccessesRequest { ServerId = serverId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListAccessesAsync(request);
            return resp.Accesses.Select(a => new TransferAccessInfo(
                ServerId: resp.ServerId,
                ExternalId: a.ExternalId,
                HomeDirectory: a.HomeDirectory,
                HomeDirectoryType: a.HomeDirectoryType?.Value,
                Role: a.Role)).ToList();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list accesses for server '{serverId}'");
        }
    }

    /// <summary>
    /// Update an access configuration.
    /// </summary>
    public static async Task<TransferAccessInfo> UpdateAccessAsync(
        UpdateAccessRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateAccessAsync(request);
            return new TransferAccessInfo(
                ServerId: resp.ServerId,
                ExternalId: resp.ExternalId);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update access");
        }
    }

    // ── Workflow execution & Identity testing ────────────────────────

    /// <summary>
    /// Send workflow step state to advance or complete a workflow step.
    /// </summary>
    public static async Task<TransferWorkflowStepResult>
        SendWorkflowStepStateAsync(
            SendWorkflowStepStateRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.SendWorkflowStepStateAsync(request);
            return new TransferWorkflowStepResult();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to send workflow step state");
        }
    }

    /// <summary>
    /// Test an identity provider configuration.
    /// </summary>
    public static async Task<TransferTestIdentityProviderResult>
        TestIdentityProviderAsync(
            string serverId,
            string userName,
            string? userPassword = null,
            string? serverProtocol = null,
            string? sourceIp = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new TestIdentityProviderRequest
        {
            ServerId = serverId,
            UserName = userName
        };
        if (userPassword != null) request.UserPassword = userPassword;
        if (serverProtocol != null) request.ServerProtocol = serverProtocol;
        if (sourceIp != null) request.SourceIp = sourceIp;

        try
        {
            var resp = await client.TestIdentityProviderAsync(request);
            return new TransferTestIdentityProviderResult(
                StatusCode: resp.StatusCode.ToString(),
                Message: resp.Message,
                Response: resp.Response,
                Url: resp.Url);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to test identity provider for user '{userName}'");
        }
    }

    // ── Tagging ──────────────────────────────────────────────────────

    /// <summary>
    /// Tag a Transfer resource.
    /// </summary>
    public static async Task<TransferTagResult> TagResourceAsync(
        string arn,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                Arn = arn,
                Tags = tags
            });
            return new TransferTagResult();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{arn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Transfer resource.
    /// </summary>
    public static async Task<TransferTagResult> UntagResourceAsync(
        string arn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                Arn = arn,
                TagKeys = tagKeys
            });
            return new TransferTagResult();
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{arn}'");
        }
    }

    /// <summary>
    /// List tags for a Transfer resource.
    /// </summary>
    public static async Task<TransferListTagsResult>
        ListTagsForResourceAsync(
            string arn,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForResourceRequest { Arn = arn };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListTagsForResourceAsync(request);
            return new TransferListTagsResult(
                Arn: resp.Arn,
                Tags: resp.Tags,
                NextToken: resp.NextToken);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{arn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateServerAsync"/>.</summary>
    public static string? CreateServer(CreateServerRequest request, RegionEndpoint? region = null)
        => CreateServerAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteServerAsync"/>.</summary>
    public static void DeleteServer(string serverId, RegionEndpoint? region = null)
        => DeleteServerAsync(serverId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeServerAsync"/>.</summary>
    public static TransferServerInfo DescribeServer(string serverId, RegionEndpoint? region = null)
        => DescribeServerAsync(serverId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListServersAsync"/>.</summary>
    public static List<TransferServerInfo> ListServers(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListServersAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateServerAsync"/>.</summary>
    public static string? UpdateServer(UpdateServerRequest request, RegionEndpoint? region = null)
        => UpdateServerAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartServerAsync"/>.</summary>
    public static void StartServer(string serverId, RegionEndpoint? region = null)
        => StartServerAsync(serverId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopServerAsync"/>.</summary>
    public static void StopServer(string serverId, RegionEndpoint? region = null)
        => StopServerAsync(serverId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateUserAsync"/>.</summary>
    public static TransferUserInfo CreateUser(CreateUserRequest request, RegionEndpoint? region = null)
        => CreateUserAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteUserAsync"/>.</summary>
    public static void DeleteUser(string serverId, string userName, RegionEndpoint? region = null)
        => DeleteUserAsync(serverId, userName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeUserAsync"/>.</summary>
    public static TransferUserInfo DescribeUser(string serverId, string userName, RegionEndpoint? region = null)
        => DescribeUserAsync(serverId, userName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListUsersAsync"/>.</summary>
    public static List<TransferUserInfo> ListUsers(string serverId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListUsersAsync(serverId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateUserAsync"/>.</summary>
    public static TransferUserInfo UpdateUser(UpdateUserRequest request, RegionEndpoint? region = null)
        => UpdateUserAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ImportSshPublicKeyAsync"/>.</summary>
    public static TransferSshPublicKeyInfo ImportSshPublicKey(string serverId, string userName, string sshPublicKeyBody, RegionEndpoint? region = null)
        => ImportSshPublicKeyAsync(serverId, userName, sshPublicKeyBody, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSshPublicKeyAsync"/>.</summary>
    public static void DeleteSshPublicKey(string serverId, string userName, string sshPublicKeyId, RegionEndpoint? region = null)
        => DeleteSshPublicKeyAsync(serverId, userName, sshPublicKeyId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateWorkflowAsync"/>.</summary>
    public static string? CreateWorkflow(CreateWorkflowRequest request, RegionEndpoint? region = null)
        => CreateWorkflowAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteWorkflowAsync"/>.</summary>
    public static void DeleteWorkflow(string workflowId, RegionEndpoint? region = null)
        => DeleteWorkflowAsync(workflowId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeWorkflowAsync"/>.</summary>
    public static TransferWorkflowInfo DescribeWorkflow(string workflowId, RegionEndpoint? region = null)
        => DescribeWorkflowAsync(workflowId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListWorkflowsAsync"/>.</summary>
    public static List<TransferWorkflowInfo> ListWorkflows(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListWorkflowsAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateAgreementAsync"/>.</summary>
    public static string? CreateAgreement(CreateAgreementRequest request, RegionEndpoint? region = null)
        => CreateAgreementAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAgreementAsync"/>.</summary>
    public static void DeleteAgreement(string agreementId, string serverId, RegionEndpoint? region = null)
        => DeleteAgreementAsync(agreementId, serverId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAgreementAsync"/>.</summary>
    public static TransferAgreementInfo DescribeAgreement(string agreementId, string serverId, RegionEndpoint? region = null)
        => DescribeAgreementAsync(agreementId, serverId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAgreementsAsync"/>.</summary>
    public static List<TransferAgreementInfo> ListAgreements(string serverId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListAgreementsAsync(serverId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateAgreementAsync"/>.</summary>
    public static string? UpdateAgreement(UpdateAgreementRequest request, RegionEndpoint? region = null)
        => UpdateAgreementAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateConnectorAsync"/>.</summary>
    public static string? CreateConnector(CreateConnectorRequest request, RegionEndpoint? region = null)
        => CreateConnectorAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteConnectorAsync"/>.</summary>
    public static void DeleteConnector(string connectorId, RegionEndpoint? region = null)
        => DeleteConnectorAsync(connectorId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeConnectorAsync"/>.</summary>
    public static TransferConnectorInfo DescribeConnector(string connectorId, RegionEndpoint? region = null)
        => DescribeConnectorAsync(connectorId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListConnectorsAsync"/>.</summary>
    public static List<TransferConnectorInfo> ListConnectors(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListConnectorsAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateConnectorAsync"/>.</summary>
    public static string? UpdateConnector(UpdateConnectorRequest request, RegionEndpoint? region = null)
        => UpdateConnectorAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateProfileAsync"/>.</summary>
    public static string? CreateProfile(CreateProfileRequest request, RegionEndpoint? region = null)
        => CreateProfileAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteProfileAsync"/>.</summary>
    public static void DeleteProfile(string profileId, RegionEndpoint? region = null)
        => DeleteProfileAsync(profileId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeProfileAsync"/>.</summary>
    public static TransferProfileInfo DescribeProfile(string profileId, RegionEndpoint? region = null)
        => DescribeProfileAsync(profileId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListProfilesAsync"/>.</summary>
    public static List<TransferProfileInfo> ListProfiles(string? profileType = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListProfilesAsync(profileType, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateProfileAsync"/>.</summary>
    public static string? UpdateProfile(UpdateProfileRequest request, RegionEndpoint? region = null)
        => UpdateProfileAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateAccessAsync"/>.</summary>
    public static TransferAccessInfo CreateAccess(CreateAccessRequest request, RegionEndpoint? region = null)
        => CreateAccessAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAccessAsync"/>.</summary>
    public static void DeleteAccess(string serverId, string externalId, RegionEndpoint? region = null)
        => DeleteAccessAsync(serverId, externalId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAccessAsync"/>.</summary>
    public static TransferAccessInfo DescribeAccess(string serverId, string externalId, RegionEndpoint? region = null)
        => DescribeAccessAsync(serverId, externalId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAccessesAsync"/>.</summary>
    public static List<TransferAccessInfo> ListAccesses(string serverId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListAccessesAsync(serverId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateAccessAsync"/>.</summary>
    public static TransferAccessInfo UpdateAccess(UpdateAccessRequest request, RegionEndpoint? region = null)
        => UpdateAccessAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SendWorkflowStepStateAsync"/>.</summary>
    public static TransferWorkflowStepResult SendWorkflowStepState(SendWorkflowStepStateRequest request, RegionEndpoint? region = null)
        => SendWorkflowStepStateAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TestIdentityProviderAsync"/>.</summary>
    public static TransferTestIdentityProviderResult TestIdentityProvider(string serverId, string userName, string? userPassword = null, string? serverProtocol = null, string? sourceIp = null, RegionEndpoint? region = null)
        => TestIdentityProviderAsync(serverId, userName, userPassword, serverProtocol, sourceIp, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static TransferTagResult TagResource(string arn, List<Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(arn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static TransferTagResult UntagResource(string arn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(arn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static TransferListTagsResult ListTagsForResource(string arn, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(arn, maxResults, nextToken, region).GetAwaiter().GetResult();

}

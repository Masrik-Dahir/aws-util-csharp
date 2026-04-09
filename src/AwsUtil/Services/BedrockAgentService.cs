using Amazon;
using Amazon.BedrockAgent;
using Amazon.BedrockAgent.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for Amazon Bedrock Agent operations.
/// </summary>
public sealed record CreateBedrockAgentResult(string? AgentId = null, string? AgentArn = null);
public sealed record GetBedrockAgentResult(Agent? Agent = null);
public sealed record ListBedrockAgentsResult(List<AgentSummary>? AgentSummaries = null);
public sealed record UpdateBedrockAgentResult(string? AgentId = null, string? AgentArn = null);
public sealed record PrepareAgentResult(string? AgentId = null, string? AgentStatus = null);
public sealed record CreateAgentAliasResult(string? AgentAliasId = null, string? AgentAliasArn = null);
public sealed record GetAgentAliasResult(AgentAlias? AgentAlias = null);
public sealed record ListAgentAliasesResult(List<AgentAliasSummary>? AgentAliasSummaries = null);
public sealed record UpdateAgentAliasResult(string? AgentAliasId = null, string? AgentAliasArn = null);
public sealed record CreateAgentActionGroupResult(string? ActionGroupId = null);
public sealed record GetAgentActionGroupResult(AgentActionGroup? AgentActionGroup = null);
public sealed record ListAgentActionGroupsResult(List<ActionGroupSummary>? ActionGroupSummaries = null);
public sealed record UpdateAgentActionGroupResult(string? ActionGroupId = null);
public sealed record CreateBedrockKnowledgeBaseResult(string? KnowledgeBaseId = null, string? KnowledgeBaseArn = null);
public sealed record GetBedrockKnowledgeBaseResult(KnowledgeBase? KnowledgeBase = null);
public sealed record ListBedrockKnowledgeBasesResult(List<KnowledgeBaseSummary>? KnowledgeBaseSummaries = null);
public sealed record UpdateBedrockKnowledgeBaseResult(string? KnowledgeBaseId = null, string? KnowledgeBaseArn = null);
public sealed record AssociateAgentKnowledgeBaseResult(bool Success = true);
public sealed record DisassociateAgentKnowledgeBaseResult(bool Success = true);
public sealed record GetAgentKnowledgeBaseResult(AgentKnowledgeBase? AgentKnowledgeBase = null);
public sealed record ListAgentKnowledgeBasesResult(List<AgentKnowledgeBaseSummary>? AgentKnowledgeBaseSummaries = null);
public sealed record UpdateAgentKnowledgeBaseResult(bool Success = true);
public sealed record CreateBedrockDataSourceResult(string? DataSourceId = null);
public sealed record GetBedrockDataSourceResult(DataSource? DataSource = null);
public sealed record ListBedrockDataSourcesResult(List<DataSourceSummary>? DataSourceSummaries = null);
public sealed record UpdateBedrockDataSourceResult(string? DataSourceId = null);
public sealed record StartIngestionJobResult(string? IngestionJobId = null);
public sealed record GetIngestionJobResult(IngestionJob? IngestionJob = null);
public sealed record ListIngestionJobsResult(List<IngestionJobSummary>? IngestionJobSummaries = null);
public sealed record BedrockAgentTagResourceResult(bool Success = true);
public sealed record BedrockAgentUntagResourceResult(bool Success = true);
public sealed record ListBedrockAgentTagsResult(Dictionary<string, string>? Tags = null);

/// <summary>
/// Utility helpers for Amazon Bedrock Agent.
/// </summary>
public static class BedrockAgentService
{
    private static AmazonBedrockAgentClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonBedrockAgentClient>(region);

    // ──────────────────────────── Agents ────────────────────────────

    /// <summary>
    /// Create a Bedrock agent.
    /// </summary>
    public static async Task<CreateBedrockAgentResult> CreateAgentAsync(
        CreateAgentRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateAgentAsync(request);
            return new CreateBedrockAgentResult(
                AgentId: resp.Agent?.AgentId,
                AgentArn: resp.Agent?.AgentArn);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Bedrock agent '{request.AgentName}'");
        }
    }

    /// <summary>
    /// Delete a Bedrock agent.
    /// </summary>
    public static async Task DeleteAgentAsync(
        string agentId,
        bool? skipResourceInUseCheck = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteAgentRequest { AgentId = agentId };
        if (skipResourceInUseCheck.HasValue)
            request.SkipResourceInUseCheck = skipResourceInUseCheck.Value;

        try
        {
            await client.DeleteAgentAsync(request);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Bedrock agent '{agentId}'");
        }
    }

    /// <summary>
    /// Get details about a Bedrock agent.
    /// </summary>
    public static async Task<GetBedrockAgentResult> GetAgentAsync(
        string agentId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAgentAsync(
                new GetAgentRequest { AgentId = agentId });
            return new GetBedrockAgentResult(Agent: resp.Agent);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Bedrock agent '{agentId}'");
        }
    }

    /// <summary>
    /// List Bedrock agents, automatically paginating.
    /// </summary>
    public static async Task<ListBedrockAgentsResult> ListAgentsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var agents = new List<AgentSummary>();
        var request = new ListAgentsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListAgentsAsync(request);
                if (resp.AgentSummaries != null) agents.AddRange(resp.AgentSummaries);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list Bedrock agents");
        }

        return new ListBedrockAgentsResult(AgentSummaries: agents);
    }

    /// <summary>
    /// Update a Bedrock agent.
    /// </summary>
    public static async Task<UpdateBedrockAgentResult> UpdateAgentAsync(
        UpdateAgentRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateAgentAsync(request);
            return new UpdateBedrockAgentResult(
                AgentId: resp.Agent?.AgentId,
                AgentArn: resp.Agent?.AgentArn);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Bedrock agent '{request.AgentId}'");
        }
    }

    /// <summary>
    /// Prepare a Bedrock agent for invocation.
    /// </summary>
    public static async Task<PrepareAgentResult> PrepareAgentAsync(
        string agentId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PrepareAgentAsync(
                new PrepareAgentRequest { AgentId = agentId });
            return new PrepareAgentResult(
                AgentId: resp.AgentId,
                AgentStatus: resp.AgentStatus?.Value);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to prepare Bedrock agent '{agentId}'");
        }
    }

    // ──────────────────────────── Agent Aliases ────────────────────────────

    /// <summary>
    /// Create an alias for a Bedrock agent.
    /// </summary>
    public static async Task<CreateAgentAliasResult> CreateAgentAliasAsync(
        CreateAgentAliasRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateAgentAliasAsync(request);
            return new CreateAgentAliasResult(
                AgentAliasId: resp.AgentAlias?.AgentAliasId,
                AgentAliasArn: resp.AgentAlias?.AgentAliasArn);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Bedrock agent alias '{request.AgentAliasName}'");
        }
    }

    /// <summary>
    /// Delete an alias for a Bedrock agent.
    /// </summary>
    public static async Task DeleteAgentAliasAsync(
        string agentId,
        string agentAliasId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAgentAliasAsync(new DeleteAgentAliasRequest
            {
                AgentId = agentId,
                AgentAliasId = agentAliasId
            });
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Bedrock agent alias '{agentAliasId}'");
        }
    }

    /// <summary>
    /// Get details about a Bedrock agent alias.
    /// </summary>
    public static async Task<GetAgentAliasResult> GetAgentAliasAsync(
        string agentId,
        string agentAliasId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAgentAliasAsync(new GetAgentAliasRequest
            {
                AgentId = agentId,
                AgentAliasId = agentAliasId
            });
            return new GetAgentAliasResult(AgentAlias: resp.AgentAlias);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Bedrock agent alias '{agentAliasId}'");
        }
    }

    /// <summary>
    /// List aliases for a Bedrock agent, automatically paginating.
    /// </summary>
    public static async Task<ListAgentAliasesResult> ListAgentAliasesAsync(
        string agentId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var aliases = new List<AgentAliasSummary>();
        var request = new ListAgentAliasesRequest { AgentId = agentId };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListAgentAliasesAsync(request);
                if (resp.AgentAliasSummaries != null)
                    aliases.AddRange(resp.AgentAliasSummaries);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list Bedrock agent aliases for '{agentId}'");
        }

        return new ListAgentAliasesResult(AgentAliasSummaries: aliases);
    }

    /// <summary>
    /// Update an alias for a Bedrock agent.
    /// </summary>
    public static async Task<UpdateAgentAliasResult> UpdateAgentAliasAsync(
        UpdateAgentAliasRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateAgentAliasAsync(request);
            return new UpdateAgentAliasResult(
                AgentAliasId: resp.AgentAlias?.AgentAliasId,
                AgentAliasArn: resp.AgentAlias?.AgentAliasArn);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Bedrock agent alias '{request.AgentAliasId}'");
        }
    }

    // ──────────────────────────── Agent Action Groups ────────────────────────────

    /// <summary>
    /// Create an action group for a Bedrock agent.
    /// </summary>
    public static async Task<CreateAgentActionGroupResult> CreateAgentActionGroupAsync(
        CreateAgentActionGroupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateAgentActionGroupAsync(request);
            return new CreateAgentActionGroupResult(
                ActionGroupId: resp.AgentActionGroup?.ActionGroupId);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Bedrock agent action group '{request.ActionGroupName}'");
        }
    }

    /// <summary>
    /// Delete an action group from a Bedrock agent.
    /// </summary>
    public static async Task DeleteAgentActionGroupAsync(
        string agentId,
        string agentVersion,
        string actionGroupId,
        bool? skipResourceInUseCheck = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteAgentActionGroupRequest
        {
            AgentId = agentId,
            AgentVersion = agentVersion,
            ActionGroupId = actionGroupId
        };
        if (skipResourceInUseCheck.HasValue)
            request.SkipResourceInUseCheck = skipResourceInUseCheck.Value;

        try
        {
            await client.DeleteAgentActionGroupAsync(request);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Bedrock agent action group '{actionGroupId}'");
        }
    }

    /// <summary>
    /// Get details about a Bedrock agent action group.
    /// </summary>
    public static async Task<GetAgentActionGroupResult> GetAgentActionGroupAsync(
        string agentId,
        string agentVersion,
        string actionGroupId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAgentActionGroupAsync(
                new GetAgentActionGroupRequest
                {
                    AgentId = agentId,
                    AgentVersion = agentVersion,
                    ActionGroupId = actionGroupId
                });
            return new GetAgentActionGroupResult(
                AgentActionGroup: resp.AgentActionGroup);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Bedrock agent action group '{actionGroupId}'");
        }
    }

    /// <summary>
    /// List action groups for a Bedrock agent, automatically paginating.
    /// </summary>
    public static async Task<ListAgentActionGroupsResult> ListAgentActionGroupsAsync(
        string agentId,
        string agentVersion,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var groups = new List<ActionGroupSummary>();
        var request = new ListAgentActionGroupsRequest
        {
            AgentId = agentId,
            AgentVersion = agentVersion
        };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListAgentActionGroupsAsync(request);
                if (resp.ActionGroupSummaries != null)
                    groups.AddRange(resp.ActionGroupSummaries);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list Bedrock agent action groups for '{agentId}'");
        }

        return new ListAgentActionGroupsResult(ActionGroupSummaries: groups);
    }

    /// <summary>
    /// Update an action group for a Bedrock agent.
    /// </summary>
    public static async Task<UpdateAgentActionGroupResult> UpdateAgentActionGroupAsync(
        UpdateAgentActionGroupRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateAgentActionGroupAsync(request);
            return new UpdateAgentActionGroupResult(
                ActionGroupId: resp.AgentActionGroup?.ActionGroupId);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Bedrock agent action group '{request.ActionGroupId}'");
        }
    }

    // ──────────────────────────── Knowledge Bases ────────────────────────────

    /// <summary>
    /// Create a Bedrock knowledge base.
    /// </summary>
    public static async Task<CreateBedrockKnowledgeBaseResult> CreateKnowledgeBaseAsync(
        CreateKnowledgeBaseRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateKnowledgeBaseAsync(request);
            return new CreateBedrockKnowledgeBaseResult(
                KnowledgeBaseId: resp.KnowledgeBase?.KnowledgeBaseId,
                KnowledgeBaseArn: resp.KnowledgeBase?.KnowledgeBaseArn);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Bedrock knowledge base '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a Bedrock knowledge base.
    /// </summary>
    public static async Task DeleteKnowledgeBaseAsync(
        string knowledgeBaseId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteKnowledgeBaseAsync(
                new DeleteKnowledgeBaseRequest { KnowledgeBaseId = knowledgeBaseId });
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Bedrock knowledge base '{knowledgeBaseId}'");
        }
    }

    /// <summary>
    /// Get details about a Bedrock knowledge base.
    /// </summary>
    public static async Task<GetBedrockKnowledgeBaseResult> GetKnowledgeBaseAsync(
        string knowledgeBaseId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetKnowledgeBaseAsync(
                new GetKnowledgeBaseRequest { KnowledgeBaseId = knowledgeBaseId });
            return new GetBedrockKnowledgeBaseResult(KnowledgeBase: resp.KnowledgeBase);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Bedrock knowledge base '{knowledgeBaseId}'");
        }
    }

    /// <summary>
    /// List Bedrock knowledge bases, automatically paginating.
    /// </summary>
    public static async Task<ListBedrockKnowledgeBasesResult> ListKnowledgeBasesAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var knowledgeBases = new List<KnowledgeBaseSummary>();
        var request = new ListKnowledgeBasesRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListKnowledgeBasesAsync(request);
                if (resp.KnowledgeBaseSummaries != null)
                    knowledgeBases.AddRange(resp.KnowledgeBaseSummaries);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Bedrock knowledge bases");
        }

        return new ListBedrockKnowledgeBasesResult(
            KnowledgeBaseSummaries: knowledgeBases);
    }

    /// <summary>
    /// Update a Bedrock knowledge base.
    /// </summary>
    public static async Task<UpdateBedrockKnowledgeBaseResult> UpdateKnowledgeBaseAsync(
        UpdateKnowledgeBaseRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateKnowledgeBaseAsync(request);
            return new UpdateBedrockKnowledgeBaseResult(
                KnowledgeBaseId: resp.KnowledgeBase?.KnowledgeBaseId,
                KnowledgeBaseArn: resp.KnowledgeBase?.KnowledgeBaseArn);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Bedrock knowledge base '{request.KnowledgeBaseId}'");
        }
    }

    // ──────────────────────────── Agent Knowledge Base Associations ────────────────────────────

    /// <summary>
    /// Associate a knowledge base with a Bedrock agent.
    /// </summary>
    public static async Task<AssociateAgentKnowledgeBaseResult>
        AssociateAgentKnowledgeBaseAsync(
            AssociateAgentKnowledgeBaseRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AssociateAgentKnowledgeBaseAsync(request);
            return new AssociateAgentKnowledgeBaseResult();
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to associate knowledge base '{request.KnowledgeBaseId}' with agent '{request.AgentId}'");
        }
    }

    /// <summary>
    /// Disassociate a knowledge base from a Bedrock agent.
    /// </summary>
    public static async Task<DisassociateAgentKnowledgeBaseResult>
        DisassociateAgentKnowledgeBaseAsync(
            string agentId,
            string agentVersion,
            string knowledgeBaseId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisassociateAgentKnowledgeBaseAsync(
                new DisassociateAgentKnowledgeBaseRequest
                {
                    AgentId = agentId,
                    AgentVersion = agentVersion,
                    KnowledgeBaseId = knowledgeBaseId
                });
            return new DisassociateAgentKnowledgeBaseResult();
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to disassociate knowledge base '{knowledgeBaseId}' from agent '{agentId}'");
        }
    }

    /// <summary>
    /// Get details about a knowledge base associated with a Bedrock agent.
    /// </summary>
    public static async Task<GetAgentKnowledgeBaseResult> GetAgentKnowledgeBaseAsync(
        string agentId,
        string agentVersion,
        string knowledgeBaseId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAgentKnowledgeBaseAsync(
                new GetAgentKnowledgeBaseRequest
                {
                    AgentId = agentId,
                    AgentVersion = agentVersion,
                    KnowledgeBaseId = knowledgeBaseId
                });
            return new GetAgentKnowledgeBaseResult(
                AgentKnowledgeBase: resp.AgentKnowledgeBase);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get agent knowledge base '{knowledgeBaseId}' for '{agentId}'");
        }
    }

    /// <summary>
    /// List knowledge bases associated with a Bedrock agent, automatically paginating.
    /// </summary>
    public static async Task<ListAgentKnowledgeBasesResult> ListAgentKnowledgeBasesAsync(
        string agentId,
        string agentVersion,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var knowledgeBases = new List<AgentKnowledgeBaseSummary>();
        var request = new ListAgentKnowledgeBasesRequest
        {
            AgentId = agentId,
            AgentVersion = agentVersion
        };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListAgentKnowledgeBasesAsync(request);
                if (resp.AgentKnowledgeBaseSummaries != null)
                    knowledgeBases.AddRange(resp.AgentKnowledgeBaseSummaries);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list agent knowledge bases for '{agentId}'");
        }

        return new ListAgentKnowledgeBasesResult(
            AgentKnowledgeBaseSummaries: knowledgeBases);
    }

    /// <summary>
    /// Update a knowledge base association with a Bedrock agent.
    /// </summary>
    public static async Task<UpdateAgentKnowledgeBaseResult> UpdateAgentKnowledgeBaseAsync(
        UpdateAgentKnowledgeBaseRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateAgentKnowledgeBaseAsync(request);
            return new UpdateAgentKnowledgeBaseResult();
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update agent knowledge base '{request.KnowledgeBaseId}' for '{request.AgentId}'");
        }
    }

    // ──────────────────────────── Data Sources ────────────────────────────

    /// <summary>
    /// Create a data source for a Bedrock knowledge base.
    /// </summary>
    public static async Task<CreateBedrockDataSourceResult> CreateDataSourceAsync(
        CreateDataSourceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDataSourceAsync(request);
            return new CreateBedrockDataSourceResult(
                DataSourceId: resp.DataSource?.DataSourceId);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Bedrock data source '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a data source from a Bedrock knowledge base.
    /// </summary>
    public static async Task DeleteDataSourceAsync(
        string knowledgeBaseId,
        string dataSourceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDataSourceAsync(new DeleteDataSourceRequest
            {
                KnowledgeBaseId = knowledgeBaseId,
                DataSourceId = dataSourceId
            });
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Bedrock data source '{dataSourceId}'");
        }
    }

    /// <summary>
    /// Get details about a Bedrock data source.
    /// </summary>
    public static async Task<GetBedrockDataSourceResult> GetDataSourceAsync(
        string knowledgeBaseId,
        string dataSourceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDataSourceAsync(new GetDataSourceRequest
            {
                KnowledgeBaseId = knowledgeBaseId,
                DataSourceId = dataSourceId
            });
            return new GetBedrockDataSourceResult(DataSource: resp.DataSource);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Bedrock data source '{dataSourceId}'");
        }
    }

    /// <summary>
    /// List data sources for a Bedrock knowledge base, automatically paginating.
    /// </summary>
    public static async Task<ListBedrockDataSourcesResult> ListDataSourcesAsync(
        string knowledgeBaseId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var dataSources = new List<DataSourceSummary>();
        var request = new ListDataSourcesRequest
        {
            KnowledgeBaseId = knowledgeBaseId
        };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListDataSourcesAsync(request);
                if (resp.DataSourceSummaries != null)
                    dataSources.AddRange(resp.DataSourceSummaries);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list Bedrock data sources for '{knowledgeBaseId}'");
        }

        return new ListBedrockDataSourcesResult(DataSourceSummaries: dataSources);
    }

    /// <summary>
    /// Update a data source for a Bedrock knowledge base.
    /// </summary>
    public static async Task<UpdateBedrockDataSourceResult> UpdateDataSourceAsync(
        UpdateDataSourceRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateDataSourceAsync(request);
            return new UpdateBedrockDataSourceResult(
                DataSourceId: resp.DataSource?.DataSourceId);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Bedrock data source '{request.DataSourceId}'");
        }
    }

    // ──────────────────────────── Ingestion Jobs ────────────────────────────

    /// <summary>
    /// Start an ingestion job for a Bedrock data source.
    /// </summary>
    public static async Task<StartIngestionJobResult> StartIngestionJobAsync(
        StartIngestionJobRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartIngestionJobAsync(request);
            return new StartIngestionJobResult(
                IngestionJobId: resp.IngestionJob?.IngestionJobId);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start ingestion job for data source '{request.DataSourceId}'");
        }
    }

    /// <summary>
    /// Get details about an ingestion job.
    /// </summary>
    public static async Task<GetIngestionJobResult> GetIngestionJobAsync(
        string knowledgeBaseId,
        string dataSourceId,
        string ingestionJobId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetIngestionJobAsync(new GetIngestionJobRequest
            {
                KnowledgeBaseId = knowledgeBaseId,
                DataSourceId = dataSourceId,
                IngestionJobId = ingestionJobId
            });
            return new GetIngestionJobResult(IngestionJob: resp.IngestionJob);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get ingestion job '{ingestionJobId}'");
        }
    }

    /// <summary>
    /// List ingestion jobs for a Bedrock data source, automatically paginating.
    /// </summary>
    public static async Task<ListIngestionJobsResult> ListIngestionJobsAsync(
        string knowledgeBaseId,
        string dataSourceId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var jobs = new List<IngestionJobSummary>();
        var request = new ListIngestionJobsRequest
        {
            KnowledgeBaseId = knowledgeBaseId,
            DataSourceId = dataSourceId
        };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListIngestionJobsAsync(request);
                if (resp.IngestionJobSummaries != null)
                    jobs.AddRange(resp.IngestionJobSummaries);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list ingestion jobs for data source '{dataSourceId}'");
        }

        return new ListIngestionJobsResult(IngestionJobSummaries: jobs);
    }

    // ──────────────────────────── Tags ────────────────────────────

    /// <summary>
    /// Add tags to a Bedrock Agent resource.
    /// </summary>
    public static async Task<BedrockAgentTagResourceResult> TagResourceAsync(
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
            return new BedrockAgentTagResourceResult();
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Bedrock Agent resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Bedrock Agent resource.
    /// </summary>
    public static async Task<BedrockAgentUntagResourceResult> UntagResourceAsync(
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
            return new BedrockAgentUntagResourceResult();
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Bedrock Agent resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Bedrock Agent resource.
    /// </summary>
    public static async Task<ListBedrockAgentTagsResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new ListBedrockAgentTagsResult(Tags: resp.Tags);
        }
        catch (AmazonBedrockAgentException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Bedrock Agent resource '{resourceArn}'");
        }
    }
}

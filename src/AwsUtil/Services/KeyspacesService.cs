using Amazon;
using Amazon.Keyspaces;
using Amazon.Keyspaces.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record KeyspacesCreateKeyspaceResult(
    string? ResourceArn = null);

public sealed record KeyspacesGetKeyspaceResult(
    string? KeyspaceName = null,
    string? ResourceArn = null,
    string? ReplicationStrategy = null);

public sealed record KeyspacesListKeyspacesResult(
    List<KeyspaceSummary>? Keyspaces = null,
    string? NextToken = null);

public sealed record KeyspacesCreateTableResult(
    string? ResourceArn = null);

public sealed record KeyspacesGetTableResult(
    string? KeyspaceName = null,
    string? TableName = null,
    string? ResourceArn = null,
    string? Status = null);

public sealed record KeyspacesListTablesResult(
    List<TableSummary>? Tables = null,
    string? NextToken = null);

public sealed record KeyspacesUpdateTableResult(
    string? ResourceArn = null);

public sealed record KeyspacesRestoreTableResult(
    string? RestoredTableARN = null);

public sealed record KeyspacesCreateTypeResult(
    string? KeyspaceArn = null,
    string? TypeName = null);

public sealed record KeyspacesGetTypeResult(
    string? KeyspaceName = null,
    string? TypeName = null,
    string? Status = null);

public sealed record KeyspacesListTypesResult(
    List<string>? Types = null,
    string? NextToken = null);

public sealed record KeyspacesListTagsForResourceResult(
    List<Tag>? Tags = null,
    string? NextToken = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Keyspaces (for Apache Cassandra).
/// </summary>
public static class KeyspacesService
{
    private static AmazonKeyspacesClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonKeyspacesClient>(region);

    // ── Keyspace operations ─────────────────────────────────────────

    /// <summary>
    /// Create a new keyspace.
    /// </summary>
    public static async Task<KeyspacesCreateKeyspaceResult>
        CreateKeyspaceAsync(
            CreateKeyspaceRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateKeyspaceAsync(request);
            return new KeyspacesCreateKeyspaceResult(
                ResourceArn: resp.ResourceArn);
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create keyspace");
        }
    }

    /// <summary>
    /// Delete a keyspace.
    /// </summary>
    public static async Task DeleteKeyspaceAsync(
        string keyspaceName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteKeyspaceAsync(new DeleteKeyspaceRequest
            {
                KeyspaceName = keyspaceName
            });
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete keyspace '{keyspaceName}'");
        }
    }

    /// <summary>
    /// Get details of a keyspace.
    /// </summary>
    public static async Task<KeyspacesGetKeyspaceResult> GetKeyspaceAsync(
        string keyspaceName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetKeyspaceAsync(new GetKeyspaceRequest
            {
                KeyspaceName = keyspaceName
            });
            return new KeyspacesGetKeyspaceResult(
                KeyspaceName: resp.KeyspaceName,
                ResourceArn: resp.ResourceArn,
                ReplicationStrategy: resp.ReplicationStrategy?.Value);
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get keyspace '{keyspaceName}'");
        }
    }

    /// <summary>
    /// List keyspaces.
    /// </summary>
    public static async Task<KeyspacesListKeyspacesResult>
        ListKeyspacesAsync(
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListKeyspacesRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListKeyspacesAsync(request);
            return new KeyspacesListKeyspacesResult(
                Keyspaces: resp.Keyspaces,
                NextToken: resp.NextToken);
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list keyspaces");
        }
    }

    // ── Table operations ────────────────────────────────────────────

    /// <summary>
    /// Create a new table in a keyspace.
    /// </summary>
    public static async Task<KeyspacesCreateTableResult> CreateTableAsync(
        CreateTableRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateTableAsync(request);
            return new KeyspacesCreateTableResult(
                ResourceArn: resp.ResourceArn);
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create table");
        }
    }

    /// <summary>
    /// Delete a table from a keyspace.
    /// </summary>
    public static async Task DeleteTableAsync(
        string keyspaceName,
        string tableName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTableAsync(new DeleteTableRequest
            {
                KeyspaceName = keyspaceName,
                TableName = tableName
            });
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete table '{tableName}' from keyspace '{keyspaceName}'");
        }
    }

    /// <summary>
    /// Get details of a table.
    /// </summary>
    public static async Task<KeyspacesGetTableResult> GetTableAsync(
        string keyspaceName,
        string tableName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetTableAsync(new GetTableRequest
            {
                KeyspaceName = keyspaceName,
                TableName = tableName
            });
            return new KeyspacesGetTableResult(
                KeyspaceName: resp.KeyspaceName,
                TableName: resp.TableName,
                ResourceArn: resp.ResourceArn,
                Status: resp.Status?.Value);
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get table '{tableName}' from keyspace '{keyspaceName}'");
        }
    }

    /// <summary>
    /// List tables in a keyspace.
    /// </summary>
    public static async Task<KeyspacesListTablesResult> ListTablesAsync(
        string keyspaceName,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTablesRequest
        {
            KeyspaceName = keyspaceName
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListTablesAsync(request);
            return new KeyspacesListTablesResult(
                Tables: resp.Tables,
                NextToken: resp.NextToken);
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tables in keyspace '{keyspaceName}'");
        }
    }

    /// <summary>
    /// Update a table in a keyspace.
    /// </summary>
    public static async Task<KeyspacesUpdateTableResult> UpdateTableAsync(
        UpdateTableRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateTableAsync(request);
            return new KeyspacesUpdateTableResult(
                ResourceArn: resp.ResourceArn);
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to update table");
        }
    }

    /// <summary>
    /// Restore a table to a point in time.
    /// </summary>
    public static async Task<KeyspacesRestoreTableResult> RestoreTableAsync(
        RestoreTableRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RestoreTableAsync(request);
            return new KeyspacesRestoreTableResult(
                RestoredTableARN: resp.RestoredTableARN);
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to restore table");
        }
    }

    // ── Type operations ─────────────────────────────────────────────

    /// <summary>
    /// Create a user-defined type in a keyspace.
    /// </summary>
    public static async Task<KeyspacesCreateTypeResult> CreateTypeAsync(
        CreateTypeRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateTypeAsync(request);
            return new KeyspacesCreateTypeResult(
                KeyspaceArn: resp.KeyspaceArn,
                TypeName: resp.TypeName);
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to create type");
        }
    }

    /// <summary>
    /// Delete a user-defined type from a keyspace.
    /// </summary>
    public static async Task DeleteTypeAsync(
        string keyspaceName,
        string typeName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTypeAsync(new DeleteTypeRequest
            {
                KeyspaceName = keyspaceName,
                TypeName = typeName
            });
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete type '{typeName}' from keyspace '{keyspaceName}'");
        }
    }

    /// <summary>
    /// Get details of a user-defined type.
    /// </summary>
    public static async Task<KeyspacesGetTypeResult> GetTypeAsync(
        string keyspaceName,
        string typeName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetTypeAsync(new GetTypeRequest
            {
                KeyspaceName = keyspaceName,
                TypeName = typeName
            });
            return new KeyspacesGetTypeResult(
                KeyspaceName: resp.KeyspaceName,
                TypeName: resp.TypeName,
                Status: resp.Status?.Value);
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get type '{typeName}' from keyspace '{keyspaceName}'");
        }
    }

    /// <summary>
    /// List user-defined types in a keyspace.
    /// </summary>
    public static async Task<KeyspacesListTypesResult> ListTypesAsync(
        string keyspaceName,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTypesRequest
        {
            KeyspaceName = keyspaceName
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListTypesAsync(request);
            return new KeyspacesListTypesResult(
                Types: resp.Types,
                NextToken: resp.NextToken);
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list types in keyspace '{keyspaceName}'");
        }
    }

    // ── Tag operations ──────────────────────────────────────────────

    /// <summary>
    /// Tag a Keyspaces resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceArn,
        List<Tag> tags,
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
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to tag Keyspaces resource");
        }
    }

    /// <summary>
    /// Untag a Keyspaces resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string resourceArn,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceArn = resourceArn,
                Tags = tags
            });
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to untag Keyspaces resource");
        }
    }

    /// <summary>
    /// List tags for a Keyspaces resource.
    /// </summary>
    public static async Task<KeyspacesListTagsForResourceResult>
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
            return new KeyspacesListTagsForResourceResult(
                Tags: resp.Tags,
                NextToken: resp.NextToken);
        }
        catch (AmazonKeyspacesException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list tags for Keyspaces resource");
        }
    }
}

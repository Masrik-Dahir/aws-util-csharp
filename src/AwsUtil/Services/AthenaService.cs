using Amazon;
using Amazon.Athena;
using Amazon.Athena.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for Amazon Athena operations.
/// </summary>
public sealed record StartQueryExecutionResult(string? QueryExecutionId = null);

public sealed record GetQueryExecutionResult(QueryExecution? QueryExecution = null);

public sealed record GetAthenaQueryResultsResult(
    ResultSet? ResultSet = null,
    string? NextToken = null);

public sealed record ListQueryExecutionsResult(
    List<string>? QueryExecutionIds = null,
    string? NextToken = null);

public sealed record BatchGetQueryExecutionResult(
    List<QueryExecution>? QueryExecutions = null,
    List<UnprocessedQueryExecutionId>? UnprocessedQueryExecutionIds = null);

public sealed record CreateWorkGroupResult(string? Name = null);

public sealed record GetWorkGroupResult(WorkGroup? WorkGroup = null);

public sealed record ListWorkGroupsResult(List<WorkGroupSummary>? WorkGroups = null);

public sealed record CreateNamedQueryResult(string? NamedQueryId = null);

public sealed record GetNamedQueryResult(NamedQuery? NamedQuery = null);

public sealed record ListNamedQueriesResult(
    List<string>? NamedQueryIds = null,
    string? NextToken = null);

public sealed record BatchGetNamedQueryResult(
    List<NamedQuery>? NamedQueries = null,
    List<UnprocessedNamedQueryId>? UnprocessedNamedQueryIds = null);

public sealed record CreateDataCatalogResult(string? Name = null);

public sealed record GetDataCatalogResult(DataCatalog? DataCatalog = null);

public sealed record ListDataCatalogsResult(List<DataCatalogSummary>? DataCatalogsSummary = null);

public sealed record GetAthenaDatabaseResult(Amazon.Athena.Model.Database? Database = null);

public sealed record ListAthenaDatabasesResult(List<Amazon.Athena.Model.Database>? DatabaseList = null);

public sealed record GetTableMetadataResult(TableMetadata? TableMetadata = null);

public sealed record ListTableMetadataResult(List<TableMetadata>? TableMetadataList = null);

public sealed record CreatePreparedStatementResult(string? StatementName = null);

public sealed record GetPreparedStatementResult(PreparedStatement? PreparedStatement = null);

public sealed record ListPreparedStatementsResult(List<PreparedStatementSummary>? PreparedStatements = null);

public sealed record ListAthenaTagsResult(List<Amazon.Athena.Model.Tag>? Tags = null);

/// <summary>
/// Utility helpers for Amazon Athena.
/// </summary>
public static class AthenaService
{
    private static AmazonAthenaClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonAthenaClient>(region);

    // ──────────────────────────── Query Execution ────────────────────────────

    /// <summary>
    /// Start an Athena query execution.
    /// </summary>
    public static async Task<StartQueryExecutionResult> StartQueryExecutionAsync(
        string queryString,
        string? database = null,
        string? catalog = null,
        string? outputLocation = null,
        string? workGroup = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartQueryExecutionRequest { QueryString = queryString };
        if (database != null || catalog != null)
        {
            request.QueryExecutionContext = new QueryExecutionContext();
            if (database != null) request.QueryExecutionContext.Database = database;
            if (catalog != null) request.QueryExecutionContext.Catalog = catalog;
        }
        if (outputLocation != null)
        {
            request.ResultConfiguration = new ResultConfiguration
            {
                OutputLocation = outputLocation
            };
        }
        if (workGroup != null) request.WorkGroup = workGroup;

        try
        {
            var resp = await client.StartQueryExecutionAsync(request);
            return new StartQueryExecutionResult(QueryExecutionId: resp.QueryExecutionId);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start Athena query execution");
        }
    }

    /// <summary>
    /// Stop an Athena query execution.
    /// </summary>
    public static async Task StopQueryExecutionAsync(
        string queryExecutionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StopQueryExecutionAsync(new StopQueryExecutionRequest
            {
                QueryExecutionId = queryExecutionId
            });
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop Athena query execution '{queryExecutionId}'");
        }
    }

    /// <summary>
    /// Get information about an Athena query execution.
    /// </summary>
    public static async Task<GetQueryExecutionResult> GetQueryExecutionAsync(
        string queryExecutionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetQueryExecutionAsync(new GetQueryExecutionRequest
            {
                QueryExecutionId = queryExecutionId
            });
            return new GetQueryExecutionResult(QueryExecution: resp.QueryExecution);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Athena query execution '{queryExecutionId}'");
        }
    }

    /// <summary>
    /// Get the results of an Athena query execution.
    /// </summary>
    public static async Task<GetAthenaQueryResultsResult> GetQueryResultsAsync(
        string queryExecutionId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetQueryResultsRequest
        {
            QueryExecutionId = queryExecutionId
        };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetQueryResultsAsync(request);
            return new GetAthenaQueryResultsResult(
                ResultSet: resp.ResultSet,
                NextToken: resp.NextToken);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Athena query results for '{queryExecutionId}'");
        }
    }

    /// <summary>
    /// List Athena query execution IDs.
    /// </summary>
    public static async Task<ListQueryExecutionsResult> ListQueryExecutionsAsync(
        string? workGroup = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListQueryExecutionsRequest();
        if (workGroup != null) request.WorkGroup = workGroup;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListQueryExecutionsAsync(request);
            return new ListQueryExecutionsResult(
                QueryExecutionIds: resp.QueryExecutionIds,
                NextToken: resp.NextToken);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list Athena query executions");
        }
    }

    /// <summary>
    /// Batch get information about multiple query executions.
    /// </summary>
    public static async Task<BatchGetQueryExecutionResult> BatchGetQueryExecutionAsync(
        List<string> queryExecutionIds, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetQueryExecutionAsync(
                new BatchGetQueryExecutionRequest
                {
                    QueryExecutionIds = queryExecutionIds
                });
            return new BatchGetQueryExecutionResult(
                QueryExecutions: resp.QueryExecutions,
                UnprocessedQueryExecutionIds: resp.UnprocessedQueryExecutionIds);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to batch get Athena query executions");
        }
    }

    // ──────────────────────────── WorkGroups ────────────────────────────

    /// <summary>
    /// Create an Athena workgroup.
    /// </summary>
    public static async Task<CreateWorkGroupResult> CreateWorkGroupAsync(
        string name,
        WorkGroupConfiguration? configuration = null,
        string? description = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateWorkGroupRequest { Name = name };
        if (configuration != null) request.Configuration = configuration;
        if (description != null) request.Description = description;
        if (tags != null)
            request.Tags = tags.Select(kv =>
                new Amazon.Athena.Model.Tag { Key = kv.Key, Value = kv.Value }).ToList();

        try
        {
            await client.CreateWorkGroupAsync(request);
            return new CreateWorkGroupResult(Name: name);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create Athena workgroup '{name}'");
        }
    }

    /// <summary>
    /// Delete an Athena workgroup.
    /// </summary>
    public static async Task DeleteWorkGroupAsync(
        string workGroup,
        bool recursiveDeleteOption = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteWorkGroupAsync(new DeleteWorkGroupRequest
            {
                WorkGroup = workGroup,
                RecursiveDeleteOption = recursiveDeleteOption
            });
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete Athena workgroup '{workGroup}'");
        }
    }

    /// <summary>
    /// Get information about an Athena workgroup.
    /// </summary>
    public static async Task<GetWorkGroupResult> GetWorkGroupAsync(
        string workGroup, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetWorkGroupAsync(new GetWorkGroupRequest
            {
                WorkGroup = workGroup
            });
            return new GetWorkGroupResult(WorkGroup: resp.WorkGroup);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Athena workgroup '{workGroup}'");
        }
    }

    /// <summary>
    /// List all Athena workgroups, automatically paginating.
    /// </summary>
    public static async Task<ListWorkGroupsResult> ListWorkGroupsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var workGroups = new List<WorkGroupSummary>();
        var request = new ListWorkGroupsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListWorkGroupsAsync(request);
                workGroups.AddRange(resp.WorkGroups);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list Athena workgroups");
        }

        return new ListWorkGroupsResult(WorkGroups: workGroups);
    }

    /// <summary>
    /// Update an Athena workgroup.
    /// </summary>
    public static async Task UpdateWorkGroupAsync(
        string workGroup,
        WorkGroupConfigurationUpdates? configurationUpdates = null,
        string? description = null,
        WorkGroupState? state = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateWorkGroupRequest { WorkGroup = workGroup };
        if (configurationUpdates != null) request.ConfigurationUpdates = configurationUpdates;
        if (description != null) request.Description = description;
        if (state != null) request.State = state.Value;

        try
        {
            await client.UpdateWorkGroupAsync(request);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to update Athena workgroup '{workGroup}'");
        }
    }

    // ──────────────────────────── Named Queries ────────────────────────────

    /// <summary>
    /// Create an Athena named query.
    /// </summary>
    public static async Task<CreateNamedQueryResult> CreateNamedQueryAsync(
        string name,
        string database,
        string queryString,
        string? description = null,
        string? workGroup = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateNamedQueryRequest
        {
            Name = name,
            Database = database,
            QueryString = queryString
        };
        if (description != null) request.Description = description;
        if (workGroup != null) request.WorkGroup = workGroup;

        try
        {
            var resp = await client.CreateNamedQueryAsync(request);
            return new CreateNamedQueryResult(NamedQueryId: resp.NamedQueryId);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create Athena named query '{name}'");
        }
    }

    /// <summary>
    /// Delete an Athena named query.
    /// </summary>
    public static async Task DeleteNamedQueryAsync(
        string namedQueryId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteNamedQueryAsync(new DeleteNamedQueryRequest
            {
                NamedQueryId = namedQueryId
            });
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Athena named query '{namedQueryId}'");
        }
    }

    /// <summary>
    /// Get an Athena named query by ID.
    /// </summary>
    public static async Task<GetNamedQueryResult> GetNamedQueryAsync(
        string namedQueryId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetNamedQueryAsync(new GetNamedQueryRequest
            {
                NamedQueryId = namedQueryId
            });
            return new GetNamedQueryResult(NamedQuery: resp.NamedQuery);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Athena named query '{namedQueryId}'");
        }
    }

    /// <summary>
    /// List Athena named query IDs.
    /// </summary>
    public static async Task<ListNamedQueriesResult> ListNamedQueriesAsync(
        string? workGroup = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListNamedQueriesRequest();
        if (workGroup != null) request.WorkGroup = workGroup;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListNamedQueriesAsync(request);
            return new ListNamedQueriesResult(
                NamedQueryIds: resp.NamedQueryIds,
                NextToken: resp.NextToken);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list Athena named queries");
        }
    }

    /// <summary>
    /// Batch get information about multiple named queries.
    /// </summary>
    public static async Task<BatchGetNamedQueryResult> BatchGetNamedQueryAsync(
        List<string> namedQueryIds, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetNamedQueryAsync(
                new BatchGetNamedQueryRequest { NamedQueryIds = namedQueryIds });
            return new BatchGetNamedQueryResult(
                NamedQueries: resp.NamedQueries,
                UnprocessedNamedQueryIds: resp.UnprocessedNamedQueryIds);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to batch get Athena named queries");
        }
    }

    // ──────────────────────────── Data Catalogs ────────────────────────────

    /// <summary>
    /// Create an Athena data catalog.
    /// </summary>
    public static async Task<CreateDataCatalogResult> CreateDataCatalogAsync(
        string name,
        DataCatalogType type,
        string? description = null,
        Dictionary<string, string>? parameters = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDataCatalogRequest { Name = name, Type = type };
        if (description != null) request.Description = description;
        if (parameters != null) request.Parameters = parameters;
        if (tags != null)
            request.Tags = tags.Select(kv =>
                new Amazon.Athena.Model.Tag { Key = kv.Key, Value = kv.Value }).ToList();

        try
        {
            await client.CreateDataCatalogAsync(request);
            return new CreateDataCatalogResult(Name: name);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create Athena data catalog '{name}'");
        }
    }

    /// <summary>
    /// Delete an Athena data catalog.
    /// </summary>
    public static async Task DeleteDataCatalogAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDataCatalogAsync(new DeleteDataCatalogRequest { Name = name });
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete Athena data catalog '{name}'");
        }
    }

    /// <summary>
    /// Get an Athena data catalog by name.
    /// </summary>
    public static async Task<GetDataCatalogResult> GetDataCatalogAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDataCatalogAsync(new GetDataCatalogRequest
            {
                Name = name
            });
            return new GetDataCatalogResult(DataCatalog: resp.DataCatalog);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Athena data catalog '{name}'");
        }
    }

    /// <summary>
    /// List all Athena data catalogs, automatically paginating.
    /// </summary>
    public static async Task<ListDataCatalogsResult> ListDataCatalogsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var catalogs = new List<DataCatalogSummary>();
        var request = new ListDataCatalogsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListDataCatalogsAsync(request);
                catalogs.AddRange(resp.DataCatalogsSummary);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list Athena data catalogs");
        }

        return new ListDataCatalogsResult(DataCatalogsSummary: catalogs);
    }

    /// <summary>
    /// Update an Athena data catalog.
    /// </summary>
    public static async Task UpdateDataCatalogAsync(
        string name,
        DataCatalogType type,
        string? description = null,
        Dictionary<string, string>? parameters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateDataCatalogRequest { Name = name, Type = type };
        if (description != null) request.Description = description;
        if (parameters != null) request.Parameters = parameters;

        try
        {
            await client.UpdateDataCatalogAsync(request);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to update Athena data catalog '{name}'");
        }
    }

    // ──────────────────────────── Databases (Catalog) ────────────────────────────

    /// <summary>
    /// Get a database from an Athena data catalog.
    /// </summary>
    public static async Task<GetAthenaDatabaseResult> GetDatabaseAsync(
        string catalogName,
        string databaseName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDatabaseAsync(new GetDatabaseRequest
            {
                CatalogName = catalogName,
                DatabaseName = databaseName
            });
            return new GetAthenaDatabaseResult(Database: resp.Database);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Athena database '{databaseName}' from catalog '{catalogName}'");
        }
    }

    /// <summary>
    /// List databases in an Athena data catalog, automatically paginating.
    /// </summary>
    public static async Task<ListAthenaDatabasesResult> ListDatabasesAsync(
        string catalogName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var databases = new List<Amazon.Athena.Model.Database>();
        var request = new ListDatabasesRequest { CatalogName = catalogName };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListDatabasesAsync(request);
                databases.AddRange(resp.DatabaseList);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list Athena databases in catalog '{catalogName}'");
        }

        return new ListAthenaDatabasesResult(DatabaseList: databases);
    }

    // ──────────────────────────── Table Metadata ────────────────────────────

    /// <summary>
    /// Get table metadata from an Athena data catalog.
    /// </summary>
    public static async Task<GetTableMetadataResult> GetTableMetadataAsync(
        string catalogName,
        string databaseName,
        string tableName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetTableMetadataAsync(new GetTableMetadataRequest
            {
                CatalogName = catalogName,
                DatabaseName = databaseName,
                TableName = tableName
            });
            return new GetTableMetadataResult(TableMetadata: resp.TableMetadata);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Athena table metadata for '{tableName}'");
        }
    }

    /// <summary>
    /// List table metadata in an Athena database, automatically paginating.
    /// </summary>
    public static async Task<ListTableMetadataResult> ListTableMetadataAsync(
        string catalogName,
        string databaseName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var tables = new List<TableMetadata>();
        var request = new ListTableMetadataRequest
        {
            CatalogName = catalogName,
            DatabaseName = databaseName
        };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListTableMetadataAsync(request);
                tables.AddRange(resp.TableMetadataList);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list Athena table metadata in '{databaseName}'");
        }

        return new ListTableMetadataResult(TableMetadataList: tables);
    }

    // ──────────────────────────── Prepared Statements ────────────────────────────

    /// <summary>
    /// Create an Athena prepared statement.
    /// </summary>
    public static async Task<CreatePreparedStatementResult> CreatePreparedStatementAsync(
        string statementName,
        string workGroup,
        string queryStatement,
        string? description = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreatePreparedStatementRequest
        {
            StatementName = statementName,
            WorkGroup = workGroup,
            QueryStatement = queryStatement
        };
        if (description != null) request.Description = description;

        try
        {
            await client.CreatePreparedStatementAsync(request);
            return new CreatePreparedStatementResult(StatementName: statementName);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Athena prepared statement '{statementName}'");
        }
    }

    /// <summary>
    /// Delete an Athena prepared statement.
    /// </summary>
    public static async Task DeletePreparedStatementAsync(
        string statementName,
        string workGroup,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePreparedStatementAsync(new DeletePreparedStatementRequest
            {
                StatementName = statementName,
                WorkGroup = workGroup
            });
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Athena prepared statement '{statementName}'");
        }
    }

    /// <summary>
    /// Get an Athena prepared statement.
    /// </summary>
    public static async Task<GetPreparedStatementResult> GetPreparedStatementAsync(
        string statementName,
        string workGroup,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetPreparedStatementAsync(
                new GetPreparedStatementRequest
                {
                    StatementName = statementName,
                    WorkGroup = workGroup
                });
            return new GetPreparedStatementResult(PreparedStatement: resp.PreparedStatement);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get Athena prepared statement '{statementName}'");
        }
    }

    /// <summary>
    /// List Athena prepared statements in a workgroup, automatically paginating.
    /// </summary>
    public static async Task<ListPreparedStatementsResult> ListPreparedStatementsAsync(
        string workGroup,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var statements = new List<PreparedStatementSummary>();
        var request = new ListPreparedStatementsRequest { WorkGroup = workGroup };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListPreparedStatementsAsync(request);
                statements.AddRange(resp.PreparedStatements);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list Athena prepared statements in workgroup '{workGroup}'");
        }

        return new ListPreparedStatementsResult(PreparedStatements: statements);
    }

    /// <summary>
    /// Update an Athena prepared statement.
    /// </summary>
    public static async Task UpdatePreparedStatementAsync(
        string statementName,
        string workGroup,
        string queryStatement,
        string? description = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdatePreparedStatementRequest
        {
            StatementName = statementName,
            WorkGroup = workGroup,
            QueryStatement = queryStatement
        };
        if (description != null) request.Description = description;

        try
        {
            await client.UpdatePreparedStatementAsync(request);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Athena prepared statement '{statementName}'");
        }
    }

    // ──────────────────────────── Tags ────────────────────────────

    /// <summary>
    /// Add tags to an Athena resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceArn,
        Dictionary<string, string> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new Amazon.Athena.Model.TagResourceRequest
            {
                ResourceARN = resourceArn,
                Tags = tags.Select(kv =>
                    new Amazon.Athena.Model.Tag { Key = kv.Key, Value = kv.Value }).ToList()
            });
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to tag Athena resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from an Athena resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new Amazon.Athena.Model.UntagResourceRequest
            {
                ResourceARN = resourceArn,
                TagKeys = tagKeys
            });
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to untag Athena resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for an Athena resource.
    /// </summary>
    public static async Task<ListAthenaTagsResult> ListTagsForResourceAsync(
        string resourceArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new Amazon.Athena.Model.ListTagsForResourceRequest
                {
                    ResourceARN = resourceArn
                });
            return new ListAthenaTagsResult(Tags: resp.Tags);
        }
        catch (AmazonAthenaException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Athena resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="StartQueryExecutionAsync"/>.</summary>
    public static StartQueryExecutionResult StartQueryExecution(string queryString, string? database = null, string? catalog = null, string? outputLocation = null, string? workGroup = null, RegionEndpoint? region = null)
        => StartQueryExecutionAsync(queryString, database, catalog, outputLocation, workGroup, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopQueryExecutionAsync"/>.</summary>
    public static void StopQueryExecution(string queryExecutionId, RegionEndpoint? region = null)
        => StopQueryExecutionAsync(queryExecutionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetQueryExecutionAsync"/>.</summary>
    public static GetQueryExecutionResult GetQueryExecution(string queryExecutionId, RegionEndpoint? region = null)
        => GetQueryExecutionAsync(queryExecutionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetQueryResultsAsync"/>.</summary>
    public static GetAthenaQueryResultsResult GetQueryResults(string queryExecutionId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetQueryResultsAsync(queryExecutionId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListQueryExecutionsAsync"/>.</summary>
    public static ListQueryExecutionsResult ListQueryExecutions(string? workGroup = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListQueryExecutionsAsync(workGroup, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetQueryExecutionAsync"/>.</summary>
    public static BatchGetQueryExecutionResult BatchGetQueryExecution(List<string> queryExecutionIds, RegionEndpoint? region = null)
        => BatchGetQueryExecutionAsync(queryExecutionIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateWorkGroupAsync"/>.</summary>
    public static CreateWorkGroupResult CreateWorkGroup(string name, WorkGroupConfiguration? configuration = null, string? description = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateWorkGroupAsync(name, configuration, description, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteWorkGroupAsync"/>.</summary>
    public static void DeleteWorkGroup(string workGroup, bool recursiveDeleteOption = false, RegionEndpoint? region = null)
        => DeleteWorkGroupAsync(workGroup, recursiveDeleteOption, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetWorkGroupAsync"/>.</summary>
    public static GetWorkGroupResult GetWorkGroup(string workGroup, RegionEndpoint? region = null)
        => GetWorkGroupAsync(workGroup, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListWorkGroupsAsync"/>.</summary>
    public static ListWorkGroupsResult ListWorkGroups(RegionEndpoint? region = null)
        => ListWorkGroupsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateWorkGroupAsync"/>.</summary>
    public static void UpdateWorkGroup(string workGroup, WorkGroupConfigurationUpdates? configurationUpdates = null, string? description = null, WorkGroupState? state = null, RegionEndpoint? region = null)
        => UpdateWorkGroupAsync(workGroup, configurationUpdates, description, state, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateNamedQueryAsync"/>.</summary>
    public static CreateNamedQueryResult CreateNamedQuery(string name, string database, string queryString, string? description = null, string? workGroup = null, RegionEndpoint? region = null)
        => CreateNamedQueryAsync(name, database, queryString, description, workGroup, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteNamedQueryAsync"/>.</summary>
    public static void DeleteNamedQuery(string namedQueryId, RegionEndpoint? region = null)
        => DeleteNamedQueryAsync(namedQueryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetNamedQueryAsync"/>.</summary>
    public static GetNamedQueryResult GetNamedQuery(string namedQueryId, RegionEndpoint? region = null)
        => GetNamedQueryAsync(namedQueryId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListNamedQueriesAsync"/>.</summary>
    public static ListNamedQueriesResult ListNamedQueries(string? workGroup = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListNamedQueriesAsync(workGroup, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetNamedQueryAsync"/>.</summary>
    public static BatchGetNamedQueryResult BatchGetNamedQuery(List<string> namedQueryIds, RegionEndpoint? region = null)
        => BatchGetNamedQueryAsync(namedQueryIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateDataCatalogAsync"/>.</summary>
    public static CreateDataCatalogResult CreateDataCatalog(string name, DataCatalogType type, string? description = null, Dictionary<string, string>? parameters = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateDataCatalogAsync(name, type, description, parameters, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDataCatalogAsync"/>.</summary>
    public static void DeleteDataCatalog(string name, RegionEndpoint? region = null)
        => DeleteDataCatalogAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDataCatalogAsync"/>.</summary>
    public static GetDataCatalogResult GetDataCatalog(string name, RegionEndpoint? region = null)
        => GetDataCatalogAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDataCatalogsAsync"/>.</summary>
    public static ListDataCatalogsResult ListDataCatalogs(RegionEndpoint? region = null)
        => ListDataCatalogsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateDataCatalogAsync"/>.</summary>
    public static void UpdateDataCatalog(string name, DataCatalogType type, string? description = null, Dictionary<string, string>? parameters = null, RegionEndpoint? region = null)
        => UpdateDataCatalogAsync(name, type, description, parameters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDatabaseAsync"/>.</summary>
    public static GetAthenaDatabaseResult GetDatabase(string catalogName, string databaseName, RegionEndpoint? region = null)
        => GetDatabaseAsync(catalogName, databaseName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDatabasesAsync"/>.</summary>
    public static ListAthenaDatabasesResult ListDatabases(string catalogName, RegionEndpoint? region = null)
        => ListDatabasesAsync(catalogName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetTableMetadataAsync"/>.</summary>
    public static GetTableMetadataResult GetTableMetadata(string catalogName, string databaseName, string tableName, RegionEndpoint? region = null)
        => GetTableMetadataAsync(catalogName, databaseName, tableName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTableMetadataAsync"/>.</summary>
    public static ListTableMetadataResult ListTableMetadata(string catalogName, string databaseName, RegionEndpoint? region = null)
        => ListTableMetadataAsync(catalogName, databaseName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreatePreparedStatementAsync"/>.</summary>
    public static CreatePreparedStatementResult CreatePreparedStatement(string statementName, string workGroup, string queryStatement, string? description = null, RegionEndpoint? region = null)
        => CreatePreparedStatementAsync(statementName, workGroup, queryStatement, description, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeletePreparedStatementAsync"/>.</summary>
    public static void DeletePreparedStatement(string statementName, string workGroup, RegionEndpoint? region = null)
        => DeletePreparedStatementAsync(statementName, workGroup, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetPreparedStatementAsync"/>.</summary>
    public static GetPreparedStatementResult GetPreparedStatement(string statementName, string workGroup, RegionEndpoint? region = null)
        => GetPreparedStatementAsync(statementName, workGroup, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListPreparedStatementsAsync"/>.</summary>
    public static ListPreparedStatementsResult ListPreparedStatements(string workGroup, RegionEndpoint? region = null)
        => ListPreparedStatementsAsync(workGroup, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdatePreparedStatementAsync"/>.</summary>
    public static void UpdatePreparedStatement(string statementName, string workGroup, string queryStatement, string? description = null, RegionEndpoint? region = null)
        => UpdatePreparedStatementAsync(statementName, workGroup, queryStatement, description, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static ListAthenaTagsResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}

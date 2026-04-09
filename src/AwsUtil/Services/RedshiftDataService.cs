using Amazon;
using Amazon.RedshiftDataAPIService;
using Amazon.RedshiftDataAPIService.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record ExecuteRedshiftStatementResult(
    string? Id = null,
    string? Database = null,
    string? ClusterIdentifier = null,
    string? WorkgroupName = null,
    DateTime? CreatedAt = null,
    string? DbUser = null,
    string? SecretArn = null);

public sealed record BatchExecuteRedshiftStatementResult(
    string? Id = null,
    string? Database = null,
    string? ClusterIdentifier = null,
    string? WorkgroupName = null,
    DateTime? CreatedAt = null,
    string? DbUser = null,
    string? SecretArn = null);

public sealed record CancelRedshiftStatementResult(bool? Status = null);

public sealed record DescribeRedshiftStatementResult(
    string? Id = null,
    string? Status = null,
    string? Database = null,
    string? ClusterIdentifier = null,
    string? WorkgroupName = null,
    long? Duration = null,
    string? Error = null,
    bool? HasResultSet = null,
    string? QueryString = null,
    long? ResultRows = null,
    long? ResultSize = null,
    DateTime? CreatedAt = null,
    DateTime? UpdatedAt = null);

public sealed record GetRedshiftStatementResultResult(
    List<List<Field>>? Records = null,
    List<ColumnMetadata>? ColumnMetadata = null,
    long? TotalNumRows = null,
    string? NextToken = null);

public sealed record ListRedshiftStatementsResult(
    List<StatementData>? Statements = null,
    string? NextToken = null);

public sealed record DescribeRedshiftTableResult(
    List<ColumnMetadata>? ColumnList = null,
    string? TableName = null,
    string? NextToken = null);

public sealed record ListRedshiftTablesResult(
    List<TableMember>? Tables = null,
    string? NextToken = null);

public sealed record ListRedshiftSchemasResult(
    List<string>? Schemas = null,
    string? NextToken = null);

public sealed record ListRedshiftDatabasesResult(
    List<string>? Databases = null,
    string? NextToken = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Redshift Data API Service.
/// </summary>
public static class RedshiftDataService
{
    private static AmazonRedshiftDataAPIServiceClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonRedshiftDataAPIServiceClient>(
            region);

    // ──────────────────────── Statements ───────────────────────────────

    /// <summary>
    /// Execute a SQL statement against a Redshift cluster or workgroup.
    /// </summary>
    public static async Task<ExecuteRedshiftStatementResult>
        ExecuteStatementAsync(
            ExecuteStatementRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ExecuteStatementAsync(request);
            return new ExecuteRedshiftStatementResult(
                Id: resp.Id,
                Database: resp.Database,
                ClusterIdentifier: resp.ClusterIdentifier,
                WorkgroupName: resp.WorkgroupName,
                CreatedAt: resp.CreatedAt,
                DbUser: resp.DbUser,
                SecretArn: resp.SecretArn);
        }
        catch (AmazonRedshiftDataAPIServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to execute Redshift Data API statement");
        }
    }

    /// <summary>
    /// Batch-execute SQL statements against a Redshift cluster or workgroup.
    /// </summary>
    public static async Task<BatchExecuteRedshiftStatementResult>
        BatchExecuteStatementAsync(
            BatchExecuteStatementRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchExecuteStatementAsync(request);
            return new BatchExecuteRedshiftStatementResult(
                Id: resp.Id,
                Database: resp.Database,
                ClusterIdentifier: resp.ClusterIdentifier,
                WorkgroupName: resp.WorkgroupName,
                CreatedAt: resp.CreatedAt,
                DbUser: resp.DbUser,
                SecretArn: resp.SecretArn);
        }
        catch (AmazonRedshiftDataAPIServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch-execute Redshift Data API statements");
        }
    }

    /// <summary>
    /// Cancel a running Redshift Data API statement.
    /// </summary>
    public static async Task<CancelRedshiftStatementResult>
        CancelStatementAsync(
            string id,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CancelStatementAsync(
                new CancelStatementRequest { Id = id });
            return new CancelRedshiftStatementResult(Status: resp.Status);
        }
        catch (AmazonRedshiftDataAPIServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to cancel Redshift Data API statement '{id}'");
        }
    }

    /// <summary>
    /// Describe a Redshift Data API statement execution.
    /// </summary>
    public static async Task<DescribeRedshiftStatementResult>
        DescribeStatementAsync(
            string id,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeStatementAsync(
                new DescribeStatementRequest { Id = id });
            return new DescribeRedshiftStatementResult(
                Id: resp.Id,
                Status: resp.Status?.Value,
                Database: resp.Database,
                ClusterIdentifier: resp.ClusterIdentifier,
                WorkgroupName: resp.WorkgroupName,
                Duration: resp.Duration,
                Error: resp.Error,
                HasResultSet: resp.HasResultSet,
                QueryString: resp.QueryString,
                ResultRows: resp.ResultRows,
                ResultSize: resp.ResultSize,
                CreatedAt: resp.CreatedAt,
                UpdatedAt: resp.UpdatedAt);
        }
        catch (AmazonRedshiftDataAPIServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Redshift Data API statement '{id}'");
        }
    }

    /// <summary>
    /// Get the result of a Redshift Data API statement execution.
    /// </summary>
    public static async Task<GetRedshiftStatementResultResult>
        GetStatementResultAsync(
            string id,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetStatementResultRequest { Id = id };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetStatementResultAsync(request);
            return new GetRedshiftStatementResultResult(
                Records: resp.Records,
                ColumnMetadata: resp.ColumnMetadata,
                TotalNumRows: resp.TotalNumRows,
                NextToken: resp.NextToken);
        }
        catch (AmazonRedshiftDataAPIServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get result for Redshift Data API statement '{id}'");
        }
    }

    /// <summary>
    /// List Redshift Data API statements.
    /// </summary>
    public static async Task<ListRedshiftStatementsResult>
        ListStatementsAsync(
            string? status = null,
            string? statementName = null,
            bool? roleLevel = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListStatementsRequest();
        if (status != null)
            request.Status = new StatusString(status);
        if (statementName != null) request.StatementName = statementName;
        if (roleLevel.HasValue) request.RoleLevel = roleLevel.Value;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListStatementsAsync(request);
            return new ListRedshiftStatementsResult(
                Statements: resp.Statements,
                NextToken: resp.NextToken);
        }
        catch (AmazonRedshiftDataAPIServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Redshift Data API statements");
        }
    }

    // ──────────────────────── Metadata ─────────────────────────────────

    /// <summary>
    /// Describe a table in a Redshift database.
    /// </summary>
    public static async Task<DescribeRedshiftTableResult>
        DescribeTableAsync(
            DescribeTableRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeTableAsync(request);
            return new DescribeRedshiftTableResult(
                ColumnList: resp.ColumnList,
                TableName: resp.TableName,
                NextToken: resp.NextToken);
        }
        catch (AmazonRedshiftDataAPIServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Redshift table");
        }
    }

    /// <summary>
    /// List tables in a Redshift database.
    /// </summary>
    public static async Task<ListRedshiftTablesResult> ListTablesAsync(
        ListTablesRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTablesAsync(request);
            return new ListRedshiftTablesResult(
                Tables: resp.Tables,
                NextToken: resp.NextToken);
        }
        catch (AmazonRedshiftDataAPIServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Redshift tables");
        }
    }

    /// <summary>
    /// List schemas in a Redshift database.
    /// </summary>
    public static async Task<ListRedshiftSchemasResult> ListSchemasAsync(
        ListSchemasRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListSchemasAsync(request);
            return new ListRedshiftSchemasResult(
                Schemas: resp.Schemas,
                NextToken: resp.NextToken);
        }
        catch (AmazonRedshiftDataAPIServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Redshift schemas");
        }
    }

    /// <summary>
    /// List databases in a Redshift cluster or workgroup.
    /// </summary>
    public static async Task<ListRedshiftDatabasesResult>
        ListDatabasesAsync(
            ListDatabasesRequest request,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListDatabasesAsync(request);
            return new ListRedshiftDatabasesResult(
                Databases: resp.Databases,
                NextToken: resp.NextToken);
        }
        catch (AmazonRedshiftDataAPIServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list Redshift databases");
        }
    }
}

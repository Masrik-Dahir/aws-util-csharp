using Amazon;
using Amazon.RDSDataService;
using Amazon.RDSDataService.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record RdsDataExecuteStatementResult(
    List<List<Field>>? Records = null,
    List<ColumnMetadata>? ColumnMetadata = null,
    long? NumberOfRecordsUpdated = null,
    string? GeneratedFields = null,
    string? FormattedRecords = null);

public sealed record RdsDataBatchExecuteStatementResult(
    List<UpdateResult>? UpdateResults = null);

public sealed record RdsDataBeginTransactionResult(
    string? TransactionId = null);

public sealed record RdsDataCommitTransactionResult(
    string? TransactionStatus = null);

public sealed record RdsDataRollbackTransactionResult(
    string? TransactionStatus = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon RDS Data API.
/// </summary>
public static class RdsDataService
{
    private static AmazonRDSDataServiceClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonRDSDataServiceClient>(region);

    /// <summary>
    /// Execute a SQL statement against an Aurora Serverless DB cluster.
    /// </summary>
    public static async Task<RdsDataExecuteStatementResult>
        ExecuteStatementAsync(
            string resourceArn,
            string secretArn,
            string sql,
            string? database = null,
            string? schema = null,
            List<SqlParameter>? parameters = null,
            string? transactionId = null,
            bool? includeResultMetadata = null,
            bool? continueAfterTimeout = null,
            ResultSetOptions? resultSetOptions = null,
            string? formatRecordsAs = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ExecuteStatementRequest
        {
            ResourceArn = resourceArn,
            SecretArn = secretArn,
            Sql = sql
        };
        if (database != null) request.Database = database;
        if (schema != null) request.Schema = schema;
        if (parameters != null) request.Parameters = parameters;
        if (transactionId != null) request.TransactionId = transactionId;
        if (includeResultMetadata.HasValue)
            request.IncludeResultMetadata = includeResultMetadata.Value;
        if (continueAfterTimeout.HasValue)
            request.ContinueAfterTimeout = continueAfterTimeout.Value;
        if (resultSetOptions != null) request.ResultSetOptions = resultSetOptions;
        if (formatRecordsAs != null) request.FormatRecordsAs = formatRecordsAs;

        try
        {
            var resp = await client.ExecuteStatementAsync(request);
            return new RdsDataExecuteStatementResult(
                Records: resp.Records,
                ColumnMetadata: resp.ColumnMetadata,
                NumberOfRecordsUpdated: resp.NumberOfRecordsUpdated,
                FormattedRecords: resp.FormattedRecords);
        }
        catch (AmazonRDSDataServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to execute RDS Data statement");
        }
    }

    /// <summary>
    /// Execute a batch of SQL statements against an Aurora Serverless DB cluster.
    /// </summary>
    public static async Task<RdsDataBatchExecuteStatementResult>
        BatchExecuteStatementAsync(
            string resourceArn,
            string secretArn,
            string sql,
            string? database = null,
            string? schema = null,
            List<List<SqlParameter>>? parameterSets = null,
            string? transactionId = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchExecuteStatementRequest
        {
            ResourceArn = resourceArn,
            SecretArn = secretArn,
            Sql = sql
        };
        if (database != null) request.Database = database;
        if (schema != null) request.Schema = schema;
        if (parameterSets != null) request.ParameterSets = parameterSets;
        if (transactionId != null) request.TransactionId = transactionId;

        try
        {
            var resp = await client.BatchExecuteStatementAsync(request);
            return new RdsDataBatchExecuteStatementResult(
                UpdateResults: resp.UpdateResults);
        }
        catch (AmazonRDSDataServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch execute RDS Data statements");
        }
    }

    /// <summary>
    /// Begin a SQL transaction on an Aurora Serverless DB cluster.
    /// </summary>
    public static async Task<RdsDataBeginTransactionResult>
        BeginTransactionAsync(
            string resourceArn,
            string secretArn,
            string? database = null,
            string? schema = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BeginTransactionRequest
        {
            ResourceArn = resourceArn,
            SecretArn = secretArn
        };
        if (database != null) request.Database = database;
        if (schema != null) request.Schema = schema;

        try
        {
            var resp = await client.BeginTransactionAsync(request);
            return new RdsDataBeginTransactionResult(
                TransactionId: resp.TransactionId);
        }
        catch (AmazonRDSDataServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to begin RDS Data transaction");
        }
    }

    /// <summary>
    /// Commit a SQL transaction on an Aurora Serverless DB cluster.
    /// </summary>
    public static async Task<RdsDataCommitTransactionResult>
        CommitTransactionAsync(
            string resourceArn,
            string secretArn,
            string transactionId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CommitTransactionAsync(
                new CommitTransactionRequest
                {
                    ResourceArn = resourceArn,
                    SecretArn = secretArn,
                    TransactionId = transactionId
                });
            return new RdsDataCommitTransactionResult(
                TransactionStatus: resp.TransactionStatus);
        }
        catch (AmazonRDSDataServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to commit RDS Data transaction '{transactionId}'");
        }
    }

    /// <summary>
    /// Roll back a SQL transaction on an Aurora Serverless DB cluster.
    /// </summary>
    public static async Task<RdsDataRollbackTransactionResult>
        RollbackTransactionAsync(
            string resourceArn,
            string secretArn,
            string transactionId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RollbackTransactionAsync(
                new RollbackTransactionRequest
                {
                    ResourceArn = resourceArn,
                    SecretArn = secretArn,
                    TransactionId = transactionId
                });
            return new RdsDataRollbackTransactionResult(
                TransactionStatus: resp.TransactionStatus);
        }
        catch (AmazonRDSDataServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to rollback RDS Data transaction '{transactionId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="ExecuteStatementAsync"/>.</summary>
    public static RdsDataExecuteStatementResult ExecuteStatement(string resourceArn, string secretArn, string sql, string? database = null, string? schema = null, List<SqlParameter>? parameters = null, string? transactionId = null, bool? includeResultMetadata = null, bool? continueAfterTimeout = null, ResultSetOptions? resultSetOptions = null, string? formatRecordsAs = null, RegionEndpoint? region = null)
        => ExecuteStatementAsync(resourceArn, secretArn, sql, database, schema, parameters, transactionId, includeResultMetadata, continueAfterTimeout, resultSetOptions, formatRecordsAs, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchExecuteStatementAsync"/>.</summary>
    public static RdsDataBatchExecuteStatementResult BatchExecuteStatement(string resourceArn, string secretArn, string sql, string? database = null, string? schema = null, List<List<SqlParameter>>? parameterSets = null, string? transactionId = null, RegionEndpoint? region = null)
        => BatchExecuteStatementAsync(resourceArn, secretArn, sql, database, schema, parameterSets, transactionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BeginTransactionAsync"/>.</summary>
    public static RdsDataBeginTransactionResult BeginTransaction(string resourceArn, string secretArn, string? database = null, string? schema = null, RegionEndpoint? region = null)
        => BeginTransactionAsync(resourceArn, secretArn, database, schema, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CommitTransactionAsync"/>.</summary>
    public static RdsDataCommitTransactionResult CommitTransaction(string resourceArn, string secretArn, string transactionId, RegionEndpoint? region = null)
        => CommitTransactionAsync(resourceArn, secretArn, transactionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RollbackTransactionAsync"/>.</summary>
    public static RdsDataRollbackTransactionResult RollbackTransaction(string resourceArn, string secretArn, string transactionId, RegionEndpoint? region = null)
        => RollbackTransactionAsync(resourceArn, secretArn, transactionId, region).GetAwaiter().GetResult();

}

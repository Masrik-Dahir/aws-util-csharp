using Amazon;
using Amazon.TimestreamWrite;
using Amazon.TimestreamWrite.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record TswCreateDatabaseResult(
    string? DatabaseName = null,
    string? Arn = null,
    string? KmsKeyId = null);

public sealed record TswDeleteDatabaseResult(bool Success = true);

public sealed record TswDescribeDatabaseResult(
    string? DatabaseName = null,
    string? Arn = null,
    string? KmsKeyId = null,
    long? TableCount = null);

public sealed record TswListDatabasesResult(
    List<Database>? Databases = null,
    string? NextToken = null);

public sealed record TswUpdateDatabaseResult(
    string? DatabaseName = null,
    string? Arn = null,
    string? KmsKeyId = null);

public sealed record TswCreateTableResult(
    string? TableName = null,
    string? DatabaseName = null,
    string? Arn = null,
    string? TableStatus = null);

public sealed record TswDeleteTableResult(bool Success = true);

public sealed record TswDescribeTableResult(
    string? TableName = null,
    string? DatabaseName = null,
    string? Arn = null,
    string? TableStatus = null,
    RetentionProperties? RetentionProperties = null);

public sealed record TswListTablesResult(
    List<Table>? Tables = null,
    string? NextToken = null);

public sealed record TswUpdateTableResult(
    string? TableName = null,
    string? DatabaseName = null,
    string? Arn = null,
    string? TableStatus = null);

public sealed record TswWriteRecordsResult(
    int? RecordsIngested = null);

public sealed record TswCreateBatchLoadTaskResult(
    string? TaskId = null);

public sealed record TswDescribeBatchLoadTaskResult(
    BatchLoadTaskDescription? BatchLoadTaskDescription = null);

public sealed record TswListBatchLoadTasksResult(
    List<BatchLoadTask>? BatchLoadTasks = null,
    string? NextToken = null);

public sealed record TswResumeBatchLoadTaskResult(bool Success = true);

public sealed record TswTagResourceResult(bool Success = true);
public sealed record TswUntagResourceResult(bool Success = true);

public sealed record TswListTagsForResourceResult(
    List<Tag>? Tags = null);

public sealed record TswDescribeEndpointsResult(
    List<Endpoint>? Endpoints = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon Timestream Write.
/// </summary>
public static class TimestreamWriteService
{
    private static AmazonTimestreamWriteClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonTimestreamWriteClient>(region);

    /// <summary>
    /// Create a new Timestream database.
    /// </summary>
    public static async Task<TswCreateDatabaseResult> CreateDatabaseAsync(
        string databaseName,
        string? kmsKeyId = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDatabaseRequest { DatabaseName = databaseName };
        if (kmsKeyId != null) request.KmsKeyId = kmsKeyId;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDatabaseAsync(request);
            return new TswCreateDatabaseResult(
                DatabaseName: resp.Database?.DatabaseName,
                Arn: resp.Database?.Arn,
                KmsKeyId: resp.Database?.KmsKeyId);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Timestream database '{databaseName}'");
        }
    }

    /// <summary>
    /// Delete a Timestream database.
    /// </summary>
    public static async Task<TswDeleteDatabaseResult> DeleteDatabaseAsync(
        string databaseName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDatabaseAsync(
                new DeleteDatabaseRequest { DatabaseName = databaseName });
            return new TswDeleteDatabaseResult();
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Timestream database '{databaseName}'");
        }
    }

    /// <summary>
    /// Describe a Timestream database.
    /// </summary>
    public static async Task<TswDescribeDatabaseResult> DescribeDatabaseAsync(
        string databaseName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeDatabaseAsync(
                new DescribeDatabaseRequest { DatabaseName = databaseName });
            return new TswDescribeDatabaseResult(
                DatabaseName: resp.Database?.DatabaseName,
                Arn: resp.Database?.Arn,
                KmsKeyId: resp.Database?.KmsKeyId,
                TableCount: resp.Database?.TableCount);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Timestream database '{databaseName}'");
        }
    }

    /// <summary>
    /// List Timestream databases.
    /// </summary>
    public static async Task<TswListDatabasesResult> ListDatabasesAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDatabasesRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListDatabasesAsync(request);
            return new TswListDatabasesResult(
                Databases: resp.Databases,
                NextToken: resp.NextToken);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list Timestream databases");
        }
    }

    /// <summary>
    /// Update a Timestream database.
    /// </summary>
    public static async Task<TswUpdateDatabaseResult> UpdateDatabaseAsync(
        string databaseName,
        string kmsKeyId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateDatabaseRequest
        {
            DatabaseName = databaseName,
            KmsKeyId = kmsKeyId
        };

        try
        {
            var resp = await client.UpdateDatabaseAsync(request);
            return new TswUpdateDatabaseResult(
                DatabaseName: resp.Database?.DatabaseName,
                Arn: resp.Database?.Arn,
                KmsKeyId: resp.Database?.KmsKeyId);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Timestream database '{databaseName}'");
        }
    }

    /// <summary>
    /// Create a new Timestream table.
    /// </summary>
    public static async Task<TswCreateTableResult> CreateTableAsync(
        string databaseName,
        string tableName,
        RetentionProperties? retentionProperties = null,
        MagneticStoreWriteProperties? magneticStoreWriteProperties = null,
        Schema? schema = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateTableRequest
        {
            DatabaseName = databaseName,
            TableName = tableName
        };
        if (retentionProperties != null) request.RetentionProperties = retentionProperties;
        if (magneticStoreWriteProperties != null)
            request.MagneticStoreWriteProperties = magneticStoreWriteProperties;
        if (schema != null) request.Schema = schema;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateTableAsync(request);
            return new TswCreateTableResult(
                TableName: resp.Table?.TableName,
                DatabaseName: resp.Table?.DatabaseName,
                Arn: resp.Table?.Arn,
                TableStatus: resp.Table?.TableStatus?.Value);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Timestream table '{tableName}'");
        }
    }

    /// <summary>
    /// Delete a Timestream table.
    /// </summary>
    public static async Task<TswDeleteTableResult> DeleteTableAsync(
        string databaseName,
        string tableName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTableAsync(new DeleteTableRequest
            {
                DatabaseName = databaseName,
                TableName = tableName
            });
            return new TswDeleteTableResult();
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Timestream table '{tableName}'");
        }
    }

    /// <summary>
    /// Describe a Timestream table.
    /// </summary>
    public static async Task<TswDescribeTableResult> DescribeTableAsync(
        string databaseName,
        string tableName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeTableAsync(new DescribeTableRequest
            {
                DatabaseName = databaseName,
                TableName = tableName
            });
            return new TswDescribeTableResult(
                TableName: resp.Table?.TableName,
                DatabaseName: resp.Table?.DatabaseName,
                Arn: resp.Table?.Arn,
                TableStatus: resp.Table?.TableStatus?.Value,
                RetentionProperties: resp.Table?.RetentionProperties);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Timestream table '{tableName}'");
        }
    }

    /// <summary>
    /// List Timestream tables in a database.
    /// </summary>
    public static async Task<TswListTablesResult> ListTablesAsync(
        string databaseName,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTablesRequest { DatabaseName = databaseName };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListTablesAsync(request);
            return new TswListTablesResult(
                Tables: resp.Tables,
                NextToken: resp.NextToken);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list Timestream tables in '{databaseName}'");
        }
    }

    /// <summary>
    /// Update a Timestream table.
    /// </summary>
    public static async Task<TswUpdateTableResult> UpdateTableAsync(
        string databaseName,
        string tableName,
        RetentionProperties? retentionProperties = null,
        MagneticStoreWriteProperties? magneticStoreWriteProperties = null,
        Schema? schema = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateTableRequest
        {
            DatabaseName = databaseName,
            TableName = tableName
        };
        if (retentionProperties != null) request.RetentionProperties = retentionProperties;
        if (magneticStoreWriteProperties != null)
            request.MagneticStoreWriteProperties = magneticStoreWriteProperties;
        if (schema != null) request.Schema = schema;

        try
        {
            var resp = await client.UpdateTableAsync(request);
            return new TswUpdateTableResult(
                TableName: resp.Table?.TableName,
                DatabaseName: resp.Table?.DatabaseName,
                Arn: resp.Table?.Arn,
                TableStatus: resp.Table?.TableStatus?.Value);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update Timestream table '{tableName}'");
        }
    }

    /// <summary>
    /// Write records to a Timestream table.
    /// </summary>
    public static async Task<TswWriteRecordsResult> WriteRecordsAsync(
        string databaseName,
        string tableName,
        List<Record> records,
        Record? commonAttributes = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new WriteRecordsRequest
        {
            DatabaseName = databaseName,
            TableName = tableName,
            Records = records
        };
        if (commonAttributes != null) request.CommonAttributes = commonAttributes;

        try
        {
            var resp = await client.WriteRecordsAsync(request);
            return new TswWriteRecordsResult(
                RecordsIngested: resp.RecordsIngested?.Total);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to write records to '{databaseName}/{tableName}'");
        }
    }

    /// <summary>
    /// Create a batch load task.
    /// </summary>
    public static async Task<TswCreateBatchLoadTaskResult> CreateBatchLoadTaskAsync(
        CreateBatchLoadTaskRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateBatchLoadTaskAsync(request);
            return new TswCreateBatchLoadTaskResult(TaskId: resp.TaskId);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create batch load task");
        }
    }

    /// <summary>
    /// Describe a batch load task.
    /// </summary>
    public static async Task<TswDescribeBatchLoadTaskResult> DescribeBatchLoadTaskAsync(
        string taskId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeBatchLoadTaskAsync(
                new DescribeBatchLoadTaskRequest { TaskId = taskId });
            return new TswDescribeBatchLoadTaskResult(
                BatchLoadTaskDescription: resp.BatchLoadTaskDescription);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe batch load task '{taskId}'");
        }
    }

    /// <summary>
    /// List batch load tasks.
    /// </summary>
    public static async Task<TswListBatchLoadTasksResult> ListBatchLoadTasksAsync(
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListBatchLoadTasksRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListBatchLoadTasksAsync(request);
            return new TswListBatchLoadTasksResult(
                BatchLoadTasks: resp.BatchLoadTasks,
                NextToken: resp.NextToken);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list batch load tasks");
        }
    }

    /// <summary>
    /// Resume a batch load task.
    /// </summary>
    public static async Task<TswResumeBatchLoadTaskResult> ResumeBatchLoadTaskAsync(
        string taskId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ResumeBatchLoadTaskAsync(
                new ResumeBatchLoadTaskRequest { TaskId = taskId });
            return new TswResumeBatchLoadTaskResult();
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to resume batch load task '{taskId}'");
        }
    }

    /// <summary>
    /// Tag a Timestream resource.
    /// </summary>
    public static async Task<TswTagResourceResult> TagResourceAsync(
        string resourceArn,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                ResourceARN = resourceArn,
                Tags = tags
            });
            return new TswTagResourceResult();
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Timestream resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Timestream resource.
    /// </summary>
    public static async Task<TswUntagResourceResult> UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceARN = resourceArn,
                TagKeys = tagKeys
            });
            return new TswUntagResourceResult();
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Timestream resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Timestream resource.
    /// </summary>
    public static async Task<TswListTagsForResourceResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceARN = resourceArn });
            return new TswListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Timestream resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Describe Timestream write endpoints.
    /// </summary>
    public static async Task<TswDescribeEndpointsResult> DescribeEndpointsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeEndpointsAsync(
                new DescribeEndpointsRequest());
            return new TswDescribeEndpointsResult(Endpoints: resp.Endpoints);
        }
        catch (AmazonTimestreamWriteException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe Timestream write endpoints");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateDatabaseAsync"/>.</summary>
    public static TswCreateDatabaseResult CreateDatabase(string databaseName, string? kmsKeyId = null, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateDatabaseAsync(databaseName, kmsKeyId, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDatabaseAsync"/>.</summary>
    public static TswDeleteDatabaseResult DeleteDatabase(string databaseName, RegionEndpoint? region = null)
        => DeleteDatabaseAsync(databaseName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDatabaseAsync"/>.</summary>
    public static TswDescribeDatabaseResult DescribeDatabase(string databaseName, RegionEndpoint? region = null)
        => DescribeDatabaseAsync(databaseName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDatabasesAsync"/>.</summary>
    public static TswListDatabasesResult ListDatabases(string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListDatabasesAsync(nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateDatabaseAsync"/>.</summary>
    public static TswUpdateDatabaseResult UpdateDatabase(string databaseName, string kmsKeyId, RegionEndpoint? region = null)
        => UpdateDatabaseAsync(databaseName, kmsKeyId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateTableAsync"/>.</summary>
    public static TswCreateTableResult CreateTable(string databaseName, string tableName, RetentionProperties? retentionProperties = null, MagneticStoreWriteProperties? magneticStoreWriteProperties = null, Schema? schema = null, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateTableAsync(databaseName, tableName, retentionProperties, magneticStoreWriteProperties, schema, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteTableAsync"/>.</summary>
    public static TswDeleteTableResult DeleteTable(string databaseName, string tableName, RegionEndpoint? region = null)
        => DeleteTableAsync(databaseName, tableName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeTableAsync"/>.</summary>
    public static TswDescribeTableResult DescribeTable(string databaseName, string tableName, RegionEndpoint? region = null)
        => DescribeTableAsync(databaseName, tableName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTablesAsync"/>.</summary>
    public static TswListTablesResult ListTables(string databaseName, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListTablesAsync(databaseName, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateTableAsync"/>.</summary>
    public static TswUpdateTableResult UpdateTable(string databaseName, string tableName, RetentionProperties? retentionProperties = null, MagneticStoreWriteProperties? magneticStoreWriteProperties = null, Schema? schema = null, RegionEndpoint? region = null)
        => UpdateTableAsync(databaseName, tableName, retentionProperties, magneticStoreWriteProperties, schema, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="WriteRecordsAsync"/>.</summary>
    public static TswWriteRecordsResult WriteRecords(string databaseName, string tableName, List<Record> records, Record? commonAttributes = null, RegionEndpoint? region = null)
        => WriteRecordsAsync(databaseName, tableName, records, commonAttributes, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateBatchLoadTaskAsync"/>.</summary>
    public static TswCreateBatchLoadTaskResult CreateBatchLoadTask(CreateBatchLoadTaskRequest request, RegionEndpoint? region = null)
        => CreateBatchLoadTaskAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeBatchLoadTaskAsync"/>.</summary>
    public static TswDescribeBatchLoadTaskResult DescribeBatchLoadTask(string taskId, RegionEndpoint? region = null)
        => DescribeBatchLoadTaskAsync(taskId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListBatchLoadTasksAsync"/>.</summary>
    public static TswListBatchLoadTasksResult ListBatchLoadTasks(string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListBatchLoadTasksAsync(nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ResumeBatchLoadTaskAsync"/>.</summary>
    public static TswResumeBatchLoadTaskResult ResumeBatchLoadTask(string taskId, RegionEndpoint? region = null)
        => ResumeBatchLoadTaskAsync(taskId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static TswTagResourceResult TagResource(string resourceArn, List<Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static TswUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static TswListTagsForResourceResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeEndpointsAsync"/>.</summary>
    public static TswDescribeEndpointsResult DescribeEndpoints(RegionEndpoint? region = null)
        => DescribeEndpointsAsync(region).GetAwaiter().GetResult();

}

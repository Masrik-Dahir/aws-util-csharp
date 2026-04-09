using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result record types
// ---------------------------------------------------------------------------

/// <summary>Result of a BatchExecuteStatement operation.</summary>
public sealed record BatchExecuteStatementResult(
    List<BatchStatementResponse>? Responses = null,
    List<ConsumedCapacity>? ConsumedCapacity = null);

/// <summary>Result of a BatchGetItem operation.</summary>
public sealed record BatchGetItemResult(
    Dictionary<string, List<Dictionary<string, AttributeValue>>>? Responses = null,
    Dictionary<string, KeysAndAttributes>? UnprocessedKeys = null,
    List<ConsumedCapacity>? ConsumedCapacity = null);

/// <summary>Result of a BatchWriteItem operation.</summary>
public sealed record BatchWriteItemResult(
    Dictionary<string, List<WriteRequest>>? UnprocessedItems = null,
    Dictionary<string, List<ItemCollectionMetrics>>? ItemCollectionMetrics = null,
    List<ConsumedCapacity>? ConsumedCapacity = null);

/// <summary>Result of a CreateBackup operation.</summary>
public sealed record CreateBackupResult(BackupDetails? BackupDetails = null);

/// <summary>Result of a CreateGlobalTable operation.</summary>
public sealed record CreateGlobalTableResult(GlobalTableDescription? GlobalTableDescription = null);

/// <summary>Result of a CreateTable operation.</summary>
public sealed record CreateTableResult(TableDescription? TableDescription = null);

/// <summary>Result of a DeleteBackup operation.</summary>
public sealed record DeleteBackupResult(BackupDescription? BackupDescription = null);

/// <summary>Result of a DeleteTable operation.</summary>
public sealed record DeleteTableResult(TableDescription? TableDescription = null);

/// <summary>Result of a DescribeBackup operation.</summary>
public sealed record DescribeBackupResult(BackupDescription? BackupDescription = null);

/// <summary>Result of a DescribeContinuousBackups operation.</summary>
public sealed record DescribeContinuousBackupsResult(
    ContinuousBackupsDescription? ContinuousBackupsDescription = null);

/// <summary>Result of a DescribeExport operation.</summary>
public sealed record DescribeExportResult(ExportDescription? ExportDescription = null);

/// <summary>Result of a DescribeGlobalTable operation.</summary>
public sealed record DescribeGlobalTableResult(GlobalTableDescription? GlobalTableDescription = null);

/// <summary>Result of a DescribeImport operation.</summary>
public sealed record DescribeImportResult(ImportTableDescription? ImportTableDescription = null);

/// <summary>Result of a DescribeLimits operation.</summary>
public sealed record DescribeLimitsResult(
    long? AccountMaxReadCapacityUnits = null,
    long? AccountMaxWriteCapacityUnits = null,
    long? TableMaxReadCapacityUnits = null,
    long? TableMaxWriteCapacityUnits = null);

/// <summary>Result of a DescribeTable operation.</summary>
public sealed record DescribeTableResult(TableDescription? Table = null);

/// <summary>Result of a DescribeTimeToLive operation.</summary>
public sealed record DescribeTimeToLiveResult(TimeToLiveDescription? TimeToLiveDescription = null);

/// <summary>Result of a DisableKinesisStreamingDestination operation.</summary>
public sealed record DisableKinesisStreamingDestinationResult(
    string? TableName = null,
    string? StreamArn = null,
    string? DestinationStatus = null);

/// <summary>Result of an EnableKinesisStreamingDestination operation.</summary>
public sealed record EnableKinesisStreamingDestinationResult(
    string? TableName = null,
    string? StreamArn = null,
    string? DestinationStatus = null);

/// <summary>Result of an ExecuteStatement (PartiQL) operation.</summary>
public sealed record ExecuteStatementResult(
    List<Dictionary<string, AttributeValue>>? Items = null,
    string? NextToken = null,
    ConsumedCapacity? ConsumedCapacity = null,
    Dictionary<string, AttributeValue>? LastEvaluatedKey = null);

/// <summary>Result of an ExecuteTransaction (PartiQL) operation.</summary>
public sealed record ExecuteTransactionResult(
    List<ItemResponse>? Responses = null,
    List<ConsumedCapacity>? ConsumedCapacity = null);

/// <summary>Result of an ExportTableToPointInTime operation.</summary>
public sealed record ExportTableToPointInTimeResult(ExportDescription? ExportDescription = null);

/// <summary>Result of an ImportTable operation.</summary>
public sealed record ImportTableResult(ImportTableDescription? ImportTableDescription = null);

/// <summary>Result of a ListBackups operation.</summary>
public sealed record ListBackupsResult(
    List<BackupSummary>? BackupSummaries = null,
    string? LastEvaluatedBackupArn = null);

/// <summary>Result of a DynamoDB ListExports operation.</summary>
public sealed record DynamoDbListExportsResult(
    List<ExportSummary>? ExportSummaries = null,
    string? NextToken = null);

/// <summary>Result of a ListGlobalTables operation.</summary>
public sealed record ListGlobalTablesResult(
    List<GlobalTable>? GlobalTables = null,
    string? LastEvaluatedGlobalTableName = null);

/// <summary>Result of a DynamoDB ListImports operation.</summary>
public sealed record DynamoDbListImportsResult(
    List<ImportSummary>? ImportSummaryList = null,
    string? NextToken = null);

/// <summary>Result of a ListTables operation.</summary>
public sealed record ListTablesResult(
    List<string>? TableNames = null,
    string? LastEvaluatedTableName = null);

/// <summary>Result of a ListTagsOfResource operation.</summary>
public sealed record ListTagsOfResourceResult(
    List<Tag>? Tags = null,
    string? NextToken = null);

/// <summary>Result of a TransactGetItems operation.</summary>
public sealed record TransactGetItemsResult(
    List<ConsumedCapacity>? ConsumedCapacity = null,
    List<ItemResponse>? Responses = null);

/// <summary>Result of a TransactWriteItems operation.</summary>
public sealed record TransactWriteItemsResult(
    List<ConsumedCapacity>? ConsumedCapacity = null,
    Dictionary<string, List<ItemCollectionMetrics>>? ItemCollectionMetrics = null);

/// <summary>Result of an UpdateContinuousBackups operation.</summary>
public sealed record UpdateContinuousBackupsResult(
    ContinuousBackupsDescription? ContinuousBackupsDescription = null);

/// <summary>Result of an UpdateGlobalTable operation.</summary>
public sealed record UpdateGlobalTableResult(GlobalTableDescription? GlobalTableDescription = null);

/// <summary>Result of an UpdateTable operation.</summary>
public sealed record UpdateTableResult(TableDescription? TableDescription = null);

/// <summary>Result of an UpdateTimeToLive operation.</summary>
public sealed record UpdateTimeToLiveResult(
    TimeToLiveSpecification? TimeToLiveSpecification = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon DynamoDB.
/// Port of the Python aws_util.dynamodb module.
/// </summary>
public static class DynamoDbService
{
    private static AmazonDynamoDBClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonDynamoDBClient>(region);

    // -----------------------------------------------------------------------
    // Single-item CRUD
    // -----------------------------------------------------------------------

    /// <summary>
    /// Fetch a single item by its primary key.
    /// Returns null if the item does not exist.
    /// </summary>
    public static async Task<Dictionary<string, AttributeValue>?> GetItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> key,
        bool consistentRead = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetItemRequest
        {
            TableName = tableName,
            Key = key,
            ConsistentRead = consistentRead
        };

        try
        {
            var resp = await client.GetItemAsync(request);
            return resp.Item is { Count: > 0 } ? resp.Item : null;
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"GetItem failed on '{tableName}'");
        }
    }

    /// <summary>
    /// Write (create or overwrite) an item in a DynamoDB table.
    /// </summary>
    public static async Task PutItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> item,
        string? conditionExpression = null,
        Dictionary<string, string>? expressionAttributeNames = null,
        Dictionary<string, AttributeValue>? expressionAttributeValues = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutItemRequest
        {
            TableName = tableName,
            Item = item
        };
        if (conditionExpression != null)
            request.ConditionExpression = conditionExpression;
        if (expressionAttributeNames != null)
            request.ExpressionAttributeNames = expressionAttributeNames;
        if (expressionAttributeValues != null)
            request.ExpressionAttributeValues = expressionAttributeValues;

        try
        {
            await client.PutItemAsync(request);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"PutItem failed on '{tableName}'");
        }
    }

    /// <summary>
    /// Delete an item by its primary key.
    /// </summary>
    public static async Task DeleteItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> key,
        string? conditionExpression = null,
        Dictionary<string, string>? expressionAttributeNames = null,
        Dictionary<string, AttributeValue>? expressionAttributeValues = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteItemRequest
        {
            TableName = tableName,
            Key = key
        };
        if (conditionExpression != null)
            request.ConditionExpression = conditionExpression;
        if (expressionAttributeNames != null)
            request.ExpressionAttributeNames = expressionAttributeNames;
        if (expressionAttributeValues != null)
            request.ExpressionAttributeValues = expressionAttributeValues;

        try
        {
            await client.DeleteItemAsync(request);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"DeleteItem failed on '{tableName}'");
        }
    }

    /// <summary>
    /// Update an item using a raw DynamoDB update expression.
    /// Returns the updated attributes (shape depends on <paramref name="returnValues"/>).
    /// </summary>
    public static async Task<Dictionary<string, AttributeValue>> UpdateItemAsync(
        string tableName,
        Dictionary<string, AttributeValue> key,
        string updateExpression,
        Dictionary<string, string>? expressionAttributeNames = null,
        Dictionary<string, AttributeValue>? expressionAttributeValues = null,
        string? conditionExpression = null,
        ReturnValue? returnValues = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateItemRequest
        {
            TableName = tableName,
            Key = key,
            UpdateExpression = updateExpression,
            ReturnValues = returnValues ?? ReturnValue.ALL_NEW
        };
        if (expressionAttributeNames != null)
            request.ExpressionAttributeNames = expressionAttributeNames;
        if (expressionAttributeValues != null)
            request.ExpressionAttributeValues = expressionAttributeValues;
        if (conditionExpression != null)
            request.ConditionExpression = conditionExpression;

        try
        {
            var resp = await client.UpdateItemAsync(request);
            return resp.Attributes ?? new Dictionary<string, AttributeValue>();
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"UpdateItem failed on '{tableName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Query / Scan
    // -----------------------------------------------------------------------

    /// <summary>
    /// Query a table or GSI using a key condition expression.
    /// Handles pagination automatically unless <paramref name="limit"/> is set.
    /// </summary>
    public static async Task<List<Dictionary<string, AttributeValue>>> QueryAsync(
        string tableName,
        string keyConditionExpression,
        Dictionary<string, string>? expressionAttributeNames = null,
        Dictionary<string, AttributeValue>? expressionAttributeValues = null,
        string? filterExpression = null,
        string? indexName = null,
        int? limit = null,
        bool scanIndexForward = true,
        bool consistentRead = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new QueryRequest
        {
            TableName = tableName,
            KeyConditionExpression = keyConditionExpression,
            ScanIndexForward = scanIndexForward,
            ConsistentRead = consistentRead
        };
        if (expressionAttributeNames != null)
            request.ExpressionAttributeNames = expressionAttributeNames;
        if (expressionAttributeValues != null)
            request.ExpressionAttributeValues = expressionAttributeValues;
        if (filterExpression != null)
            request.FilterExpression = filterExpression;
        if (indexName != null)
            request.IndexName = indexName;

        var items = new List<Dictionary<string, AttributeValue>>();
        try
        {
            do
            {
                var resp = await client.QueryAsync(request);
                items.AddRange(resp.Items);
                if (limit.HasValue && items.Count >= limit.Value)
                    return items.GetRange(0, limit.Value);
                request.ExclusiveStartKey = resp.LastEvaluatedKey;
            } while (request.ExclusiveStartKey is { Count: > 0 });
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Query failed on '{tableName}'");
        }
        return items;
    }

    /// <summary>
    /// Scan an entire table or GSI, optionally filtered.
    /// Full-table scans are expensive on large tables; prefer <see cref="QueryAsync"/> where possible.
    /// </summary>
    public static async Task<List<Dictionary<string, AttributeValue>>> ScanAsync(
        string tableName,
        string? filterExpression = null,
        Dictionary<string, string>? expressionAttributeNames = null,
        Dictionary<string, AttributeValue>? expressionAttributeValues = null,
        string? indexName = null,
        int? limit = null,
        bool consistentRead = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ScanRequest
        {
            TableName = tableName,
            ConsistentRead = consistentRead
        };
        if (filterExpression != null)
            request.FilterExpression = filterExpression;
        if (expressionAttributeNames != null)
            request.ExpressionAttributeNames = expressionAttributeNames;
        if (expressionAttributeValues != null)
            request.ExpressionAttributeValues = expressionAttributeValues;
        if (indexName != null)
            request.IndexName = indexName;

        var items = new List<Dictionary<string, AttributeValue>>();
        try
        {
            do
            {
                var resp = await client.ScanAsync(request);
                items.AddRange(resp.Items);
                if (limit.HasValue && items.Count >= limit.Value)
                    return items.GetRange(0, limit.Value);
                request.ExclusiveStartKey = resp.LastEvaluatedKey;
            } while (request.ExclusiveStartKey is { Count: > 0 });
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Scan failed on '{tableName}'");
        }
        return items;
    }

    // -----------------------------------------------------------------------
    // Batch operations (high-level)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Write items in batch (put only). Automatically retries unprocessed items.
    /// Items are batched into groups of 25 per DynamoDB limits.
    /// </summary>
    public static async Task BatchWriteAsync(
        string tableName,
        List<Dictionary<string, AttributeValue>> items,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            for (var i = 0; i < items.Count; i += 25)
            {
                var batch = items.GetRange(i, Math.Min(25, items.Count - i));
                var writeRequests = batch.Select(item =>
                    new WriteRequest(new PutRequest { Item = item })).ToList();
                var request = new BatchWriteItemRequest
                {
                    RequestItems = new Dictionary<string, List<WriteRequest>>
                    {
                        [tableName] = writeRequests
                    }
                };

                Dictionary<string, List<WriteRequest>>? unprocessed;
                do
                {
                    var resp = await client.BatchWriteItemAsync(request);
                    unprocessed = resp.UnprocessedItems;
                    if (unprocessed is { Count: > 0 })
                        request.RequestItems = unprocessed;
                } while (unprocessed is { Count: > 0 });
            }
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"BatchWrite failed on '{tableName}'");
        }
    }

    /// <summary>
    /// Retrieve up to 100 items by key in a single batch request.
    /// Automatically retries unprocessed keys.
    /// </summary>
    public static async Task<List<Dictionary<string, AttributeValue>>> BatchGetAsync(
        string tableName,
        List<Dictionary<string, AttributeValue>> keys,
        RegionEndpoint? region = null)
    {
        if (keys.Count > 100)
            throw new ArgumentException("BatchGet supports at most 100 keys per call", nameof(keys));

        var client = GetClient(region);
        var items = new List<Dictionary<string, AttributeValue>>();
        var requestItems = new Dictionary<string, KeysAndAttributes>
        {
            [tableName] = new KeysAndAttributes { Keys = keys }
        };

        try
        {
            while (requestItems is { Count: > 0 })
            {
                var resp = await client.BatchGetItemAsync(new BatchGetItemRequest
                {
                    RequestItems = requestItems
                });
                if (resp.Responses.TryGetValue(tableName, out var tableItems))
                    items.AddRange(tableItems);
                requestItems = resp.UnprocessedKeys is { Count: > 0 }
                    ? resp.UnprocessedKeys
                    : null!;
            }
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"BatchGet failed on '{tableName}'");
        }
        return items;
    }

    // -----------------------------------------------------------------------
    // Batch operations (low-level)
    // -----------------------------------------------------------------------

    /// <summary>
    /// Low-level batch write item. Caller provides the full RequestItems structure.
    /// </summary>
    public static async Task<BatchWriteItemResult> BatchWriteItemAsync(
        Dictionary<string, List<WriteRequest>> requestItems,
        ReturnConsumedCapacity? returnConsumedCapacity = null,
        ReturnItemCollectionMetrics? returnItemCollectionMetrics = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchWriteItemRequest
        {
            RequestItems = requestItems
        };
        if (returnConsumedCapacity != null)
            request.ReturnConsumedCapacity = returnConsumedCapacity;
        if (returnItemCollectionMetrics != null)
            request.ReturnItemCollectionMetrics = returnItemCollectionMetrics;

        try
        {
            var resp = await client.BatchWriteItemAsync(request);
            return new BatchWriteItemResult(
                UnprocessedItems: resp.UnprocessedItems,
                ItemCollectionMetrics: resp.ItemCollectionMetrics,
                ConsumedCapacity: resp.ConsumedCapacity);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "BatchWriteItem failed");
        }
    }

    /// <summary>
    /// Low-level batch get item. Caller provides the full RequestItems structure.
    /// </summary>
    public static async Task<BatchGetItemResult> BatchGetItemAsync(
        Dictionary<string, KeysAndAttributes> requestItems,
        ReturnConsumedCapacity? returnConsumedCapacity = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchGetItemRequest
        {
            RequestItems = requestItems
        };
        if (returnConsumedCapacity != null)
            request.ReturnConsumedCapacity = returnConsumedCapacity;

        try
        {
            var resp = await client.BatchGetItemAsync(request);
            return new BatchGetItemResult(
                Responses: resp.Responses,
                UnprocessedKeys: resp.UnprocessedKeys,
                ConsumedCapacity: resp.ConsumedCapacity);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "BatchGetItem failed");
        }
    }

    // -----------------------------------------------------------------------
    // Table management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a DynamoDB table.
    /// </summary>
    public static async Task<CreateTableResult> CreateTableAsync(
        string tableName,
        List<AttributeDefinition> attributeDefinitions,
        List<KeySchemaElement> keySchema,
        List<LocalSecondaryIndex>? localSecondaryIndexes = null,
        List<GlobalSecondaryIndex>? globalSecondaryIndexes = null,
        BillingMode? billingMode = null,
        ProvisionedThroughput? provisionedThroughput = null,
        StreamSpecification? streamSpecification = null,
        SSESpecification? sseSpecification = null,
        List<Tag>? tags = null,
        TableClass? tableClass = null,
        bool? deletionProtectionEnabled = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateTableRequest
        {
            TableName = tableName,
            AttributeDefinitions = attributeDefinitions,
            KeySchema = keySchema
        };
        if (localSecondaryIndexes != null)
            request.LocalSecondaryIndexes = localSecondaryIndexes;
        if (globalSecondaryIndexes != null)
            request.GlobalSecondaryIndexes = globalSecondaryIndexes;
        if (billingMode != null)
            request.BillingMode = billingMode;
        if (provisionedThroughput != null)
            request.ProvisionedThroughput = provisionedThroughput;
        if (streamSpecification != null)
            request.StreamSpecification = streamSpecification;
        if (sseSpecification != null)
            request.SSESpecification = sseSpecification;
        if (tags != null)
            request.Tags = tags;
        if (tableClass != null)
            request.TableClass = tableClass;
        if (deletionProtectionEnabled.HasValue)
            request.DeletionProtectionEnabled = deletionProtectionEnabled.Value;

        try
        {
            var resp = await client.CreateTableAsync(request);
            return new CreateTableResult(TableDescription: resp.TableDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "CreateTable failed");
        }
    }

    /// <summary>
    /// Delete a DynamoDB table.
    /// </summary>
    public static async Task<DeleteTableResult> DeleteTableAsync(
        string tableName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteTableAsync(new DeleteTableRequest
            {
                TableName = tableName
            });
            return new DeleteTableResult(TableDescription: resp.TableDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "DeleteTable failed");
        }
    }

    /// <summary>
    /// Describe a DynamoDB table.
    /// </summary>
    public static async Task<DescribeTableResult> DescribeTableAsync(
        string tableName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeTableAsync(new DescribeTableRequest
            {
                TableName = tableName
            });
            return new DescribeTableResult(Table: resp.Table);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "DescribeTable failed");
        }
    }

    /// <summary>
    /// List DynamoDB tables, with optional pagination.
    /// </summary>
    public static async Task<ListTablesResult> ListTablesAsync(
        string? exclusiveStartTableName = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTablesRequest();
        if (exclusiveStartTableName != null)
            request.ExclusiveStartTableName = exclusiveStartTableName;
        if (limit.HasValue)
            request.Limit = limit.Value;

        try
        {
            var resp = await client.ListTablesAsync(request);
            return new ListTablesResult(
                TableNames: resp.TableNames,
                LastEvaluatedTableName: resp.LastEvaluatedTableName);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ListTables failed");
        }
    }

    /// <summary>
    /// Update table settings (billing mode, throughput, GSI updates, etc.).
    /// </summary>
    public static async Task<UpdateTableResult> UpdateTableAsync(
        string tableName,
        List<AttributeDefinition>? attributeDefinitions = null,
        BillingMode? billingMode = null,
        ProvisionedThroughput? provisionedThroughput = null,
        List<GlobalSecondaryIndexUpdate>? globalSecondaryIndexUpdates = null,
        StreamSpecification? streamSpecification = null,
        SSESpecification? sseSpecification = null,
        List<ReplicationGroupUpdate>? replicaUpdates = null,
        TableClass? tableClass = null,
        bool? deletionProtectionEnabled = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateTableRequest { TableName = tableName };
        if (attributeDefinitions != null)
            request.AttributeDefinitions = attributeDefinitions;
        if (billingMode != null)
            request.BillingMode = billingMode;
        if (provisionedThroughput != null)
            request.ProvisionedThroughput = provisionedThroughput;
        if (globalSecondaryIndexUpdates != null)
            request.GlobalSecondaryIndexUpdates = globalSecondaryIndexUpdates;
        if (streamSpecification != null)
            request.StreamSpecification = streamSpecification;
        if (sseSpecification != null)
            request.SSESpecification = sseSpecification;
        if (replicaUpdates != null)
            request.ReplicaUpdates = replicaUpdates;
        if (tableClass != null)
            request.TableClass = tableClass;
        if (deletionProtectionEnabled.HasValue)
            request.DeletionProtectionEnabled = deletionProtectionEnabled.Value;

        try
        {
            var resp = await client.UpdateTableAsync(request);
            return new UpdateTableResult(TableDescription: resp.TableDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "UpdateTable failed");
        }
    }

    // -----------------------------------------------------------------------
    // Atomic counter
    // -----------------------------------------------------------------------

    /// <summary>
    /// Atomically increment (or decrement) a numeric attribute.
    /// Creates the attribute with value <paramref name="amount"/> if it does not exist.
    /// Returns the new value after the increment.
    /// </summary>
    public static async Task<long> AtomicIncrementAsync(
        string tableName,
        Dictionary<string, AttributeValue> key,
        string attribute,
        long amount = 1,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateItemRequest
        {
            TableName = tableName,
            Key = key,
            UpdateExpression = "ADD #attr :delta",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                ["#attr"] = attribute
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                [":delta"] = new AttributeValue { N = amount.ToString() }
            },
            ReturnValues = ReturnValue.UPDATED_NEW
        };

        try
        {
            var resp = await client.UpdateItemAsync(request);
            return long.Parse(resp.Attributes[attribute].N);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"AtomicIncrement failed on '{tableName}'.{attribute}");
        }
    }

    // -----------------------------------------------------------------------
    // Transactions
    // -----------------------------------------------------------------------

    /// <summary>
    /// Execute multiple write operations atomically (ACID transaction).
    /// </summary>
    public static async Task<TransactWriteItemsResult> TransactWriteItemsAsync(
        List<TransactWriteItem> transactItems,
        ReturnConsumedCapacity? returnConsumedCapacity = null,
        ReturnItemCollectionMetrics? returnItemCollectionMetrics = null,
        string? clientRequestToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new TransactWriteItemsRequest
        {
            TransactItems = transactItems
        };
        if (returnConsumedCapacity != null)
            request.ReturnConsumedCapacity = returnConsumedCapacity;
        if (returnItemCollectionMetrics != null)
            request.ReturnItemCollectionMetrics = returnItemCollectionMetrics;
        if (clientRequestToken != null)
            request.ClientRequestToken = clientRequestToken;

        try
        {
            var resp = await client.TransactWriteItemsAsync(request);
            return new TransactWriteItemsResult(
                ConsumedCapacity: resp.ConsumedCapacity,
                ItemCollectionMetrics: resp.ItemCollectionMetrics);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "TransactWriteItems failed");
        }
    }

    /// <summary>
    /// Fetch multiple items atomically across tables (ACID read).
    /// </summary>
    public static async Task<TransactGetItemsResult> TransactGetItemsAsync(
        List<TransactGetItem> transactItems,
        ReturnConsumedCapacity? returnConsumedCapacity = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new TransactGetItemsRequest
        {
            TransactItems = transactItems
        };
        if (returnConsumedCapacity != null)
            request.ReturnConsumedCapacity = returnConsumedCapacity;

        try
        {
            var resp = await client.TransactGetItemsAsync(request);
            return new TransactGetItemsResult(
                ConsumedCapacity: resp.ConsumedCapacity,
                Responses: resp.Responses);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "TransactGetItems failed");
        }
    }

    // -----------------------------------------------------------------------
    // PartiQL
    // -----------------------------------------------------------------------

    /// <summary>
    /// Execute a PartiQL statement.
    /// </summary>
    public static async Task<ExecuteStatementResult> ExecuteStatementAsync(
        string statement,
        List<AttributeValue>? parameters = null,
        bool? consistentRead = null,
        string? nextToken = null,
        ReturnConsumedCapacity? returnConsumedCapacity = null,
        int? limit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ExecuteStatementRequest
        {
            Statement = statement
        };
        if (parameters != null)
            request.Parameters = parameters;
        if (consistentRead.HasValue)
            request.ConsistentRead = consistentRead.Value;
        if (nextToken != null)
            request.NextToken = nextToken;
        if (returnConsumedCapacity != null)
            request.ReturnConsumedCapacity = returnConsumedCapacity;
        if (limit.HasValue)
            request.Limit = limit.Value;

        try
        {
            var resp = await client.ExecuteStatementAsync(request);
            return new ExecuteStatementResult(
                Items: resp.Items,
                NextToken: resp.NextToken,
                ConsumedCapacity: resp.ConsumedCapacity,
                LastEvaluatedKey: resp.LastEvaluatedKey);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ExecuteStatement failed");
        }
    }

    /// <summary>
    /// Batch execute PartiQL statements.
    /// </summary>
    public static async Task<BatchExecuteStatementResult> BatchExecuteStatementAsync(
        List<BatchStatementRequest> statements,
        ReturnConsumedCapacity? returnConsumedCapacity = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchExecuteStatementRequest
        {
            Statements = statements
        };
        if (returnConsumedCapacity != null)
            request.ReturnConsumedCapacity = returnConsumedCapacity;

        try
        {
            var resp = await client.BatchExecuteStatementAsync(request);
            return new BatchExecuteStatementResult(
                Responses: resp.Responses,
                ConsumedCapacity: resp.ConsumedCapacity);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "BatchExecuteStatement failed");
        }
    }

    /// <summary>
    /// Execute a PartiQL transaction.
    /// </summary>
    public static async Task<ExecuteTransactionResult> ExecuteTransactionAsync(
        List<ParameterizedStatement> transactStatements,
        string? clientRequestToken = null,
        ReturnConsumedCapacity? returnConsumedCapacity = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ExecuteTransactionRequest
        {
            TransactStatements = transactStatements
        };
        if (clientRequestToken != null)
            request.ClientRequestToken = clientRequestToken;
        if (returnConsumedCapacity != null)
            request.ReturnConsumedCapacity = returnConsumedCapacity;

        try
        {
            var resp = await client.ExecuteTransactionAsync(request);
            return new ExecuteTransactionResult(
                Responses: resp.Responses,
                ConsumedCapacity: resp.ConsumedCapacity);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ExecuteTransaction failed");
        }
    }

    // -----------------------------------------------------------------------
    // Backups
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a backup of a DynamoDB table.
    /// </summary>
    public static async Task<CreateBackupResult> CreateBackupAsync(
        string tableName,
        string backupName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateBackupAsync(new CreateBackupRequest
            {
                TableName = tableName,
                BackupName = backupName
            });
            return new CreateBackupResult(BackupDetails: resp.BackupDetails);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "CreateBackup failed");
        }
    }

    /// <summary>
    /// Delete a DynamoDB backup.
    /// </summary>
    public static async Task<DeleteBackupResult> DeleteBackupAsync(
        string backupArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteBackupAsync(new DeleteBackupRequest
            {
                BackupArn = backupArn
            });
            return new DeleteBackupResult(BackupDescription: resp.BackupDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "DeleteBackup failed");
        }
    }

    /// <summary>
    /// Describe a DynamoDB backup.
    /// </summary>
    public static async Task<DescribeBackupResult> DescribeBackupAsync(
        string backupArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeBackupAsync(new DescribeBackupRequest
            {
                BackupArn = backupArn
            });
            return new DescribeBackupResult(BackupDescription: resp.BackupDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "DescribeBackup failed");
        }
    }

    /// <summary>
    /// List DynamoDB backups.
    /// </summary>
    public static async Task<ListBackupsResult> ListBackupsAsync(
        string? tableName = null,
        int? limit = null,
        string? exclusiveStartBackupArn = null,
        BackupTypeFilter? backupType = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListBackupsRequest();
        if (tableName != null) request.TableName = tableName;
        if (limit.HasValue) request.Limit = limit.Value;
        if (exclusiveStartBackupArn != null)
            request.ExclusiveStartBackupArn = exclusiveStartBackupArn;
        if (backupType != null)
            request.BackupType = backupType;

        try
        {
            var resp = await client.ListBackupsAsync(request);
            return new ListBackupsResult(
                BackupSummaries: resp.BackupSummaries,
                LastEvaluatedBackupArn: resp.LastEvaluatedBackupArn);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ListBackups failed");
        }
    }

    // -----------------------------------------------------------------------
    // Time To Live
    // -----------------------------------------------------------------------

    /// <summary>
    /// Describe TTL settings for a table.
    /// </summary>
    public static async Task<DescribeTimeToLiveResult> DescribeTimeToLiveAsync(
        string tableName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeTimeToLiveAsync(
                new DescribeTimeToLiveRequest { TableName = tableName });
            return new DescribeTimeToLiveResult(
                TimeToLiveDescription: resp.TimeToLiveDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "DescribeTimeToLive failed");
        }
    }

    /// <summary>
    /// Update TTL settings for a table.
    /// </summary>
    public static async Task<UpdateTimeToLiveResult> UpdateTimeToLiveAsync(
        string tableName,
        TimeToLiveSpecification timeToLiveSpecification,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
            {
                TableName = tableName,
                TimeToLiveSpecification = timeToLiveSpecification
            });
            return new UpdateTimeToLiveResult(
                TimeToLiveSpecification: resp.TimeToLiveSpecification);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "UpdateTimeToLive failed");
        }
    }

    // -----------------------------------------------------------------------
    // Continuous backups / Point-in-time recovery
    // -----------------------------------------------------------------------

    /// <summary>
    /// Describe continuous backup / PITR settings for a table.
    /// </summary>
    public static async Task<DescribeContinuousBackupsResult> DescribeContinuousBackupsAsync(
        string tableName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeContinuousBackupsAsync(
                new DescribeContinuousBackupsRequest { TableName = tableName });
            return new DescribeContinuousBackupsResult(
                ContinuousBackupsDescription: resp.ContinuousBackupsDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "DescribeContinuousBackups failed");
        }
    }

    /// <summary>
    /// Update continuous backup / PITR settings for a table.
    /// </summary>
    public static async Task<UpdateContinuousBackupsResult> UpdateContinuousBackupsAsync(
        string tableName,
        PointInTimeRecoverySpecification pointInTimeRecoverySpecification,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateContinuousBackupsAsync(
                new UpdateContinuousBackupsRequest
                {
                    TableName = tableName,
                    PointInTimeRecoverySpecification = pointInTimeRecoverySpecification
                });
            return new UpdateContinuousBackupsResult(
                ContinuousBackupsDescription: resp.ContinuousBackupsDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "UpdateContinuousBackups failed");
        }
    }

    // -----------------------------------------------------------------------
    // Exports
    // -----------------------------------------------------------------------

    /// <summary>
    /// Export a table to S3 at a point in time.
    /// </summary>
    public static async Task<ExportTableToPointInTimeResult> ExportTableToPointInTimeAsync(
        string tableArn,
        string s3Bucket,
        string? s3Prefix = null,
        string? s3SseKmsKeyId = null,
        ExportFormat? exportFormat = null,
        DateTime? exportTime = null,
        string? clientToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ExportTableToPointInTimeRequest
        {
            TableArn = tableArn,
            S3Bucket = s3Bucket
        };
        if (s3Prefix != null) request.S3Prefix = s3Prefix;
        if (s3SseKmsKeyId != null) request.S3SseKmsKeyId = s3SseKmsKeyId;
        if (exportFormat != null) request.ExportFormat = exportFormat;
        if (exportTime.HasValue) request.ExportTime = exportTime.Value;
        if (clientToken != null) request.ClientToken = clientToken;

        try
        {
            var resp = await client.ExportTableToPointInTimeAsync(request);
            return new ExportTableToPointInTimeResult(
                ExportDescription: resp.ExportDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ExportTableToPointInTime failed");
        }
    }

    /// <summary>
    /// Describe an export.
    /// </summary>
    public static async Task<DescribeExportResult> DescribeExportAsync(
        string exportArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeExportAsync(
                new DescribeExportRequest { ExportArn = exportArn });
            return new DescribeExportResult(ExportDescription: resp.ExportDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "DescribeExport failed");
        }
    }

    /// <summary>
    /// List exports.
    /// </summary>
    public static async Task<DynamoDbListExportsResult> ListExportsAsync(
        string? tableArn = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListExportsRequest();
        if (tableArn != null) request.TableArn = tableArn;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListExportsAsync(request);
            return new DynamoDbListExportsResult(
                ExportSummaries: resp.ExportSummaries,
                NextToken: resp.NextToken);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ListExports failed");
        }
    }

    // -----------------------------------------------------------------------
    // Imports
    // -----------------------------------------------------------------------

    /// <summary>
    /// Import a table from S3.
    /// </summary>
    public static async Task<ImportTableResult> ImportTableAsync(
        S3BucketSource s3BucketSource,
        InputFormat inputFormat,
        TableCreationParameters tableCreationParameters,
        string? clientToken = null,
        InputFormatOptions? inputFormatOptions = null,
        InputCompressionType? inputCompressionType = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ImportTableRequest
        {
            S3BucketSource = s3BucketSource,
            InputFormat = inputFormat,
            TableCreationParameters = tableCreationParameters
        };
        if (clientToken != null) request.ClientToken = clientToken;
        if (inputFormatOptions != null) request.InputFormatOptions = inputFormatOptions;
        if (inputCompressionType != null) request.InputCompressionType = inputCompressionType;

        try
        {
            var resp = await client.ImportTableAsync(request);
            return new ImportTableResult(
                ImportTableDescription: resp.ImportTableDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ImportTable failed");
        }
    }

    /// <summary>
    /// Describe an import.
    /// </summary>
    public static async Task<DescribeImportResult> DescribeImportAsync(
        string importArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeImportAsync(
                new DescribeImportRequest { ImportArn = importArn });
            return new DescribeImportResult(
                ImportTableDescription: resp.ImportTableDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "DescribeImport failed");
        }
    }

    /// <summary>
    /// List imports.
    /// </summary>
    public static async Task<DynamoDbListImportsResult> ListImportsAsync(
        string? tableArn = null,
        int? pageSize = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListImportsRequest();
        if (tableArn != null) request.TableArn = tableArn;
        if (pageSize.HasValue) request.PageSize = pageSize.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListImportsAsync(request);
            return new DynamoDbListImportsResult(
                ImportSummaryList: resp.ImportSummaryList,
                NextToken: resp.NextToken);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ListImports failed");
        }
    }

    // -----------------------------------------------------------------------
    // Limits
    // -----------------------------------------------------------------------

    /// <summary>
    /// Describe account and table provisioned-capacity limits.
    /// </summary>
    public static async Task<DescribeLimitsResult> DescribeLimitsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeLimitsAsync(new DescribeLimitsRequest());
            return new DescribeLimitsResult(
                AccountMaxReadCapacityUnits: resp.AccountMaxReadCapacityUnits,
                AccountMaxWriteCapacityUnits: resp.AccountMaxWriteCapacityUnits,
                TableMaxReadCapacityUnits: resp.TableMaxReadCapacityUnits,
                TableMaxWriteCapacityUnits: resp.TableMaxWriteCapacityUnits);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "DescribeLimits failed");
        }
    }

    // -----------------------------------------------------------------------
    // Global tables
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a global table.
    /// </summary>
    public static async Task<CreateGlobalTableResult> CreateGlobalTableAsync(
        string globalTableName,
        List<Replica> replicationGroup,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateGlobalTableAsync(new CreateGlobalTableRequest
            {
                GlobalTableName = globalTableName,
                ReplicationGroup = replicationGroup
            });
            return new CreateGlobalTableResult(
                GlobalTableDescription: resp.GlobalTableDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "CreateGlobalTable failed");
        }
    }

    /// <summary>
    /// Describe a global table.
    /// </summary>
    public static async Task<DescribeGlobalTableResult> DescribeGlobalTableAsync(
        string globalTableName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeGlobalTableAsync(
                new DescribeGlobalTableRequest { GlobalTableName = globalTableName });
            return new DescribeGlobalTableResult(
                GlobalTableDescription: resp.GlobalTableDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "DescribeGlobalTable failed");
        }
    }

    /// <summary>
    /// Update a global table (add/remove replicas).
    /// </summary>
    public static async Task<UpdateGlobalTableResult> UpdateGlobalTableAsync(
        string globalTableName,
        List<ReplicaUpdate> replicaUpdates,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateGlobalTableAsync(new UpdateGlobalTableRequest
            {
                GlobalTableName = globalTableName,
                ReplicaUpdates = replicaUpdates
            });
            return new UpdateGlobalTableResult(
                GlobalTableDescription: resp.GlobalTableDescription);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "UpdateGlobalTable failed");
        }
    }

    /// <summary>
    /// List global tables.
    /// </summary>
    public static async Task<ListGlobalTablesResult> ListGlobalTablesAsync(
        string? exclusiveStartGlobalTableName = null,
        int? limit = null,
        string? regionName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListGlobalTablesRequest();
        if (exclusiveStartGlobalTableName != null)
            request.ExclusiveStartGlobalTableName = exclusiveStartGlobalTableName;
        if (limit.HasValue) request.Limit = limit.Value;
        if (regionName != null) request.RegionName = regionName;

        try
        {
            var resp = await client.ListGlobalTablesAsync(request);
            return new ListGlobalTablesResult(
                GlobalTables: resp.GlobalTables,
                LastEvaluatedGlobalTableName: resp.LastEvaluatedGlobalTableName);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ListGlobalTables failed");
        }
    }

    // -----------------------------------------------------------------------
    // Tagging
    // -----------------------------------------------------------------------

    /// <summary>
    /// Tag a DynamoDB resource.
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
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "TagResource failed");
        }
    }

    /// <summary>
    /// Remove tags from a DynamoDB resource.
    /// </summary>
    public static async Task UntagResourceAsync(
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
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "UntagResource failed");
        }
    }

    /// <summary>
    /// List tags for a DynamoDB resource.
    /// </summary>
    public static async Task<ListTagsOfResourceResult> ListTagsOfResourceAsync(
        string resourceArn,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsOfResourceRequest
        {
            ResourceArn = resourceArn
        };
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListTagsOfResourceAsync(request);
            return new ListTagsOfResourceResult(
                Tags: resp.Tags,
                NextToken: resp.NextToken);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "ListTagsOfResource failed");
        }
    }

    // -----------------------------------------------------------------------
    // Kinesis streaming
    // -----------------------------------------------------------------------

    /// <summary>
    /// Enable Kinesis streaming for a DynamoDB table.
    /// </summary>
    public static async Task<EnableKinesisStreamingDestinationResult>
        EnableKinesisStreamingDestinationAsync(
            string tableName,
            string streamArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.EnableKinesisStreamingDestinationAsync(
                new EnableKinesisStreamingDestinationRequest
                {
                    TableName = tableName,
                    StreamArn = streamArn
                });
            return new EnableKinesisStreamingDestinationResult(
                TableName: resp.TableName,
                StreamArn: resp.StreamArn,
                DestinationStatus: resp.DestinationStatus?.Value);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "EnableKinesisStreamingDestination failed");
        }
    }

    /// <summary>
    /// Disable Kinesis streaming for a DynamoDB table.
    /// </summary>
    public static async Task<DisableKinesisStreamingDestinationResult>
        DisableKinesisStreamingDestinationAsync(
            string tableName,
            string streamArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DisableKinesisStreamingDestinationAsync(
                new DisableKinesisStreamingDestinationRequest
                {
                    TableName = tableName,
                    StreamArn = streamArn
                });
            return new DisableKinesisStreamingDestinationResult(
                TableName: resp.TableName,
                StreamArn: resp.StreamArn,
                DestinationStatus: resp.DestinationStatus?.Value);
        }
        catch (AmazonDynamoDBException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "DisableKinesisStreamingDestination failed");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="GetItemAsync"/>.</summary>
    public static Dictionary<string, AttributeValue>? GetItem(string tableName, Dictionary<string, AttributeValue> key, bool consistentRead = false, RegionEndpoint? region = null)
        => GetItemAsync(tableName, key, consistentRead, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutItemAsync"/>.</summary>
    public static void PutItem(string tableName, Dictionary<string, AttributeValue> item, string? conditionExpression = null, Dictionary<string, string>? expressionAttributeNames = null, Dictionary<string, AttributeValue>? expressionAttributeValues = null, RegionEndpoint? region = null)
        => PutItemAsync(tableName, item, conditionExpression, expressionAttributeNames, expressionAttributeValues, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteItemAsync"/>.</summary>
    public static void DeleteItem(string tableName, Dictionary<string, AttributeValue> key, string? conditionExpression = null, Dictionary<string, string>? expressionAttributeNames = null, Dictionary<string, AttributeValue>? expressionAttributeValues = null, RegionEndpoint? region = null)
        => DeleteItemAsync(tableName, key, conditionExpression, expressionAttributeNames, expressionAttributeValues, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateItemAsync"/>.</summary>
    public static Dictionary<string, AttributeValue> UpdateItem(string tableName, Dictionary<string, AttributeValue> key, string updateExpression, Dictionary<string, string>? expressionAttributeNames = null, Dictionary<string, AttributeValue>? expressionAttributeValues = null, string? conditionExpression = null, ReturnValue? returnValues = null, RegionEndpoint? region = null)
        => UpdateItemAsync(tableName, key, updateExpression, expressionAttributeNames, expressionAttributeValues, conditionExpression, returnValues, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="QueryAsync"/>.</summary>
    public static List<Dictionary<string, AttributeValue>> Query(string tableName, string keyConditionExpression, Dictionary<string, string>? expressionAttributeNames = null, Dictionary<string, AttributeValue>? expressionAttributeValues = null, string? filterExpression = null, string? indexName = null, int? limit = null, bool scanIndexForward = true, bool consistentRead = false, RegionEndpoint? region = null)
        => QueryAsync(tableName, keyConditionExpression, expressionAttributeNames, expressionAttributeValues, filterExpression, indexName, limit, scanIndexForward, consistentRead, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ScanAsync"/>.</summary>
    public static List<Dictionary<string, AttributeValue>> Scan(string tableName, string? filterExpression = null, Dictionary<string, string>? expressionAttributeNames = null, Dictionary<string, AttributeValue>? expressionAttributeValues = null, string? indexName = null, int? limit = null, bool consistentRead = false, RegionEndpoint? region = null)
        => ScanAsync(tableName, filterExpression, expressionAttributeNames, expressionAttributeValues, indexName, limit, consistentRead, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchWriteAsync"/>.</summary>
    public static void BatchWrite(string tableName, List<Dictionary<string, AttributeValue>> items, RegionEndpoint? region = null)
        => BatchWriteAsync(tableName, items, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetAsync"/>.</summary>
    public static List<Dictionary<string, AttributeValue>> BatchGet(string tableName, List<Dictionary<string, AttributeValue>> keys, RegionEndpoint? region = null)
        => BatchGetAsync(tableName, keys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchWriteItemAsync"/>.</summary>
    public static BatchWriteItemResult BatchWriteItem(Dictionary<string, List<WriteRequest>> requestItems, ReturnConsumedCapacity? returnConsumedCapacity = null, ReturnItemCollectionMetrics? returnItemCollectionMetrics = null, RegionEndpoint? region = null)
        => BatchWriteItemAsync(requestItems, returnConsumedCapacity, returnItemCollectionMetrics, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchGetItemAsync"/>.</summary>
    public static BatchGetItemResult BatchGetItem(Dictionary<string, KeysAndAttributes> requestItems, ReturnConsumedCapacity? returnConsumedCapacity = null, RegionEndpoint? region = null)
        => BatchGetItemAsync(requestItems, returnConsumedCapacity, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateTableAsync"/>.</summary>
    public static CreateTableResult CreateTable(string tableName, List<AttributeDefinition> attributeDefinitions, List<KeySchemaElement> keySchema, List<LocalSecondaryIndex>? localSecondaryIndexes = null, List<GlobalSecondaryIndex>? globalSecondaryIndexes = null, BillingMode? billingMode = null, ProvisionedThroughput? provisionedThroughput = null, StreamSpecification? streamSpecification = null, SSESpecification? sseSpecification = null, List<Tag>? tags = null, TableClass? tableClass = null, bool? deletionProtectionEnabled = null, RegionEndpoint? region = null)
        => CreateTableAsync(tableName, attributeDefinitions, keySchema, localSecondaryIndexes, globalSecondaryIndexes, billingMode, provisionedThroughput, streamSpecification, sseSpecification, tags, tableClass, deletionProtectionEnabled, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteTableAsync"/>.</summary>
    public static DeleteTableResult DeleteTable(string tableName, RegionEndpoint? region = null)
        => DeleteTableAsync(tableName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeTableAsync"/>.</summary>
    public static DescribeTableResult DescribeTable(string tableName, RegionEndpoint? region = null)
        => DescribeTableAsync(tableName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTablesAsync"/>.</summary>
    public static ListTablesResult ListTables(string? exclusiveStartTableName = null, int? limit = null, RegionEndpoint? region = null)
        => ListTablesAsync(exclusiveStartTableName, limit, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateTableAsync"/>.</summary>
    public static UpdateTableResult UpdateTable(string tableName, List<AttributeDefinition>? attributeDefinitions = null, BillingMode? billingMode = null, ProvisionedThroughput? provisionedThroughput = null, List<GlobalSecondaryIndexUpdate>? globalSecondaryIndexUpdates = null, StreamSpecification? streamSpecification = null, SSESpecification? sseSpecification = null, List<ReplicationGroupUpdate>? replicaUpdates = null, TableClass? tableClass = null, bool? deletionProtectionEnabled = null, RegionEndpoint? region = null)
        => UpdateTableAsync(tableName, attributeDefinitions, billingMode, provisionedThroughput, globalSecondaryIndexUpdates, streamSpecification, sseSpecification, replicaUpdates, tableClass, deletionProtectionEnabled, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AtomicIncrementAsync"/>.</summary>
    public static long AtomicIncrement(string tableName, Dictionary<string, AttributeValue> key, string attribute, long amount = 1, RegionEndpoint? region = null)
        => AtomicIncrementAsync(tableName, key, attribute, amount, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TransactWriteItemsAsync"/>.</summary>
    public static TransactWriteItemsResult TransactWriteItems(List<TransactWriteItem> transactItems, ReturnConsumedCapacity? returnConsumedCapacity = null, ReturnItemCollectionMetrics? returnItemCollectionMetrics = null, string? clientRequestToken = null, RegionEndpoint? region = null)
        => TransactWriteItemsAsync(transactItems, returnConsumedCapacity, returnItemCollectionMetrics, clientRequestToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TransactGetItemsAsync"/>.</summary>
    public static TransactGetItemsResult TransactGetItems(List<TransactGetItem> transactItems, ReturnConsumedCapacity? returnConsumedCapacity = null, RegionEndpoint? region = null)
        => TransactGetItemsAsync(transactItems, returnConsumedCapacity, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ExecuteStatementAsync"/>.</summary>
    public static ExecuteStatementResult ExecuteStatement(string statement, List<AttributeValue>? parameters = null, bool? consistentRead = null, string? nextToken = null, ReturnConsumedCapacity? returnConsumedCapacity = null, int? limit = null, RegionEndpoint? region = null)
        => ExecuteStatementAsync(statement, parameters, consistentRead, nextToken, returnConsumedCapacity, limit, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchExecuteStatementAsync"/>.</summary>
    public static BatchExecuteStatementResult BatchExecuteStatement(List<BatchStatementRequest> statements, ReturnConsumedCapacity? returnConsumedCapacity = null, RegionEndpoint? region = null)
        => BatchExecuteStatementAsync(statements, returnConsumedCapacity, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ExecuteTransactionAsync"/>.</summary>
    public static ExecuteTransactionResult ExecuteTransaction(List<ParameterizedStatement> transactStatements, string? clientRequestToken = null, ReturnConsumedCapacity? returnConsumedCapacity = null, RegionEndpoint? region = null)
        => ExecuteTransactionAsync(transactStatements, clientRequestToken, returnConsumedCapacity, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateBackupAsync"/>.</summary>
    public static CreateBackupResult CreateBackup(string tableName, string backupName, RegionEndpoint? region = null)
        => CreateBackupAsync(tableName, backupName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteBackupAsync"/>.</summary>
    public static DeleteBackupResult DeleteBackup(string backupArn, RegionEndpoint? region = null)
        => DeleteBackupAsync(backupArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeBackupAsync"/>.</summary>
    public static DescribeBackupResult DescribeBackup(string backupArn, RegionEndpoint? region = null)
        => DescribeBackupAsync(backupArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListBackupsAsync"/>.</summary>
    public static ListBackupsResult ListBackups(string? tableName = null, int? limit = null, string? exclusiveStartBackupArn = null, BackupTypeFilter? backupType = null, RegionEndpoint? region = null)
        => ListBackupsAsync(tableName, limit, exclusiveStartBackupArn, backupType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeTimeToLiveAsync"/>.</summary>
    public static DescribeTimeToLiveResult DescribeTimeToLive(string tableName, RegionEndpoint? region = null)
        => DescribeTimeToLiveAsync(tableName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateTimeToLiveAsync"/>.</summary>
    public static UpdateTimeToLiveResult UpdateTimeToLive(string tableName, TimeToLiveSpecification timeToLiveSpecification, RegionEndpoint? region = null)
        => UpdateTimeToLiveAsync(tableName, timeToLiveSpecification, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeContinuousBackupsAsync"/>.</summary>
    public static DescribeContinuousBackupsResult DescribeContinuousBackups(string tableName, RegionEndpoint? region = null)
        => DescribeContinuousBackupsAsync(tableName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateContinuousBackupsAsync"/>.</summary>
    public static UpdateContinuousBackupsResult UpdateContinuousBackups(string tableName, PointInTimeRecoverySpecification pointInTimeRecoverySpecification, RegionEndpoint? region = null)
        => UpdateContinuousBackupsAsync(tableName, pointInTimeRecoverySpecification, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ExportTableToPointInTimeAsync"/>.</summary>
    public static ExportTableToPointInTimeResult ExportTableToPointInTime(string tableArn, string s3Bucket, string? s3Prefix = null, string? s3SseKmsKeyId = null, ExportFormat? exportFormat = null, DateTime? exportTime = null, string? clientToken = null, RegionEndpoint? region = null)
        => ExportTableToPointInTimeAsync(tableArn, s3Bucket, s3Prefix, s3SseKmsKeyId, exportFormat, exportTime, clientToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeExportAsync"/>.</summary>
    public static DescribeExportResult DescribeExport(string exportArn, RegionEndpoint? region = null)
        => DescribeExportAsync(exportArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListExportsAsync"/>.</summary>
    public static DynamoDbListExportsResult ListExports(string? tableArn = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListExportsAsync(tableArn, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ImportTableAsync"/>.</summary>
    public static ImportTableResult ImportTable(S3BucketSource s3BucketSource, InputFormat inputFormat, TableCreationParameters tableCreationParameters, string? clientToken = null, InputFormatOptions? inputFormatOptions = null, InputCompressionType? inputCompressionType = null, RegionEndpoint? region = null)
        => ImportTableAsync(s3BucketSource, inputFormat, tableCreationParameters, clientToken, inputFormatOptions, inputCompressionType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeImportAsync"/>.</summary>
    public static DescribeImportResult DescribeImport(string importArn, RegionEndpoint? region = null)
        => DescribeImportAsync(importArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListImportsAsync"/>.</summary>
    public static DynamoDbListImportsResult ListImports(string? tableArn = null, int? pageSize = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListImportsAsync(tableArn, pageSize, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeLimitsAsync"/>.</summary>
    public static DescribeLimitsResult DescribeLimits(RegionEndpoint? region = null)
        => DescribeLimitsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateGlobalTableAsync"/>.</summary>
    public static CreateGlobalTableResult CreateGlobalTable(string globalTableName, List<Replica> replicationGroup, RegionEndpoint? region = null)
        => CreateGlobalTableAsync(globalTableName, replicationGroup, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeGlobalTableAsync"/>.</summary>
    public static DescribeGlobalTableResult DescribeGlobalTable(string globalTableName, RegionEndpoint? region = null)
        => DescribeGlobalTableAsync(globalTableName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateGlobalTableAsync"/>.</summary>
    public static UpdateGlobalTableResult UpdateGlobalTable(string globalTableName, List<ReplicaUpdate> replicaUpdates, RegionEndpoint? region = null)
        => UpdateGlobalTableAsync(globalTableName, replicaUpdates, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListGlobalTablesAsync"/>.</summary>
    public static ListGlobalTablesResult ListGlobalTables(string? exclusiveStartGlobalTableName = null, int? limit = null, string? regionName = null, RegionEndpoint? region = null)
        => ListGlobalTablesAsync(exclusiveStartGlobalTableName, limit, regionName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, List<Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsOfResourceAsync"/>.</summary>
    public static ListTagsOfResourceResult ListTagsOfResource(string resourceArn, string? nextToken = null, RegionEndpoint? region = null)
        => ListTagsOfResourceAsync(resourceArn, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EnableKinesisStreamingDestinationAsync"/>.</summary>
    public static EnableKinesisStreamingDestinationResult EnableKinesisStreamingDestination(string tableName, string streamArn, RegionEndpoint? region = null)
        => EnableKinesisStreamingDestinationAsync(tableName, streamArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisableKinesisStreamingDestinationAsync"/>.</summary>
    public static DisableKinesisStreamingDestinationResult DisableKinesisStreamingDestination(string tableName, string streamArn, RegionEndpoint? region = null)
        => DisableKinesisStreamingDestinationAsync(tableName, streamArn, region).GetAwaiter().GetResult();

}

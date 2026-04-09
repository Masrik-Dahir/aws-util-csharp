using Amazon;
using Amazon.Glue;
using Amazon.Glue.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for AWS Glue operations.
/// </summary>
public sealed record CreateGlueDatabaseResult(string? Name = null);
public sealed record GetGlueDatabaseResult(Database? Database = null);
public sealed record GetGlueDatabasesResult(List<Database>? DatabaseList = null);
public sealed record CreateGlueTableResult(string? Name = null);
public sealed record GetGlueTableResult(Table? Table = null);
public sealed record GetGlueTablesResult(List<Table>? TableList = null);
public sealed record CreateCrawlerResult(string? Name = null);
public sealed record GetCrawlerResult(Crawler? Crawler = null);
public sealed record GetCrawlersResult(List<Crawler>? Crawlers = null);
public sealed record CreateGlueJobResult(string? Name = null);
public sealed record GetGlueJobResult(Job? Job = null);
public sealed record GetGlueJobsResult(List<Job>? Jobs = null);
public sealed record StartJobRunResult(string? JobRunId = null);
public sealed record GetJobRunResult(JobRun? JobRun = null);
public sealed record GetJobRunsResult(List<JobRun>? JobRuns = null);
public sealed record BatchStopJobRunResult(
    List<BatchStopJobRunSuccessfulSubmission>? SuccessfulSubmissions = null,
    List<BatchStopJobRunError>? Errors = null);
public sealed record UpdateGlueJobResult(string? JobName = null);
public sealed record CreateTriggerResult(string? Name = null);
public sealed record GetTriggerResult(Trigger? Trigger = null);
public sealed record GetTriggersResult(List<Trigger>? Triggers = null);
public sealed record GetPartitionResult(Partition? Partition = null);
public sealed record GetPartitionsResult(List<Partition>? Partitions = null);
public sealed record BatchCreatePartitionResult(List<PartitionError>? Errors = null);
public sealed record BatchDeletePartitionResult(List<PartitionError>? Errors = null);
public sealed record GetConnectionResult(Connection? Connection = null);
public sealed record GetConnectionsResult(List<Connection>? ConnectionList = null);
public sealed record GetClassifierResult(Classifier? Classifier = null);
public sealed record GetClassifiersResult(List<Classifier>? Classifiers = null);
public sealed record GetGlueTagsResult(Dictionary<string, string>? Tags = null);
public sealed record CreateWorkflowResult(string? Name = null);
public sealed record GetWorkflowResult(Workflow? Workflow = null);
public sealed record GetWorkflowRunResult(WorkflowRun? Run = null);
public sealed record StartWorkflowRunResult(string? RunId = null);

/// <summary>
/// Utility helpers for AWS Glue.
/// </summary>
public static class GlueService
{
    private static AmazonGlueClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonGlueClient>(region);

    // ──────────────────────────── Databases ────────────────────────────

    /// <summary>
    /// Create a Glue database.
    /// </summary>
    public static async Task<CreateGlueDatabaseResult> CreateDatabaseAsync(
        DatabaseInput databaseInput,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDatabaseRequest { DatabaseInput = databaseInput };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            await client.CreateDatabaseAsync(request);
            return new CreateGlueDatabaseResult(Name: databaseInput.Name);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create Glue database '{databaseInput.Name}'");
        }
    }

    /// <summary>
    /// Delete a Glue database.
    /// </summary>
    public static async Task DeleteDatabaseAsync(
        string name,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteDatabaseRequest { Name = name };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            await client.DeleteDatabaseAsync(request);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete Glue database '{name}'");
        }
    }

    /// <summary>
    /// Get a Glue database by name.
    /// </summary>
    public static async Task<GetGlueDatabaseResult> GetDatabaseAsync(
        string name,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetDatabaseRequest { Name = name };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            var resp = await client.GetDatabaseAsync(request);
            return new GetGlueDatabaseResult(Database: resp.Database);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Glue database '{name}'");
        }
    }

    /// <summary>
    /// List all Glue databases, automatically paginating.
    /// </summary>
    public static async Task<GetGlueDatabasesResult> GetDatabasesAsync(
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var databases = new List<Database>();
        var request = new GetDatabasesRequest();
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.GetDatabasesAsync(request);
                databases.AddRange(resp.DatabaseList);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get Glue databases");
        }

        return new GetGlueDatabasesResult(DatabaseList: databases);
    }

    // ──────────────────────────── Tables ────────────────────────────

    /// <summary>
    /// Create a Glue table.
    /// </summary>
    public static async Task<CreateGlueTableResult> CreateTableAsync(
        string databaseName,
        TableInput tableInput,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateTableRequest
        {
            DatabaseName = databaseName,
            TableInput = tableInput
        };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            await client.CreateTableAsync(request);
            return new CreateGlueTableResult(Name: tableInput.Name);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create Glue table '{tableInput.Name}'");
        }
    }

    /// <summary>
    /// Delete a Glue table.
    /// </summary>
    public static async Task DeleteTableAsync(
        string databaseName,
        string name,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteTableRequest
        {
            DatabaseName = databaseName,
            Name = name
        };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            await client.DeleteTableAsync(request);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete Glue table '{name}'");
        }
    }

    /// <summary>
    /// Get a Glue table by name.
    /// </summary>
    public static async Task<GetGlueTableResult> GetTableAsync(
        string databaseName,
        string name,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetTableRequest
        {
            DatabaseName = databaseName,
            Name = name
        };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            var resp = await client.GetTableAsync(request);
            return new GetGlueTableResult(Table: resp.Table);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Glue table '{name}'");
        }
    }

    /// <summary>
    /// List all tables in a Glue database, automatically paginating.
    /// </summary>
    public static async Task<GetGlueTablesResult> GetTablesAsync(
        string databaseName,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var tables = new List<Table>();
        var request = new GetTablesRequest { DatabaseName = databaseName };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.GetTablesAsync(request);
                tables.AddRange(resp.TableList);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Glue tables for database '{databaseName}'");
        }

        return new GetGlueTablesResult(TableList: tables);
    }

    /// <summary>
    /// Update a Glue table.
    /// </summary>
    public static async Task UpdateTableAsync(
        string databaseName,
        TableInput tableInput,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateTableRequest
        {
            DatabaseName = databaseName,
            TableInput = tableInput
        };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            await client.UpdateTableAsync(request);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to update Glue table '{tableInput.Name}'");
        }
    }

    // ──────────────────────────── Crawlers ────────────────────────────

    /// <summary>
    /// Create a Glue crawler.
    /// </summary>
    public static async Task<CreateCrawlerResult> CreateCrawlerAsync(
        CreateCrawlerRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateCrawlerAsync(request);
            return new CreateCrawlerResult(Name: request.Name);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create Glue crawler '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a Glue crawler.
    /// </summary>
    public static async Task DeleteCrawlerAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteCrawlerAsync(new DeleteCrawlerRequest { Name = name });
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete Glue crawler '{name}'");
        }
    }

    /// <summary>
    /// Get a Glue crawler by name.
    /// </summary>
    public static async Task<GetCrawlerResult> GetCrawlerAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetCrawlerAsync(new GetCrawlerRequest { Name = name });
            return new GetCrawlerResult(Crawler: resp.Crawler);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Glue crawler '{name}'");
        }
    }

    /// <summary>
    /// List all Glue crawlers, automatically paginating.
    /// </summary>
    public static async Task<GetCrawlersResult> GetCrawlersAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var crawlers = new List<Crawler>();
        var request = new GetCrawlersRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.GetCrawlersAsync(request);
                crawlers.AddRange(resp.Crawlers);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get Glue crawlers");
        }

        return new GetCrawlersResult(Crawlers: crawlers);
    }

    /// <summary>
    /// Start a Glue crawler.
    /// </summary>
    public static async Task StartCrawlerAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StartCrawlerAsync(new StartCrawlerRequest { Name = name });
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to start Glue crawler '{name}'");
        }
    }

    /// <summary>
    /// Stop a Glue crawler.
    /// </summary>
    public static async Task StopCrawlerAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StopCrawlerAsync(new StopCrawlerRequest { Name = name });
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to stop Glue crawler '{name}'");
        }
    }

    /// <summary>
    /// Update a Glue crawler.
    /// </summary>
    public static async Task UpdateCrawlerAsync(
        UpdateCrawlerRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateCrawlerAsync(request);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to update Glue crawler '{request.Name}'");
        }
    }

    // ──────────────────────────── Jobs ────────────────────────────

    /// <summary>
    /// Create a Glue job.
    /// </summary>
    public static async Task<CreateGlueJobResult> CreateJobAsync(
        CreateJobRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateJobAsync(request);
            return new CreateGlueJobResult(Name: resp.Name);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create Glue job '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a Glue job.
    /// </summary>
    public static async Task DeleteJobAsync(
        string jobName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteJobAsync(new DeleteJobRequest { JobName = jobName });
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete Glue job '{jobName}'");
        }
    }

    /// <summary>
    /// Get a Glue job by name.
    /// </summary>
    public static async Task<GetGlueJobResult> GetJobAsync(
        string jobName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetJobAsync(new GetJobRequest { JobName = jobName });
            return new GetGlueJobResult(Job: resp.Job);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Glue job '{jobName}'");
        }
    }

    /// <summary>
    /// List all Glue jobs, automatically paginating.
    /// </summary>
    public static async Task<GetGlueJobsResult> GetJobsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var jobs = new List<Job>();
        var request = new GetJobsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.GetJobsAsync(request);
                jobs.AddRange(resp.Jobs);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get Glue jobs");
        }

        return new GetGlueJobsResult(Jobs: jobs);
    }

    /// <summary>
    /// Start a Glue job run.
    /// </summary>
    public static async Task<StartJobRunResult> StartJobRunAsync(
        string jobName,
        Dictionary<string, string>? arguments = null,
        int? timeout = null,
        string? workerType = null,
        int? numberOfWorkers = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartJobRunRequest { JobName = jobName };
        if (arguments != null) request.Arguments = arguments;
        if (timeout.HasValue) request.Timeout = timeout.Value;
        if (workerType != null) request.WorkerType = workerType;
        if (numberOfWorkers.HasValue) request.NumberOfWorkers = numberOfWorkers.Value;

        try
        {
            var resp = await client.StartJobRunAsync(request);
            return new StartJobRunResult(JobRunId: resp.JobRunId);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to start Glue job run for '{jobName}'");
        }
    }

    /// <summary>
    /// Get a specific Glue job run.
    /// </summary>
    public static async Task<GetJobRunResult> GetJobRunAsync(
        string jobName,
        string runId,
        bool predecessorsIncluded = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetJobRunAsync(new GetJobRunRequest
            {
                JobName = jobName,
                RunId = runId,
                PredecessorsIncluded = predecessorsIncluded
            });
            return new GetJobRunResult(JobRun: resp.JobRun);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Glue job run '{runId}'");
        }
    }

    /// <summary>
    /// List all runs for a Glue job, automatically paginating.
    /// </summary>
    public static async Task<GetJobRunsResult> GetJobRunsAsync(
        string jobName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var runs = new List<JobRun>();
        var request = new GetJobRunsRequest { JobName = jobName };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.GetJobRunsAsync(request);
                runs.AddRange(resp.JobRuns);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Glue job runs for '{jobName}'");
        }

        return new GetJobRunsResult(JobRuns: runs);
    }

    /// <summary>
    /// Stop one or more Glue job runs.
    /// </summary>
    public static async Task<BatchStopJobRunResult> BatchStopJobRunAsync(
        string jobName,
        List<string> jobRunIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchStopJobRunAsync(new BatchStopJobRunRequest
            {
                JobName = jobName,
                JobRunIds = jobRunIds
            });
            return new BatchStopJobRunResult(
                SuccessfulSubmissions: resp.SuccessfulSubmissions,
                Errors: resp.Errors);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to batch stop job runs for '{jobName}'");
        }
    }

    /// <summary>
    /// Update a Glue job.
    /// </summary>
    public static async Task<UpdateGlueJobResult> UpdateJobAsync(
        string jobName,
        JobUpdate jobUpdate,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateJobAsync(new UpdateJobRequest
            {
                JobName = jobName,
                JobUpdate = jobUpdate
            });
            return new UpdateGlueJobResult(JobName: resp.JobName);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to update Glue job '{jobName}'");
        }
    }

    // ──────────────────────────── Triggers ────────────────────────────

    /// <summary>
    /// Create a Glue trigger.
    /// </summary>
    public static async Task<CreateTriggerResult> CreateTriggerAsync(
        CreateTriggerRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateTriggerAsync(request);
            return new CreateTriggerResult(Name: resp.Name);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create Glue trigger '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a Glue trigger.
    /// </summary>
    public static async Task DeleteTriggerAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteTriggerAsync(new DeleteTriggerRequest { Name = name });
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete Glue trigger '{name}'");
        }
    }

    /// <summary>
    /// Get a Glue trigger by name.
    /// </summary>
    public static async Task<GetTriggerResult> GetTriggerAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetTriggerAsync(new GetTriggerRequest { Name = name });
            return new GetTriggerResult(Trigger: resp.Trigger);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Glue trigger '{name}'");
        }
    }

    /// <summary>
    /// List all Glue triggers, automatically paginating.
    /// </summary>
    public static async Task<GetTriggersResult> GetTriggersAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var triggers = new List<Trigger>();
        var request = new GetTriggersRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.GetTriggersAsync(request);
                triggers.AddRange(resp.Triggers);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get Glue triggers");
        }

        return new GetTriggersResult(Triggers: triggers);
    }

    /// <summary>
    /// Start a Glue trigger.
    /// </summary>
    public static async Task StartTriggerAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StartTriggerAsync(new StartTriggerRequest { Name = name });
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to start Glue trigger '{name}'");
        }
    }

    /// <summary>
    /// Stop a Glue trigger.
    /// </summary>
    public static async Task StopTriggerAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.StopTriggerAsync(new StopTriggerRequest { Name = name });
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to stop Glue trigger '{name}'");
        }
    }

    /// <summary>
    /// Update a Glue trigger.
    /// </summary>
    public static async Task UpdateTriggerAsync(
        string name,
        TriggerUpdate triggerUpdate,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateTriggerAsync(new UpdateTriggerRequest
            {
                Name = name,
                TriggerUpdate = triggerUpdate
            });
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to update Glue trigger '{name}'");
        }
    }

    // ──────────────────────────── Partitions ────────────────────────────

    /// <summary>
    /// Create a partition in a Glue table.
    /// </summary>
    public static async Task CreatePartitionAsync(
        string databaseName,
        string tableName,
        PartitionInput partitionInput,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreatePartitionRequest
        {
            DatabaseName = databaseName,
            TableName = tableName,
            PartitionInput = partitionInput
        };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            await client.CreatePartitionAsync(request);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create partition in '{databaseName}.{tableName}'");
        }
    }

    /// <summary>
    /// Delete a partition from a Glue table.
    /// </summary>
    public static async Task DeletePartitionAsync(
        string databaseName,
        string tableName,
        List<string> partitionValues,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeletePartitionRequest
        {
            DatabaseName = databaseName,
            TableName = tableName,
            PartitionValues = partitionValues
        };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            await client.DeletePartitionAsync(request);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete partition in '{databaseName}.{tableName}'");
        }
    }

    /// <summary>
    /// Get a partition from a Glue table.
    /// </summary>
    public static async Task<GetPartitionResult> GetPartitionAsync(
        string databaseName,
        string tableName,
        List<string> partitionValues,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetPartitionRequest
        {
            DatabaseName = databaseName,
            TableName = tableName,
            PartitionValues = partitionValues
        };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            var resp = await client.GetPartitionAsync(request);
            return new GetPartitionResult(Partition: resp.Partition);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get partition in '{databaseName}.{tableName}'");
        }
    }

    /// <summary>
    /// List partitions in a Glue table, automatically paginating.
    /// </summary>
    public static async Task<GetPartitionsResult> GetPartitionsAsync(
        string databaseName,
        string tableName,
        string? catalogId = null,
        string? expression = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var partitions = new List<Partition>();
        var request = new GetPartitionsRequest
        {
            DatabaseName = databaseName,
            TableName = tableName
        };
        if (catalogId != null) request.CatalogId = catalogId;
        if (expression != null) request.Expression = expression;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.GetPartitionsAsync(request);
                partitions.AddRange(resp.Partitions);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get partitions in '{databaseName}.{tableName}'");
        }

        return new GetPartitionsResult(Partitions: partitions);
    }

    /// <summary>
    /// Batch create partitions in a Glue table.
    /// </summary>
    public static async Task<BatchCreatePartitionResult> BatchCreatePartitionAsync(
        string databaseName,
        string tableName,
        List<PartitionInput> partitionInputList,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchCreatePartitionRequest
        {
            DatabaseName = databaseName,
            TableName = tableName,
            PartitionInputList = partitionInputList
        };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            var resp = await client.BatchCreatePartitionAsync(request);
            return new BatchCreatePartitionResult(Errors: resp.Errors);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to batch create partitions in '{databaseName}.{tableName}'");
        }
    }

    /// <summary>
    /// Batch delete partitions from a Glue table.
    /// </summary>
    public static async Task<BatchDeletePartitionResult> BatchDeletePartitionAsync(
        string databaseName,
        string tableName,
        List<PartitionValueList> partitionsToDelete,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchDeletePartitionRequest
        {
            DatabaseName = databaseName,
            TableName = tableName,
            PartitionsToDelete = partitionsToDelete
        };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            var resp = await client.BatchDeletePartitionAsync(request);
            return new BatchDeletePartitionResult(Errors: resp.Errors);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to batch delete partitions in '{databaseName}.{tableName}'");
        }
    }

    /// <summary>
    /// Update a partition in a Glue table.
    /// </summary>
    public static async Task UpdatePartitionAsync(
        string databaseName,
        string tableName,
        List<string> partitionValueList,
        PartitionInput partitionInput,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdatePartitionRequest
        {
            DatabaseName = databaseName,
            TableName = tableName,
            PartitionValueList = partitionValueList,
            PartitionInput = partitionInput
        };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            await client.UpdatePartitionAsync(request);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update partition in '{databaseName}.{tableName}'");
        }
    }

    // ──────────────────────────── Connections ────────────────────────────

    /// <summary>
    /// Get a Glue connection by name.
    /// </summary>
    public static async Task<GetConnectionResult> GetConnectionAsync(
        string name,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetConnectionRequest { Name = name };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            var resp = await client.GetConnectionAsync(request);
            return new GetConnectionResult(Connection: resp.Connection);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Glue connection '{name}'");
        }
    }

    /// <summary>
    /// List all Glue connections, automatically paginating.
    /// </summary>
    public static async Task<GetConnectionsResult> GetConnectionsAsync(
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var connections = new List<Connection>();
        var request = new GetConnectionsRequest();
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.GetConnectionsAsync(request);
                connections.AddRange(resp.ConnectionList);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get Glue connections");
        }

        return new GetConnectionsResult(ConnectionList: connections);
    }

    /// <summary>
    /// Create a Glue connection.
    /// </summary>
    public static async Task CreateConnectionAsync(
        ConnectionInput connectionInput,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateConnectionRequest { ConnectionInput = connectionInput };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            await client.CreateConnectionAsync(request);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create Glue connection '{connectionInput.Name}'");
        }
    }

    /// <summary>
    /// Delete a Glue connection.
    /// </summary>
    public static async Task DeleteConnectionAsync(
        string connectionName,
        string? catalogId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteConnectionRequest { ConnectionName = connectionName };
        if (catalogId != null) request.CatalogId = catalogId;

        try
        {
            await client.DeleteConnectionAsync(request);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete Glue connection '{connectionName}'");
        }
    }

    // ──────────────────────────── Classifiers ────────────────────────────

    /// <summary>
    /// Create a Glue classifier.
    /// </summary>
    public static async Task CreateClassifierAsync(
        CreateClassifierRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.CreateClassifierAsync(request);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to create Glue classifier");
        }
    }

    /// <summary>
    /// Delete a Glue classifier.
    /// </summary>
    public static async Task DeleteClassifierAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteClassifierAsync(new DeleteClassifierRequest { Name = name });
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete Glue classifier '{name}'");
        }
    }

    /// <summary>
    /// Get a Glue classifier by name.
    /// </summary>
    public static async Task<GetClassifierResult> GetClassifierAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetClassifierAsync(new GetClassifierRequest { Name = name });
            return new GetClassifierResult(Classifier: resp.Classifier);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Glue classifier '{name}'");
        }
    }

    /// <summary>
    /// List all Glue classifiers, automatically paginating.
    /// </summary>
    public static async Task<GetClassifiersResult> GetClassifiersAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var classifiers = new List<Classifier>();
        var request = new GetClassifiersRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.GetClassifiersAsync(request);
                classifiers.AddRange(resp.Classifiers);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get Glue classifiers");
        }

        return new GetClassifiersResult(Classifiers: classifiers);
    }

    // ──────────────────────────── Tags ────────────────────────────

    /// <summary>
    /// Add tags to a Glue resource.
    /// </summary>
    public static async Task TagResourceAsync(
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
                TagsToAdd = tags
            });
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to tag Glue resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Glue resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string resourceArn,
        List<string> tagsToRemove,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceArn = resourceArn,
                TagsToRemove = tagsToRemove
            });
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to untag Glue resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Get tags for a Glue resource.
    /// </summary>
    public static async Task<GetGlueTagsResult> GetTagsAsync(
        string resourceArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetTagsAsync(new GetTagsRequest
            {
                ResourceArn = resourceArn
            });
            return new GetGlueTagsResult(Tags: resp.Tags);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get tags for Glue resource '{resourceArn}'");
        }
    }

    // ──────────────────────────── Workflows ────────────────────────────

    /// <summary>
    /// Create a Glue workflow.
    /// </summary>
    public static async Task<CreateWorkflowResult> CreateWorkflowAsync(
        string name,
        string? description = null,
        Dictionary<string, string>? defaultRunProperties = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateWorkflowRequest { Name = name };
        if (description != null) request.Description = description;
        if (defaultRunProperties != null) request.DefaultRunProperties = defaultRunProperties;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateWorkflowAsync(request);
            return new CreateWorkflowResult(Name: resp.Name);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to create Glue workflow '{name}'");
        }
    }

    /// <summary>
    /// Delete a Glue workflow.
    /// </summary>
    public static async Task DeleteWorkflowAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteWorkflowAsync(new DeleteWorkflowRequest { Name = name });
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete Glue workflow '{name}'");
        }
    }

    /// <summary>
    /// Get a Glue workflow by name.
    /// </summary>
    public static async Task<GetWorkflowResult> GetWorkflowAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetWorkflowAsync(new GetWorkflowRequest { Name = name });
            return new GetWorkflowResult(Workflow: resp.Workflow);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Glue workflow '{name}'");
        }
    }

    /// <summary>
    /// Get a specific Glue workflow run.
    /// </summary>
    public static async Task<GetWorkflowRunResult> GetWorkflowRunAsync(
        string name,
        string runId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetWorkflowRunAsync(new GetWorkflowRunRequest
            {
                Name = name,
                RunId = runId
            });
            return new GetWorkflowRunResult(Run: resp.Run);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get Glue workflow run '{runId}'");
        }
    }

    /// <summary>
    /// Start a Glue workflow run.
    /// </summary>
    public static async Task<StartWorkflowRunResult> StartWorkflowRunAsync(
        string name,
        Dictionary<string, string>? runProperties = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartWorkflowRunRequest { Name = name };
        if (runProperties != null) request.RunProperties = runProperties;

        try
        {
            var resp = await client.StartWorkflowRunAsync(request);
            return new StartWorkflowRunResult(RunId: resp.RunId);
        }
        catch (AmazonGlueException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to start Glue workflow run for '{name}'");
        }
    }
}

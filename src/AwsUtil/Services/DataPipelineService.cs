using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.Athena.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of running a Glue job followed by an Athena query.</summary>
public sealed record GlueThenQueryResult(
    string? JobRunId = null,
    string? JobRunState = null,
    string? QueryExecutionId = null,
    ResultSet? QueryResultSet = null);

/// <summary>Result of exporting an Athena query's results to S3 as JSON.</summary>
public sealed record ExportQueryToS3JsonResult(
    string? QueryExecutionId = null,
    int RowCount = 0,
    string? S3Bucket = null,
    string? S3Key = null,
    string? ETag = null);

/// <summary>Result of running a Glue job to completion.</summary>
public sealed record RunGlueJobResult(
    string? JobRunId = null,
    string? JobRunState = null,
    int ElapsedSeconds = 0);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Multi-service data pipeline combining AWS Glue, Athena, and S3
/// into high-level ETL and query orchestration workflows.
/// </summary>
public static class DataPipelineService
{
    private static readonly HashSet<string> GlueTerminalStates = new(StringComparer.OrdinalIgnoreCase)
    {
        "SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"
    };

    private static readonly HashSet<string> AthenaTerminalStates = new(StringComparer.OrdinalIgnoreCase)
    {
        "SUCCEEDED", "FAILED", "CANCELLED"
    };

    /// <summary>
    /// Start a Glue job, poll until it completes, then run an Athena query
    /// against the data the job produced and return the query results.
    /// </summary>
    /// <param name="jobName">Glue job name.</param>
    /// <param name="queryString">Athena SQL query to run after the job completes.</param>
    /// <param name="database">Athena database for the query.</param>
    /// <param name="outputLocation">S3 output location for Athena results.</param>
    /// <param name="jobArguments">Optional Glue job arguments.</param>
    /// <param name="pollIntervalSeconds">Seconds between Glue job status polls.</param>
    /// <param name="workGroup">Athena workgroup.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<GlueThenQueryResult> RunGlueThenQueryAsync(
        string jobName,
        string queryString,
        string database,
        string outputLocation,
        Dictionary<string, string>? jobArguments = null,
        int pollIntervalSeconds = 15,
        string? workGroup = null,
        RegionEndpoint? region = null)
    {
        try
        {
            // 1. Run the Glue job to completion
            var glueResult = await RunGlueJobAsync(
                jobName,
                jobArguments,
                pollIntervalSeconds,
                region);

            if (!string.Equals(glueResult.JobRunState, "SUCCEEDED", StringComparison.OrdinalIgnoreCase))
            {
                return new GlueThenQueryResult(
                    JobRunId: glueResult.JobRunId,
                    JobRunState: glueResult.JobRunState);
            }

            // 2. Start the Athena query
            var startResult = await AthenaService.StartQueryExecutionAsync(
                queryString,
                database: database,
                outputLocation: outputLocation,
                workGroup: workGroup,
                region: region);

            var queryId = startResult.QueryExecutionId!;

            // 3. Poll Athena until the query completes
            string? queryState = null;
            while (true)
            {
                var status = await AthenaService.GetQueryExecutionAsync(queryId, region);
                queryState = status.QueryExecution?.Status?.State?.Value;
                if (queryState != null && AthenaTerminalStates.Contains(queryState))
                    break;
                await Task.Delay(TimeSpan.FromSeconds(5));
            }

            // 4. Fetch and return the query results
            ResultSet? resultSet = null;
            if (string.Equals(queryState, "SUCCEEDED", StringComparison.OrdinalIgnoreCase))
            {
                var queryResults = await AthenaService.GetQueryResultsAsync(queryId, region: region);
                resultSet = queryResults.ResultSet;
            }

            return new GlueThenQueryResult(
                JobRunId: glueResult.JobRunId,
                JobRunState: glueResult.JobRunState,
                QueryExecutionId: queryId,
                QueryResultSet: resultSet);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed Glue-then-query pipeline for job '{jobName}'");
        }
    }

    /// <summary>
    /// Run an Athena query, wait for it to complete, convert the results to
    /// JSON, and upload them to S3.
    /// </summary>
    /// <param name="queryString">Athena SQL query.</param>
    /// <param name="database">Athena database.</param>
    /// <param name="outputLocation">S3 location for Athena raw results.</param>
    /// <param name="destBucket">S3 bucket for the JSON export.</param>
    /// <param name="destKey">S3 key for the JSON export.</param>
    /// <param name="workGroup">Athena workgroup.</param>
    /// <param name="pollIntervalSeconds">Seconds between Athena status polls.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<ExportQueryToS3JsonResult> ExportQueryToS3JsonAsync(
        string queryString,
        string database,
        string outputLocation,
        string destBucket,
        string destKey,
        string? workGroup = null,
        int pollIntervalSeconds = 5,
        RegionEndpoint? region = null)
    {
        try
        {
            // 1. Start the Athena query
            var startResult = await AthenaService.StartQueryExecutionAsync(
                queryString,
                database: database,
                outputLocation: outputLocation,
                workGroup: workGroup,
                region: region);

            var queryId = startResult.QueryExecutionId!;

            // 2. Poll until the query completes
            string? queryState = null;
            while (true)
            {
                var status = await AthenaService.GetQueryExecutionAsync(queryId, region);
                queryState = status.QueryExecution?.Status?.State?.Value;
                if (queryState != null && AthenaTerminalStates.Contains(queryState))
                    break;
                await Task.Delay(TimeSpan.FromSeconds(pollIntervalSeconds));
            }

            if (!string.Equals(queryState, "SUCCEEDED", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Athena query '{queryId}' finished with state '{queryState}'");
            }

            // 3. Fetch all result rows and convert to a list of dictionaries
            var rows = new List<Dictionary<string, string>>();
            string? nextToken = null;
            List<string>? columnNames = null;

            do
            {
                var page = await AthenaService.GetQueryResultsAsync(
                    queryId, nextToken: nextToken, region: region);

                var resultSet = page.ResultSet;
                if (resultSet?.ResultSetMetadata?.ColumnInfo != null && columnNames == null)
                {
                    columnNames = resultSet.ResultSetMetadata.ColumnInfo
                        .Select(c => c.Name)
                        .ToList();
                }

                if (resultSet?.Rows != null && columnNames != null)
                {
                    // Skip the header row on the first page
                    var dataRows = nextToken == null && resultSet.Rows.Count > 0
                        ? resultSet.Rows.Skip(1)
                        : resultSet.Rows;

                    foreach (var row in dataRows)
                    {
                        var dict = new Dictionary<string, string>();
                        for (var i = 0; i < columnNames.Count && i < row.Data.Count; i++)
                        {
                            dict[columnNames[i]] = row.Data[i].VarCharValue ?? "";
                        }
                        rows.Add(dict);
                    }
                }

                nextToken = page.NextToken;
            } while (nextToken != null);

            // 4. Serialize to JSON and upload to S3
            var json = JsonSerializer.Serialize(rows, new JsonSerializerOptions
            {
                WriteIndented = true
            });
            var jsonBytes = Encoding.UTF8.GetBytes(json);

            var putResult = await S3Service.PutObjectAsync(
                destBucket,
                destKey,
                body: jsonBytes,
                contentType: "application/json",
                region: region);

            return new ExportQueryToS3JsonResult(
                QueryExecutionId: queryId,
                RowCount: rows.Count,
                S3Bucket: destBucket,
                S3Key: destKey,
                ETag: putResult.ETag);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to export Athena query results to S3 as JSON");
        }
    }

    /// <summary>
    /// Start a Glue job and poll until it reaches a terminal state.
    /// </summary>
    /// <param name="jobName">Glue job name.</param>
    /// <param name="arguments">Optional Glue job arguments.</param>
    /// <param name="pollIntervalSeconds">Seconds between status polls.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<RunGlueJobResult> RunGlueJobAsync(
        string jobName,
        Dictionary<string, string>? arguments = null,
        int pollIntervalSeconds = 15,
        RegionEndpoint? region = null)
    {
        try
        {
            var startResult = await GlueService.StartJobRunAsync(
                jobName,
                arguments: arguments,
                region: region);

            var runId = startResult.JobRunId!;
            var startTime = DateTime.UtcNow;

            while (true)
            {
                var runResult = await GlueService.GetJobRunAsync(jobName, runId, region: region);
                var state = runResult.JobRun?.JobRunState?.Value;

                if (state != null && GlueTerminalStates.Contains(state))
                {
                    var elapsed = (int)(DateTime.UtcNow - startTime).TotalSeconds;
                    return new RunGlueJobResult(
                        JobRunId: runId,
                        JobRunState: state,
                        ElapsedSeconds: elapsed);
                }

                await Task.Delay(TimeSpan.FromSeconds(pollIntervalSeconds));
            }
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to run Glue job '{jobName}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="RunGlueThenQueryAsync"/>.</summary>
    public static GlueThenQueryResult RunGlueThenQuery(string jobName, string queryString, string database, string outputLocation, Dictionary<string, string>? jobArguments = null, int pollIntervalSeconds = 15, string? workGroup = null, RegionEndpoint? region = null)
        => RunGlueThenQueryAsync(jobName, queryString, database, outputLocation, jobArguments, pollIntervalSeconds, workGroup, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ExportQueryToS3JsonAsync"/>.</summary>
    public static ExportQueryToS3JsonResult ExportQueryToS3Json(string queryString, string database, string outputLocation, string destBucket, string destKey, string? workGroup = null, int pollIntervalSeconds = 5, RegionEndpoint? region = null)
        => ExportQueryToS3JsonAsync(queryString, database, outputLocation, destBucket, destKey, workGroup, pollIntervalSeconds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RunGlueJobAsync"/>.</summary>
    public static RunGlueJobResult RunGlueJob(string jobName, Dictionary<string, string>? arguments = null, int pollIntervalSeconds = 15, RegionEndpoint? region = null)
        => RunGlueJobAsync(jobName, arguments, pollIntervalSeconds, region).GetAwaiter().GetResult();

}

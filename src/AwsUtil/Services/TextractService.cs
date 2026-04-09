using Amazon;
using Amazon.Textract;
using Amazon.Textract.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of a synchronous document text detection.</summary>
public sealed record DetectDocumentTextResult(
    List<Block>? Blocks = null,
    DocumentMetadata? DocumentMetadata = null,
    string? DetectDocumentTextModelVersion = null);

/// <summary>Result of a synchronous document analysis.</summary>
public sealed record AnalyzeDocumentResult(
    List<Block>? Blocks = null,
    DocumentMetadata? DocumentMetadata = null,
    string? AnalyzeDocumentModelVersion = null,
    string? HumanLoopActivationOutput = null);

/// <summary>Result of a synchronous expense analysis.</summary>
public sealed record AnalyzeExpenseResult(
    List<ExpenseDocument>? ExpenseDocuments = null,
    DocumentMetadata? DocumentMetadata = null);

/// <summary>Result of a synchronous ID analysis.</summary>
public sealed record AnalyzeIdResult(
    List<IdentityDocument>? IdentityDocuments = null,
    DocumentMetadata? DocumentMetadata = null,
    string? AnalyzeIDModelVersion = null);

/// <summary>Result of starting an async job.</summary>
public sealed record StartTextractJobResult(string? JobId = null);

/// <summary>Result of a get document text detection job.</summary>
public sealed record GetDocumentTextDetectionResult(
    string? JobStatus = null,
    List<Block>? Blocks = null,
    DocumentMetadata? DocumentMetadata = null,
    string? NextToken = null,
    string? StatusMessage = null,
    string? DetectDocumentTextModelVersion = null);

/// <summary>Result of a get document analysis job.</summary>
public sealed record GetDocumentAnalysisResult(
    string? JobStatus = null,
    List<Block>? Blocks = null,
    DocumentMetadata? DocumentMetadata = null,
    string? NextToken = null,
    string? StatusMessage = null,
    string? AnalyzeDocumentModelVersion = null);

/// <summary>Result of a get expense analysis job.</summary>
public sealed record GetExpenseAnalysisResult(
    string? JobStatus = null,
    List<ExpenseDocument>? ExpenseDocuments = null,
    DocumentMetadata? DocumentMetadata = null,
    string? NextToken = null,
    string? StatusMessage = null);

/// <summary>Result of a get lending analysis job.</summary>
public sealed record GetLendingAnalysisResult(
    string? JobStatus = null,
    List<LendingResult>? Results = null,
    DocumentMetadata? DocumentMetadata = null,
    string? NextToken = null,
    string? StatusMessage = null,
    string? AnalyzeLendingModelVersion = null);

/// <summary>Result of a get lending analysis summary job.</summary>
public sealed record GetLendingAnalysisSummaryResult(
    string? JobStatus = null,
    LendingSummary? Summary = null,
    DocumentMetadata? DocumentMetadata = null,
    string? StatusMessage = null,
    string? AnalyzeLendingModelVersion = null);

/// <summary>Result of creating an adapter.</summary>
public sealed record CreateAdapterResult(string? AdapterId = null);

/// <summary>Result of getting an adapter.</summary>
public sealed record GetAdapterResult(
    string? AdapterId = null,
    string? AdapterName = null,
    string? Description = null,
    DateTime? CreationTime = null,
    List<string>? FeatureTypes = null,
    string? AutoUpdate = null);

/// <summary>Result of listing adapters.</summary>
public sealed record ListAdaptersResult(
    List<AdapterOverview>? Adapters = null,
    string? NextToken = null);

/// <summary>Result of updating an adapter.</summary>
public sealed record UpdateAdapterResult(
    string? AdapterId = null,
    string? AdapterName = null,
    string? Description = null,
    DateTime? CreationTime = null,
    List<string>? FeatureTypes = null,
    string? AutoUpdate = null);

/// <summary>Result of creating an adapter version.</summary>
public sealed record CreateAdapterVersionResult(
    string? AdapterId = null,
    string? AdapterVersion = null);

/// <summary>Result of getting an adapter version.</summary>
public sealed record GetAdapterVersionResult(
    string? AdapterId = null,
    string? AdapterVersion = null,
    string? Status = null,
    string? StatusMessage = null,
    DateTime? CreationTime = null,
    List<string>? FeatureTypes = null);

/// <summary>Result of listing adapter versions.</summary>
public sealed record ListAdapterVersionsResult(
    List<AdapterVersionOverview>? AdapterVersions = null,
    string? NextToken = null);

/// <summary>Result of listing tags for a resource.</summary>
public sealed record TextractListTagsResult(
    Dictionary<string, string>? Tags = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon Textract.
/// Port of the Python <c>aws_util.textract</c> module.
/// </summary>
public static class TextractService
{
    private static AmazonTextractClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonTextractClient>(region);

    // -----------------------------------------------------------------------
    // Synchronous operations
    // -----------------------------------------------------------------------

    /// <summary>
    /// Detect text in a document synchronously.
    /// </summary>
    public static async Task<DetectDocumentTextResult> DetectDocumentTextAsync(
        Document document,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectDocumentTextRequest { Document = document };

        try
        {
            var resp = await client.DetectDocumentTextAsync(request);
            return new DetectDocumentTextResult(
                Blocks: resp.Blocks,
                DocumentMetadata: resp.DocumentMetadata,
                DetectDocumentTextModelVersion: resp.DetectDocumentTextModelVersion);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to detect document text");
        }
    }

    /// <summary>
    /// Analyze a document synchronously for forms, tables, queries, or signatures.
    /// </summary>
    public static async Task<AnalyzeDocumentResult> AnalyzeDocumentAsync(
        Document document,
        List<string> featureTypes,
        HumanLoopConfig? humanLoopConfig = null,
        QueriesConfig? queriesConfig = null,
        AdaptersConfig? adaptersConfig = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AnalyzeDocumentRequest
        {
            Document = document,
            FeatureTypes = featureTypes
        };
        if (humanLoopConfig != null) request.HumanLoopConfig = humanLoopConfig;
        if (queriesConfig != null) request.QueriesConfig = queriesConfig;
        if (adaptersConfig != null) request.AdaptersConfig = adaptersConfig;

        try
        {
            var resp = await client.AnalyzeDocumentAsync(request);
            return new AnalyzeDocumentResult(
                Blocks: resp.Blocks,
                DocumentMetadata: resp.DocumentMetadata,
                AnalyzeDocumentModelVersion: resp.AnalyzeDocumentModelVersion);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to analyze document");
        }
    }

    /// <summary>
    /// Analyze expense (invoice/receipt) data synchronously.
    /// </summary>
    public static async Task<AnalyzeExpenseResult> AnalyzeExpenseAsync(
        Document document,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AnalyzeExpenseRequest { Document = document };

        try
        {
            var resp = await client.AnalyzeExpenseAsync(request);
            return new AnalyzeExpenseResult(
                ExpenseDocuments: resp.ExpenseDocuments,
                DocumentMetadata: resp.DocumentMetadata);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to analyze expense document");
        }
    }

    /// <summary>
    /// Analyze identity documents (driver's license, passport) synchronously.
    /// </summary>
    public static async Task<AnalyzeIdResult> AnalyzeIDAsync(
        List<Document> documentPages,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new AnalyzeIDRequest { DocumentPages = documentPages };

        try
        {
            var resp = await client.AnalyzeIDAsync(request);
            return new AnalyzeIdResult(
                IdentityDocuments: resp.IdentityDocuments,
                DocumentMetadata: resp.DocumentMetadata,
                AnalyzeIDModelVersion: resp.AnalyzeIDModelVersion);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to analyze ID document");
        }
    }

    // -----------------------------------------------------------------------
    // Async document text detection
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start an asynchronous document text detection job.
    /// </summary>
    public static async Task<StartTextractJobResult> StartDocumentTextDetectionAsync(
        DocumentLocation documentLocation,
        string? clientRequestToken = null,
        string? jobTag = null,
        NotificationChannel? notificationChannel = null,
        OutputConfig? outputConfig = null,
        string? kmsKeyId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartDocumentTextDetectionRequest
        {
            DocumentLocation = documentLocation
        };
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (jobTag != null) request.JobTag = jobTag;
        if (notificationChannel != null) request.NotificationChannel = notificationChannel;
        if (outputConfig != null) request.OutputConfig = outputConfig;
        if (kmsKeyId != null) request.KMSKeyId = kmsKeyId;

        try
        {
            var resp = await client.StartDocumentTextDetectionAsync(request);
            return new StartTextractJobResult(JobId: resp.JobId);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start document text detection job");
        }
    }

    /// <summary>
    /// Get results of an asynchronous document text detection job.
    /// </summary>
    public static async Task<GetDocumentTextDetectionResult> GetDocumentTextDetectionAsync(
        string jobId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetDocumentTextDetectionRequest { JobId = jobId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetDocumentTextDetectionAsync(request);
            return new GetDocumentTextDetectionResult(
                JobStatus: resp.JobStatus?.Value,
                Blocks: resp.Blocks,
                DocumentMetadata: resp.DocumentMetadata,
                NextToken: resp.NextToken,
                StatusMessage: resp.StatusMessage,
                DetectDocumentTextModelVersion: resp.DetectDocumentTextModelVersion);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get document text detection results for job '{jobId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Async document analysis
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start an asynchronous document analysis job.
    /// </summary>
    public static async Task<StartTextractJobResult> StartDocumentAnalysisAsync(
        DocumentLocation documentLocation,
        List<string> featureTypes,
        string? clientRequestToken = null,
        string? jobTag = null,
        NotificationChannel? notificationChannel = null,
        OutputConfig? outputConfig = null,
        string? kmsKeyId = null,
        QueriesConfig? queriesConfig = null,
        AdaptersConfig? adaptersConfig = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartDocumentAnalysisRequest
        {
            DocumentLocation = documentLocation,
            FeatureTypes = featureTypes
        };
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (jobTag != null) request.JobTag = jobTag;
        if (notificationChannel != null) request.NotificationChannel = notificationChannel;
        if (outputConfig != null) request.OutputConfig = outputConfig;
        if (kmsKeyId != null) request.KMSKeyId = kmsKeyId;
        if (queriesConfig != null) request.QueriesConfig = queriesConfig;
        if (adaptersConfig != null) request.AdaptersConfig = adaptersConfig;

        try
        {
            var resp = await client.StartDocumentAnalysisAsync(request);
            return new StartTextractJobResult(JobId: resp.JobId);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start document analysis job");
        }
    }

    /// <summary>
    /// Get results of an asynchronous document analysis job.
    /// </summary>
    public static async Task<GetDocumentAnalysisResult> GetDocumentAnalysisAsync(
        string jobId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetDocumentAnalysisRequest { JobId = jobId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetDocumentAnalysisAsync(request);
            return new GetDocumentAnalysisResult(
                JobStatus: resp.JobStatus?.Value,
                Blocks: resp.Blocks,
                DocumentMetadata: resp.DocumentMetadata,
                NextToken: resp.NextToken,
                StatusMessage: resp.StatusMessage,
                AnalyzeDocumentModelVersion: resp.AnalyzeDocumentModelVersion);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get document analysis results for job '{jobId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Async expense analysis
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start an asynchronous expense analysis job.
    /// </summary>
    public static async Task<StartTextractJobResult> StartExpenseAnalysisAsync(
        DocumentLocation documentLocation,
        string? clientRequestToken = null,
        string? jobTag = null,
        NotificationChannel? notificationChannel = null,
        OutputConfig? outputConfig = null,
        string? kmsKeyId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartExpenseAnalysisRequest
        {
            DocumentLocation = documentLocation
        };
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (jobTag != null) request.JobTag = jobTag;
        if (notificationChannel != null) request.NotificationChannel = notificationChannel;
        if (outputConfig != null) request.OutputConfig = outputConfig;
        if (kmsKeyId != null) request.KMSKeyId = kmsKeyId;

        try
        {
            var resp = await client.StartExpenseAnalysisAsync(request);
            return new StartTextractJobResult(JobId: resp.JobId);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start expense analysis job");
        }
    }

    /// <summary>
    /// Get results of an asynchronous expense analysis job.
    /// </summary>
    public static async Task<GetExpenseAnalysisResult> GetExpenseAnalysisAsync(
        string jobId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetExpenseAnalysisRequest { JobId = jobId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetExpenseAnalysisAsync(request);
            return new GetExpenseAnalysisResult(
                JobStatus: resp.JobStatus?.Value,
                ExpenseDocuments: resp.ExpenseDocuments,
                DocumentMetadata: resp.DocumentMetadata,
                NextToken: resp.NextToken,
                StatusMessage: resp.StatusMessage);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get expense analysis results for job '{jobId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Async lending analysis
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start an asynchronous lending analysis job.
    /// </summary>
    public static async Task<StartTextractJobResult> StartLendingAnalysisAsync(
        DocumentLocation documentLocation,
        string? clientRequestToken = null,
        string? jobTag = null,
        NotificationChannel? notificationChannel = null,
        OutputConfig? outputConfig = null,
        string? kmsKeyId = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartLendingAnalysisRequest
        {
            DocumentLocation = documentLocation
        };
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (jobTag != null) request.JobTag = jobTag;
        if (notificationChannel != null) request.NotificationChannel = notificationChannel;
        if (outputConfig != null) request.OutputConfig = outputConfig;
        if (kmsKeyId != null) request.KMSKeyId = kmsKeyId;

        try
        {
            var resp = await client.StartLendingAnalysisAsync(request);
            return new StartTextractJobResult(JobId: resp.JobId);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start lending analysis job");
        }
    }

    /// <summary>
    /// Get results of an asynchronous lending analysis job.
    /// </summary>
    public static async Task<GetLendingAnalysisResult> GetLendingAnalysisAsync(
        string jobId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetLendingAnalysisRequest { JobId = jobId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetLendingAnalysisAsync(request);
            return new GetLendingAnalysisResult(
                JobStatus: resp.JobStatus?.Value,
                Results: resp.Results,
                DocumentMetadata: resp.DocumentMetadata,
                NextToken: resp.NextToken,
                StatusMessage: resp.StatusMessage,
                AnalyzeLendingModelVersion: resp.AnalyzeLendingModelVersion);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get lending analysis results for job '{jobId}'");
        }
    }

    /// <summary>
    /// Get the summary of an asynchronous lending analysis job.
    /// </summary>
    public static async Task<GetLendingAnalysisSummaryResult>
        GetLendingAnalysisSummaryAsync(
            string jobId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetLendingAnalysisSummaryRequest { JobId = jobId };

        try
        {
            var resp = await client.GetLendingAnalysisSummaryAsync(request);
            return new GetLendingAnalysisSummaryResult(
                JobStatus: resp.JobStatus?.Value,
                Summary: resp.Summary,
                DocumentMetadata: resp.DocumentMetadata,
                StatusMessage: resp.StatusMessage,
                AnalyzeLendingModelVersion: resp.AnalyzeLendingModelVersion);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get lending analysis summary for job '{jobId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Adapter management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a new Textract adapter.
    /// </summary>
    public static async Task<CreateAdapterResult> CreateAdapterAsync(
        string adapterName,
        List<string> featureTypes,
        string? description = null,
        string? autoUpdate = null,
        string? clientRequestToken = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAdapterRequest
        {
            AdapterName = adapterName,
            FeatureTypes = featureTypes
        };
        if (description != null) request.Description = description;
        if (autoUpdate != null) request.AutoUpdate = autoUpdate;
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateAdapterAsync(request);
            return new CreateAdapterResult(AdapterId: resp.AdapterId);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create adapter '{adapterName}'");
        }
    }

    /// <summary>
    /// Delete a Textract adapter.
    /// </summary>
    public static async Task DeleteAdapterAsync(
        string adapterId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteAdapterRequest { AdapterId = adapterId };

        try
        {
            await client.DeleteAdapterAsync(request);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete adapter '{adapterId}'");
        }
    }

    /// <summary>
    /// Get details of a Textract adapter.
    /// </summary>
    public static async Task<GetAdapterResult> GetAdapterAsync(
        string adapterId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetAdapterRequest { AdapterId = adapterId };

        try
        {
            var resp = await client.GetAdapterAsync(request);
            return new GetAdapterResult(
                AdapterId: resp.AdapterId,
                AdapterName: resp.AdapterName,
                Description: resp.Description,
                CreationTime: resp.CreationTime,
                FeatureTypes: resp.FeatureTypes,
                AutoUpdate: resp.AutoUpdate?.Value);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get adapter '{adapterId}'");
        }
    }

    /// <summary>
    /// List Textract adapters.
    /// </summary>
    public static async Task<ListAdaptersResult> ListAdaptersAsync(
        int? maxResults = null,
        string? nextToken = null,
        DateTime? afterCreationTime = null,
        DateTime? beforeCreationTime = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAdaptersRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;
        if (afterCreationTime.HasValue) request.AfterCreationTime = afterCreationTime.Value;
        if (beforeCreationTime.HasValue) request.BeforeCreationTime = beforeCreationTime.Value;

        try
        {
            var resp = await client.ListAdaptersAsync(request);
            return new ListAdaptersResult(
                Adapters: resp.Adapters,
                NextToken: resp.NextToken);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list adapters");
        }
    }

    /// <summary>
    /// Update a Textract adapter.
    /// </summary>
    public static async Task<UpdateAdapterResult> UpdateAdapterAsync(
        string adapterId,
        string? adapterName = null,
        string? description = null,
        string? autoUpdate = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateAdapterRequest { AdapterId = adapterId };
        if (adapterName != null) request.AdapterName = adapterName;
        if (description != null) request.Description = description;
        if (autoUpdate != null) request.AutoUpdate = autoUpdate;

        try
        {
            var resp = await client.UpdateAdapterAsync(request);
            return new UpdateAdapterResult(
                AdapterId: resp.AdapterId,
                AdapterName: resp.AdapterName,
                Description: resp.Description,
                CreationTime: resp.CreationTime,
                FeatureTypes: resp.FeatureTypes,
                AutoUpdate: resp.AutoUpdate?.Value);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update adapter '{adapterId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Adapter version management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a new version of a Textract adapter.
    /// </summary>
    public static async Task<CreateAdapterVersionResult> CreateAdapterVersionAsync(
        string adapterId,
        AdapterVersionDatasetConfig datasetConfig,
        OutputConfig? outputConfig = null,
        string? clientRequestToken = null,
        string? kmsKeyId = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAdapterVersionRequest
        {
            AdapterId = adapterId,
            DatasetConfig = datasetConfig
        };
        if (outputConfig != null) request.OutputConfig = outputConfig;
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (kmsKeyId != null) request.KMSKeyId = kmsKeyId;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateAdapterVersionAsync(request);
            return new CreateAdapterVersionResult(
                AdapterId: resp.AdapterId,
                AdapterVersion: resp.AdapterVersion);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create adapter version for '{adapterId}'");
        }
    }

    /// <summary>
    /// Delete a version of a Textract adapter.
    /// </summary>
    public static async Task DeleteAdapterVersionAsync(
        string adapterId,
        string adapterVersion,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteAdapterVersionRequest
        {
            AdapterId = adapterId,
            AdapterVersion = adapterVersion
        };

        try
        {
            await client.DeleteAdapterVersionAsync(request);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete adapter version '{adapterVersion}' for '{adapterId}'");
        }
    }

    /// <summary>
    /// Get details of a Textract adapter version.
    /// </summary>
    public static async Task<GetAdapterVersionResult> GetAdapterVersionAsync(
        string adapterId,
        string adapterVersion,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetAdapterVersionRequest
        {
            AdapterId = adapterId,
            AdapterVersion = adapterVersion
        };

        try
        {
            var resp = await client.GetAdapterVersionAsync(request);
            return new GetAdapterVersionResult(
                AdapterId: resp.AdapterId,
                AdapterVersion: resp.AdapterVersion,
                Status: resp.Status?.Value,
                StatusMessage: resp.StatusMessage,
                CreationTime: resp.CreationTime,
                FeatureTypes: resp.FeatureTypes);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get adapter version '{adapterVersion}' for '{adapterId}'");
        }
    }

    /// <summary>
    /// List versions of a Textract adapter.
    /// </summary>
    public static async Task<ListAdapterVersionsResult> ListAdapterVersionsAsync(
        string? adapterId = null,
        int? maxResults = null,
        string? nextToken = null,
        DateTime? afterCreationTime = null,
        DateTime? beforeCreationTime = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListAdapterVersionsRequest();
        if (adapterId != null) request.AdapterId = adapterId;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;
        if (afterCreationTime.HasValue) request.AfterCreationTime = afterCreationTime.Value;
        if (beforeCreationTime.HasValue) request.BeforeCreationTime = beforeCreationTime.Value;

        try
        {
            var resp = await client.ListAdapterVersionsAsync(request);
            return new ListAdapterVersionsResult(
                AdapterVersions: resp.AdapterVersions,
                NextToken: resp.NextToken);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list adapter versions");
        }
    }

    // -----------------------------------------------------------------------
    // Tagging
    // -----------------------------------------------------------------------

    /// <summary>
    /// Add tags to a Textract resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceArn,
        Dictionary<string, string> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new TagResourceRequest
        {
            ResourceARN = resourceArn,
            Tags = tags
        };

        try
        {
            await client.TagResourceAsync(request);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Textract resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Textract resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UntagResourceRequest
        {
            ResourceARN = resourceArn,
            TagKeys = tagKeys
        };

        try
        {
            await client.UntagResourceAsync(request);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Textract resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Textract resource.
    /// </summary>
    public static async Task<TextractListTagsResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForResourceRequest
        {
            ResourceARN = resourceArn
        };

        try
        {
            var resp = await client.ListTagsForResourceAsync(request);
            return new TextractListTagsResult(Tags: resp.Tags);
        }
        catch (AmazonTextractException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Textract resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="DetectDocumentTextAsync"/>.</summary>
    public static DetectDocumentTextResult DetectDocumentText(Document document, RegionEndpoint? region = null)
        => DetectDocumentTextAsync(document, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AnalyzeDocumentAsync"/>.</summary>
    public static AnalyzeDocumentResult AnalyzeDocument(Document document, List<string> featureTypes, HumanLoopConfig? humanLoopConfig = null, QueriesConfig? queriesConfig = null, AdaptersConfig? adaptersConfig = null, RegionEndpoint? region = null)
        => AnalyzeDocumentAsync(document, featureTypes, humanLoopConfig, queriesConfig, adaptersConfig, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AnalyzeExpenseAsync"/>.</summary>
    public static AnalyzeExpenseResult AnalyzeExpense(Document document, RegionEndpoint? region = null)
        => AnalyzeExpenseAsync(document, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AnalyzeIDAsync"/>.</summary>
    public static AnalyzeIdResult AnalyzeID(List<Document> documentPages, RegionEndpoint? region = null)
        => AnalyzeIDAsync(documentPages, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartDocumentTextDetectionAsync"/>.</summary>
    public static StartTextractJobResult StartDocumentTextDetection(DocumentLocation documentLocation, string? clientRequestToken = null, string? jobTag = null, NotificationChannel? notificationChannel = null, OutputConfig? outputConfig = null, string? kmsKeyId = null, RegionEndpoint? region = null)
        => StartDocumentTextDetectionAsync(documentLocation, clientRequestToken, jobTag, notificationChannel, outputConfig, kmsKeyId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDocumentTextDetectionAsync"/>.</summary>
    public static GetDocumentTextDetectionResult GetDocumentTextDetection(string jobId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetDocumentTextDetectionAsync(jobId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartDocumentAnalysisAsync"/>.</summary>
    public static StartTextractJobResult StartDocumentAnalysis(DocumentLocation documentLocation, List<string> featureTypes, string? clientRequestToken = null, string? jobTag = null, NotificationChannel? notificationChannel = null, OutputConfig? outputConfig = null, string? kmsKeyId = null, QueriesConfig? queriesConfig = null, AdaptersConfig? adaptersConfig = null, RegionEndpoint? region = null)
        => StartDocumentAnalysisAsync(documentLocation, featureTypes, clientRequestToken, jobTag, notificationChannel, outputConfig, kmsKeyId, queriesConfig, adaptersConfig, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDocumentAnalysisAsync"/>.</summary>
    public static GetDocumentAnalysisResult GetDocumentAnalysis(string jobId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetDocumentAnalysisAsync(jobId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartExpenseAnalysisAsync"/>.</summary>
    public static StartTextractJobResult StartExpenseAnalysis(DocumentLocation documentLocation, string? clientRequestToken = null, string? jobTag = null, NotificationChannel? notificationChannel = null, OutputConfig? outputConfig = null, string? kmsKeyId = null, RegionEndpoint? region = null)
        => StartExpenseAnalysisAsync(documentLocation, clientRequestToken, jobTag, notificationChannel, outputConfig, kmsKeyId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetExpenseAnalysisAsync"/>.</summary>
    public static GetExpenseAnalysisResult GetExpenseAnalysis(string jobId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetExpenseAnalysisAsync(jobId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartLendingAnalysisAsync"/>.</summary>
    public static StartTextractJobResult StartLendingAnalysis(DocumentLocation documentLocation, string? clientRequestToken = null, string? jobTag = null, NotificationChannel? notificationChannel = null, OutputConfig? outputConfig = null, string? kmsKeyId = null, RegionEndpoint? region = null)
        => StartLendingAnalysisAsync(documentLocation, clientRequestToken, jobTag, notificationChannel, outputConfig, kmsKeyId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetLendingAnalysisAsync"/>.</summary>
    public static GetLendingAnalysisResult GetLendingAnalysis(string jobId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetLendingAnalysisAsync(jobId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetLendingAnalysisSummaryAsync"/>.</summary>
    public static GetLendingAnalysisSummaryResult GetLendingAnalysisSummary(string jobId, RegionEndpoint? region = null)
        => GetLendingAnalysisSummaryAsync(jobId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateAdapterAsync"/>.</summary>
    public static CreateAdapterResult CreateAdapter(string adapterName, List<string> featureTypes, string? description = null, string? autoUpdate = null, string? clientRequestToken = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateAdapterAsync(adapterName, featureTypes, description, autoUpdate, clientRequestToken, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAdapterAsync"/>.</summary>
    public static void DeleteAdapter(string adapterId, RegionEndpoint? region = null)
        => DeleteAdapterAsync(adapterId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAdapterAsync"/>.</summary>
    public static GetAdapterResult GetAdapter(string adapterId, RegionEndpoint? region = null)
        => GetAdapterAsync(adapterId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAdaptersAsync"/>.</summary>
    public static ListAdaptersResult ListAdapters(int? maxResults = null, string? nextToken = null, DateTime? afterCreationTime = null, DateTime? beforeCreationTime = null, RegionEndpoint? region = null)
        => ListAdaptersAsync(maxResults, nextToken, afterCreationTime, beforeCreationTime, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateAdapterAsync"/>.</summary>
    public static UpdateAdapterResult UpdateAdapter(string adapterId, string? adapterName = null, string? description = null, string? autoUpdate = null, RegionEndpoint? region = null)
        => UpdateAdapterAsync(adapterId, adapterName, description, autoUpdate, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateAdapterVersionAsync"/>.</summary>
    public static CreateAdapterVersionResult CreateAdapterVersion(string adapterId, AdapterVersionDatasetConfig datasetConfig, OutputConfig? outputConfig = null, string? clientRequestToken = null, string? kmsKeyId = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateAdapterVersionAsync(adapterId, datasetConfig, outputConfig, clientRequestToken, kmsKeyId, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAdapterVersionAsync"/>.</summary>
    public static void DeleteAdapterVersion(string adapterId, string adapterVersion, RegionEndpoint? region = null)
        => DeleteAdapterVersionAsync(adapterId, adapterVersion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAdapterVersionAsync"/>.</summary>
    public static GetAdapterVersionResult GetAdapterVersion(string adapterId, string adapterVersion, RegionEndpoint? region = null)
        => GetAdapterVersionAsync(adapterId, adapterVersion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAdapterVersionsAsync"/>.</summary>
    public static ListAdapterVersionsResult ListAdapterVersions(string? adapterId = null, int? maxResults = null, string? nextToken = null, DateTime? afterCreationTime = null, DateTime? beforeCreationTime = null, RegionEndpoint? region = null)
        => ListAdapterVersionsAsync(adapterId, maxResults, nextToken, afterCreationTime, beforeCreationTime, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static TextractListTagsResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}

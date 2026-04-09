using Amazon;
using Amazon.Translate;
using Amazon.Translate.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of a text translation.</summary>
public sealed record TranslateTextResult(
    string? TranslatedText = null,
    string? SourceLanguageCode = null,
    string? TargetLanguageCode = null,
    List<AppliedTerminology>? AppliedTerminologies = null,
    TranslationSettings? AppliedSettings = null);

/// <summary>Result of a document translation.</summary>
public sealed record TranslateDocumentResult(
    TranslatedDocument? TranslatedDocument = null,
    string? SourceLanguageCode = null,
    string? TargetLanguageCode = null,
    List<AppliedTerminology>? AppliedTerminologies = null,
    TranslationSettings? AppliedSettings = null);

/// <summary>Result of starting a text translation job.</summary>
public sealed record StartTextTranslationJobResult(
    string? JobId = null,
    string? JobStatus = null);

/// <summary>Result of stopping a text translation job.</summary>
public sealed record StopTextTranslationJobResult(
    string? JobId = null,
    string? JobStatus = null);

/// <summary>Result of describing a text translation job.</summary>
public sealed record DescribeTextTranslationJobResult(
    TextTranslationJobProperties? JobProperties = null);

/// <summary>Result of listing text translation jobs.</summary>
public sealed record ListTextTranslationJobsResult(
    List<TextTranslationJobProperties>? Jobs = null,
    string? NextToken = null);

/// <summary>Result of creating parallel data.</summary>
public sealed record CreateParallelDataResult(
    string? Name = null,
    string? Status = null);

/// <summary>Result of getting parallel data.</summary>
public sealed record GetParallelDataResult(
    ParallelDataProperties? Properties = null,
    ParallelDataDataLocation? DataLocation = null,
    ParallelDataDataLocation? AuxiliaryDataLocation = null,
    ParallelDataDataLocation? LatestUpdateAttemptAuxiliaryDataLocation = null);

/// <summary>Result of listing parallel data resources.</summary>
public sealed record ListParallelDataResult(
    List<ParallelDataProperties>? ParallelDataPropertiesList = null,
    string? NextToken = null);

/// <summary>Result of updating parallel data.</summary>
public sealed record UpdateParallelDataResult(
    string? Name = null,
    string? Status = null,
    string? LatestUpdateAttemptStatus = null,
    DateTime? LatestUpdateAttemptAt = null);

/// <summary>Result of importing a terminology.</summary>
public sealed record ImportTerminologyResult(
    TerminologyProperties? Properties = null,
    TerminologyDataLocation? AuxiliaryDataLocation = null);

/// <summary>Result of getting a terminology.</summary>
public sealed record GetTerminologyResult(
    TerminologyProperties? Properties = null,
    TerminologyDataLocation? DataLocation = null,
    TerminologyDataLocation? AuxiliaryDataLocation = null);

/// <summary>Result of listing terminologies.</summary>
public sealed record ListTerminologiesResult(
    List<TerminologyProperties>? TerminologyPropertiesList = null,
    string? NextToken = null);

/// <summary>Result of listing supported languages.</summary>
public sealed record ListLanguagesResult(
    List<Language>? Languages = null,
    string? DisplayLanguageCode = null,
    string? NextToken = null);

/// <summary>Result of listing tags for a Translate resource.</summary>
public sealed record TranslateListTagsResult(
    List<Tag>? Tags = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon Translate.
/// Port of the Python <c>aws_util.translate</c> module.
/// </summary>
public static class TranslateService
{
    private static AmazonTranslateClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonTranslateClient>(region);

    // -----------------------------------------------------------------------
    // Real-time translation
    // -----------------------------------------------------------------------

    /// <summary>
    /// Translate text between languages.
    /// </summary>
    public static async Task<TranslateTextResult> TranslateTextAsync(
        string text,
        string sourceLanguageCode,
        string targetLanguageCode,
        List<string>? terminologyNames = null,
        TranslationSettings? settings = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new TranslateTextRequest
        {
            Text = text,
            SourceLanguageCode = sourceLanguageCode,
            TargetLanguageCode = targetLanguageCode
        };
        if (terminologyNames != null) request.TerminologyNames = terminologyNames;
        if (settings != null) request.Settings = settings;

        try
        {
            var resp = await client.TranslateTextAsync(request);
            return new TranslateTextResult(
                TranslatedText: resp.TranslatedText,
                SourceLanguageCode: resp.SourceLanguageCode,
                TargetLanguageCode: resp.TargetLanguageCode,
                AppliedTerminologies: resp.AppliedTerminologies,
                AppliedSettings: resp.AppliedSettings);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to translate text from '{sourceLanguageCode}' to '{targetLanguageCode}'");
        }
    }

    /// <summary>
    /// Translate a document.
    /// </summary>
    public static async Task<TranslateDocumentResult> TranslateDocumentAsync(
        Amazon.Translate.Model.Document document,
        string sourceLanguageCode,
        string targetLanguageCode,
        List<string>? terminologyNames = null,
        TranslationSettings? settings = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new TranslateDocumentRequest
        {
            Document = document,
            SourceLanguageCode = sourceLanguageCode,
            TargetLanguageCode = targetLanguageCode
        };
        if (terminologyNames != null) request.TerminologyNames = terminologyNames;
        if (settings != null) request.Settings = settings;

        try
        {
            var resp = await client.TranslateDocumentAsync(request);
            return new TranslateDocumentResult(
                TranslatedDocument: resp.TranslatedDocument,
                SourceLanguageCode: resp.SourceLanguageCode,
                TargetLanguageCode: resp.TargetLanguageCode,
                AppliedTerminologies: resp.AppliedTerminologies,
                AppliedSettings: resp.AppliedSettings);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to translate document from '{sourceLanguageCode}' to '{targetLanguageCode}'");
        }
    }

    // -----------------------------------------------------------------------
    // Batch translation jobs
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start an asynchronous text translation job.
    /// </summary>
    public static async Task<StartTextTranslationJobResult>
        StartTextTranslationJobAsync(
            InputDataConfig inputDataConfig,
            OutputDataConfig outputDataConfig,
            string dataAccessRoleArn,
            string sourceLanguageCode,
            List<string> targetLanguageCodes,
            string? jobName = null,
            List<string>? terminologyNames = null,
            List<string>? parallelDataNames = null,
            string? clientToken = null,
            TranslationSettings? settings = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartTextTranslationJobRequest
        {
            InputDataConfig = inputDataConfig,
            OutputDataConfig = outputDataConfig,
            DataAccessRoleArn = dataAccessRoleArn,
            SourceLanguageCode = sourceLanguageCode,
            TargetLanguageCodes = targetLanguageCodes
        };
        if (jobName != null) request.JobName = jobName;
        if (terminologyNames != null) request.TerminologyNames = terminologyNames;
        if (parallelDataNames != null) request.ParallelDataNames = parallelDataNames;
        if (clientToken != null) request.ClientToken = clientToken;
        if (settings != null) request.Settings = settings;

        try
        {
            var resp = await client.StartTextTranslationJobAsync(request);
            return new StartTextTranslationJobResult(
                JobId: resp.JobId,
                JobStatus: resp.JobStatus?.Value);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start text translation job");
        }
    }

    /// <summary>
    /// Stop an asynchronous text translation job.
    /// </summary>
    public static async Task<StopTextTranslationJobResult>
        StopTextTranslationJobAsync(
            string jobId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopTextTranslationJobRequest { JobId = jobId };

        try
        {
            var resp = await client.StopTextTranslationJobAsync(request);
            return new StopTextTranslationJobResult(
                JobId: resp.JobId,
                JobStatus: resp.JobStatus?.Value);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop text translation job '{jobId}'");
        }
    }

    /// <summary>
    /// Describe a text translation job.
    /// </summary>
    public static async Task<DescribeTextTranslationJobResult>
        DescribeTextTranslationJobAsync(
            string jobId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeTextTranslationJobRequest { JobId = jobId };

        try
        {
            var resp = await client.DescribeTextTranslationJobAsync(request);
            return new DescribeTextTranslationJobResult(
                JobProperties: resp.TextTranslationJobProperties);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe text translation job '{jobId}'");
        }
    }

    /// <summary>
    /// List text translation jobs.
    /// </summary>
    public static async Task<ListTextTranslationJobsResult>
        ListTextTranslationJobsAsync(
            TextTranslationJobFilter? filter = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTextTranslationJobsRequest();
        if (filter != null) request.Filter = filter;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListTextTranslationJobsAsync(request);
            return new ListTextTranslationJobsResult(
                Jobs: resp.TextTranslationJobPropertiesList,
                NextToken: resp.NextToken);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list text translation jobs");
        }
    }

    // -----------------------------------------------------------------------
    // Parallel data
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a parallel data resource.
    /// </summary>
    public static async Task<CreateParallelDataResult> CreateParallelDataAsync(
        string name,
        ParallelDataConfig parallelDataConfig,
        string? description = null,
        EncryptionKey? encryptionKey = null,
        string? clientToken = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateParallelDataRequest
        {
            Name = name,
            ParallelDataConfig = parallelDataConfig
        };
        if (description != null) request.Description = description;
        if (encryptionKey != null) request.EncryptionKey = encryptionKey;
        if (clientToken != null) request.ClientToken = clientToken;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateParallelDataAsync(request);
            return new CreateParallelDataResult(
                Name: resp.Name,
                Status: resp.Status?.Value);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create parallel data '{name}'");
        }
    }

    /// <summary>
    /// Delete a parallel data resource.
    /// </summary>
    public static async Task DeleteParallelDataAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteParallelDataRequest { Name = name };

        try
        {
            await client.DeleteParallelDataAsync(request);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete parallel data '{name}'");
        }
    }

    /// <summary>
    /// Get a parallel data resource.
    /// </summary>
    public static async Task<GetParallelDataResult> GetParallelDataAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetParallelDataRequest { Name = name };

        try
        {
            var resp = await client.GetParallelDataAsync(request);
            return new GetParallelDataResult(
                Properties: resp.ParallelDataProperties,
                DataLocation: resp.DataLocation,
                AuxiliaryDataLocation: resp.AuxiliaryDataLocation,
                LatestUpdateAttemptAuxiliaryDataLocation:
                    resp.LatestUpdateAttemptAuxiliaryDataLocation);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get parallel data '{name}'");
        }
    }

    /// <summary>
    /// List parallel data resources.
    /// </summary>
    public static async Task<ListParallelDataResult> ListParallelDataAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListParallelDataRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListParallelDataAsync(request);
            return new ListParallelDataResult(
                ParallelDataPropertiesList: resp.ParallelDataPropertiesList,
                NextToken: resp.NextToken);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list parallel data");
        }
    }

    /// <summary>
    /// Update a parallel data resource.
    /// </summary>
    public static async Task<UpdateParallelDataResult> UpdateParallelDataAsync(
        string name,
        ParallelDataConfig parallelDataConfig,
        string clientToken,
        string? description = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateParallelDataRequest
        {
            Name = name,
            ParallelDataConfig = parallelDataConfig,
            ClientToken = clientToken
        };
        if (description != null) request.Description = description;

        try
        {
            var resp = await client.UpdateParallelDataAsync(request);
            return new UpdateParallelDataResult(
                Name: resp.Name,
                Status: resp.Status?.Value,
                LatestUpdateAttemptStatus: resp.LatestUpdateAttemptStatus?.Value,
                LatestUpdateAttemptAt: resp.LatestUpdateAttemptAt);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update parallel data '{name}'");
        }
    }

    // -----------------------------------------------------------------------
    // Terminology
    // -----------------------------------------------------------------------

    /// <summary>
    /// Import a terminology resource.
    /// </summary>
    public static async Task<ImportTerminologyResult> ImportTerminologyAsync(
        string name,
        string mergeStrategy,
        TerminologyData terminologyData,
        string? description = null,
        EncryptionKey? encryptionKey = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ImportTerminologyRequest
        {
            Name = name,
            MergeStrategy = mergeStrategy,
            TerminologyData = terminologyData
        };
        if (description != null) request.Description = description;
        if (encryptionKey != null) request.EncryptionKey = encryptionKey;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.ImportTerminologyAsync(request);
            return new ImportTerminologyResult(
                Properties: resp.TerminologyProperties,
                AuxiliaryDataLocation: resp.AuxiliaryDataLocation);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to import terminology '{name}'");
        }
    }

    /// <summary>
    /// Delete a terminology resource.
    /// </summary>
    public static async Task DeleteTerminologyAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteTerminologyRequest { Name = name };

        try
        {
            await client.DeleteTerminologyAsync(request);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete terminology '{name}'");
        }
    }

    /// <summary>
    /// Get a terminology resource.
    /// </summary>
    public static async Task<GetTerminologyResult> GetTerminologyAsync(
        string name,
        string? terminologyDataFormat = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetTerminologyRequest { Name = name };
        if (terminologyDataFormat != null)
            request.TerminologyDataFormat = terminologyDataFormat;

        try
        {
            var resp = await client.GetTerminologyAsync(request);
            return new GetTerminologyResult(
                Properties: resp.TerminologyProperties,
                DataLocation: resp.TerminologyDataLocation,
                AuxiliaryDataLocation: resp.AuxiliaryDataLocation);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get terminology '{name}'");
        }
    }

    /// <summary>
    /// List terminology resources.
    /// </summary>
    public static async Task<ListTerminologiesResult> ListTerminologiesAsync(
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTerminologiesRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListTerminologiesAsync(request);
            return new ListTerminologiesResult(
                TerminologyPropertiesList: resp.TerminologyPropertiesList,
                NextToken: resp.NextToken);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list terminologies");
        }
    }

    // -----------------------------------------------------------------------
    // Languages
    // -----------------------------------------------------------------------

    /// <summary>
    /// List supported languages for translation.
    /// </summary>
    public static async Task<ListLanguagesResult> ListLanguagesAsync(
        string? displayLanguageCode = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListLanguagesRequest();
        if (displayLanguageCode != null) request.DisplayLanguageCode = displayLanguageCode;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListLanguagesAsync(request);
            return new ListLanguagesResult(
                Languages: resp.Languages,
                DisplayLanguageCode: resp.DisplayLanguageCode,
                NextToken: resp.NextToken);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list languages");
        }
    }

    // -----------------------------------------------------------------------
    // Tagging
    // -----------------------------------------------------------------------

    /// <summary>
    /// Add tags to a Translate resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceArn,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new TagResourceRequest
        {
            ResourceArn = resourceArn,
            Tags = tags
        };

        try
        {
            await client.TagResourceAsync(request);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Translate resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Translate resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UntagResourceRequest
        {
            ResourceArn = resourceArn,
            TagKeys = tagKeys
        };

        try
        {
            await client.UntagResourceAsync(request);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Translate resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Translate resource.
    /// </summary>
    public static async Task<TranslateListTagsResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTagsForResourceRequest
        {
            ResourceArn = resourceArn
        };

        try
        {
            var resp = await client.ListTagsForResourceAsync(request);
            return new TranslateListTagsResult(Tags: resp.Tags);
        }
        catch (AmazonTranslateException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Translate resource '{resourceArn}'");
        }
    }
}

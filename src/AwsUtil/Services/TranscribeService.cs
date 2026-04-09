using Amazon;
using Amazon.TranscribeService;
using Amazon.TranscribeService.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of starting a transcription job.</summary>
public sealed record StartTranscriptionJobResult(
    TranscriptionJob? TranscriptionJob = null);

/// <summary>Result of getting a transcription job.</summary>
public sealed record GetTranscriptionJobResult(
    TranscriptionJob? TranscriptionJob = null);

/// <summary>Result of listing transcription jobs.</summary>
public sealed record ListTranscriptionJobsResult(
    List<TranscriptionJobSummary>? TranscriptionJobSummaries = null,
    string? NextToken = null,
    string? Status = null);

/// <summary>Result of starting a medical transcription job.</summary>
public sealed record StartMedicalTranscriptionJobResult(
    MedicalTranscriptionJob? MedicalTranscriptionJob = null);

/// <summary>Result of getting a medical transcription job.</summary>
public sealed record GetMedicalTranscriptionJobResult(
    MedicalTranscriptionJob? MedicalTranscriptionJob = null);

/// <summary>Result of listing medical transcription jobs.</summary>
public sealed record ListMedicalTranscriptionJobsResult(
    List<MedicalTranscriptionJobSummary>? MedicalTranscriptionJobSummaries = null,
    string? NextToken = null,
    string? Status = null);

/// <summary>Result of creating a vocabulary.</summary>
public sealed record CreateVocabularyResult(
    string? VocabularyName = null,
    string? LanguageCode = null,
    string? VocabularyState = null,
    DateTime? LastModifiedTime = null,
    string? FailureReason = null);

/// <summary>Result of getting a vocabulary.</summary>
public sealed record GetVocabularyResult(
    string? VocabularyName = null,
    string? LanguageCode = null,
    string? VocabularyState = null,
    DateTime? LastModifiedTime = null,
    string? FailureReason = null,
    string? DownloadUri = null);

/// <summary>Result of listing vocabularies.</summary>
public sealed record ListVocabulariesResult(
    List<VocabularyInfo>? Vocabularies = null,
    string? NextToken = null,
    string? Status = null);

/// <summary>Result of updating a vocabulary.</summary>
public sealed record UpdateVocabularyResult(
    string? VocabularyName = null,
    string? LanguageCode = null,
    string? VocabularyState = null,
    DateTime? LastModifiedTime = null);

/// <summary>Result of creating a vocabulary filter.</summary>
public sealed record CreateVocabularyFilterResult(
    string? VocabularyFilterName = null,
    string? LanguageCode = null,
    DateTime? LastModifiedTime = null);

/// <summary>Result of getting a vocabulary filter.</summary>
public sealed record GetVocabularyFilterResult(
    string? VocabularyFilterName = null,
    string? LanguageCode = null,
    DateTime? LastModifiedTime = null,
    string? DownloadUri = null);

/// <summary>Result of listing vocabulary filters.</summary>
public sealed record ListVocabularyFiltersResult(
    List<VocabularyFilterInfo>? VocabularyFilters = null,
    string? NextToken = null);

/// <summary>Result of updating a vocabulary filter.</summary>
public sealed record UpdateVocabularyFilterResult(
    string? VocabularyFilterName = null,
    string? LanguageCode = null,
    DateTime? LastModifiedTime = null);

/// <summary>Result of creating a language model.</summary>
public sealed record CreateLanguageModelResult(
    string? ModelName = null,
    string? LanguageCode = null,
    string? BaseModelName = null,
    string? ModelStatus = null);

/// <summary>Result of describing a language model.</summary>
public sealed record DescribeLanguageModelResult(
    LanguageModel? LanguageModel = null);

/// <summary>Result of listing language models.</summary>
public sealed record ListLanguageModelsResult(
    List<LanguageModel>? Models = null,
    string? NextToken = null);

/// <summary>Result of creating a call analytics category.</summary>
public sealed record CreateCallAnalyticsCategoryResult(
    CategoryProperties? CategoryProperties = null);

/// <summary>Result of getting a call analytics category.</summary>
public sealed record GetCallAnalyticsCategoryResult(
    CategoryProperties? CategoryProperties = null);

/// <summary>Result of listing call analytics categories.</summary>
public sealed record ListCallAnalyticsCategoriesResult(
    List<CategoryProperties>? Categories = null,
    string? NextToken = null);

/// <summary>Result of updating a call analytics category.</summary>
public sealed record UpdateCallAnalyticsCategoryResult(
    CategoryProperties? CategoryProperties = null);

/// <summary>Result of listing tags for a Transcribe resource.</summary>
public sealed record TranscribeListTagsResult(
    string? ResourceArn = null,
    List<Tag>? Tags = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon Transcribe.
/// Port of the Python <c>aws_util.transcribe</c> module.
/// </summary>
public static class TranscribeService
{
    private static AmazonTranscribeServiceClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonTranscribeServiceClient>(region);

    // -----------------------------------------------------------------------
    // Transcription jobs
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start a transcription job.
    /// </summary>
    public static async Task<StartTranscriptionJobResult>
        StartTranscriptionJobAsync(
            string transcriptionJobName,
            Media media,
            string? languageCode = null,
            string? mediaSampleRateHertz = null,
            string? mediaFormat = null,
            string? outputBucketName = null,
            string? outputKey = null,
            string? outputEncryptionKMSKeyId = null,
            Settings? settings = null,
            ModelSettings? modelSettings = null,
            JobExecutionSettings? jobExecutionSettings = null,
            ContentRedaction? contentRedaction = null,
            bool? identifyLanguage = null,
            bool? identifyMultipleLanguages = null,
            List<string>? languageOptions = null,
            Subtitles? subtitles = null,
            List<Tag>? tags = null,
            Dictionary<string, LanguageIdSettings>? languageIdSettings = null,
            List<ToxicityDetectionSettings>? toxicityDetection = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartTranscriptionJobRequest
        {
            TranscriptionJobName = transcriptionJobName,
            Media = media
        };
        if (languageCode != null) request.LanguageCode = languageCode;
        if (mediaFormat != null) request.MediaFormat = mediaFormat;
        if (outputBucketName != null) request.OutputBucketName = outputBucketName;
        if (outputKey != null) request.OutputKey = outputKey;
        if (outputEncryptionKMSKeyId != null)
            request.OutputEncryptionKMSKeyId = outputEncryptionKMSKeyId;
        if (settings != null) request.Settings = settings;
        if (modelSettings != null) request.ModelSettings = modelSettings;
        if (jobExecutionSettings != null)
            request.JobExecutionSettings = jobExecutionSettings;
        if (contentRedaction != null) request.ContentRedaction = contentRedaction;
        if (identifyLanguage.HasValue)
            request.IdentifyLanguage = identifyLanguage.Value;
        if (identifyMultipleLanguages.HasValue)
            request.IdentifyMultipleLanguages = identifyMultipleLanguages.Value;
        if (languageOptions != null) request.LanguageOptions = languageOptions;
        if (subtitles != null) request.Subtitles = subtitles;
        if (tags != null) request.Tags = tags;
        if (languageIdSettings != null)
            request.LanguageIdSettings = languageIdSettings;
        if (toxicityDetection != null) request.ToxicityDetection = toxicityDetection;

        try
        {
            var resp = await client.StartTranscriptionJobAsync(request);
            return new StartTranscriptionJobResult(
                TranscriptionJob: resp.TranscriptionJob);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start transcription job '{transcriptionJobName}'");
        }
    }

    /// <summary>
    /// Get details of a transcription job.
    /// </summary>
    public static async Task<GetTranscriptionJobResult>
        GetTranscriptionJobAsync(
            string transcriptionJobName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetTranscriptionJobRequest
        {
            TranscriptionJobName = transcriptionJobName
        };

        try
        {
            var resp = await client.GetTranscriptionJobAsync(request);
            return new GetTranscriptionJobResult(
                TranscriptionJob: resp.TranscriptionJob);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get transcription job '{transcriptionJobName}'");
        }
    }

    /// <summary>
    /// List transcription jobs.
    /// </summary>
    public static async Task<ListTranscriptionJobsResult>
        ListTranscriptionJobsAsync(
            string? status = null,
            string? jobNameContains = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListTranscriptionJobsRequest();
        if (status != null) request.Status = status;
        if (jobNameContains != null) request.JobNameContains = jobNameContains;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListTranscriptionJobsAsync(request);
            return new ListTranscriptionJobsResult(
                TranscriptionJobSummaries: resp.TranscriptionJobSummaries,
                NextToken: resp.NextToken,
                Status: resp.Status?.Value);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list transcription jobs");
        }
    }

    /// <summary>
    /// Delete a transcription job.
    /// </summary>
    public static async Task DeleteTranscriptionJobAsync(
        string transcriptionJobName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteTranscriptionJobRequest
        {
            TranscriptionJobName = transcriptionJobName
        };

        try
        {
            await client.DeleteTranscriptionJobAsync(request);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete transcription job '{transcriptionJobName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Medical transcription jobs
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start a medical transcription job.
    /// </summary>
    public static async Task<StartMedicalTranscriptionJobResult>
        StartMedicalTranscriptionJobAsync(
            string medicalTranscriptionJobName,
            Media media,
            string outputBucketName,
            string languageCode,
            string specialty,
            string type,
            string? mediaFormat = null,
            string? outputKey = null,
            string? outputEncryptionKMSKeyId = null,
            MedicalTranscriptionSetting? settings = null,
            ContentRedaction? contentRedaction = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartMedicalTranscriptionJobRequest
        {
            MedicalTranscriptionJobName = medicalTranscriptionJobName,
            Media = media,
            OutputBucketName = outputBucketName,
            LanguageCode = languageCode,
            Specialty = specialty,
            Type = type
        };
        if (mediaFormat != null) request.MediaFormat = mediaFormat;
        if (outputKey != null) request.OutputKey = outputKey;
        if (outputEncryptionKMSKeyId != null)
            request.OutputEncryptionKMSKeyId = outputEncryptionKMSKeyId;
        if (settings != null) request.Settings = settings;
        // ContentRedaction is not available on StartMedicalTranscriptionJobRequest in SDK v4
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.StartMedicalTranscriptionJobAsync(request);
            return new StartMedicalTranscriptionJobResult(
                MedicalTranscriptionJob: resp.MedicalTranscriptionJob);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start medical transcription job '{medicalTranscriptionJobName}'");
        }
    }

    /// <summary>
    /// Get details of a medical transcription job.
    /// </summary>
    public static async Task<GetMedicalTranscriptionJobResult>
        GetMedicalTranscriptionJobAsync(
            string medicalTranscriptionJobName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetMedicalTranscriptionJobRequest
        {
            MedicalTranscriptionJobName = medicalTranscriptionJobName
        };

        try
        {
            var resp = await client.GetMedicalTranscriptionJobAsync(request);
            return new GetMedicalTranscriptionJobResult(
                MedicalTranscriptionJob: resp.MedicalTranscriptionJob);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get medical transcription job '{medicalTranscriptionJobName}'");
        }
    }

    /// <summary>
    /// List medical transcription jobs.
    /// </summary>
    public static async Task<ListMedicalTranscriptionJobsResult>
        ListMedicalTranscriptionJobsAsync(
            string? status = null,
            string? jobNameContains = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListMedicalTranscriptionJobsRequest();
        if (status != null) request.Status = status;
        if (jobNameContains != null) request.JobNameContains = jobNameContains;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListMedicalTranscriptionJobsAsync(request);
            return new ListMedicalTranscriptionJobsResult(
                MedicalTranscriptionJobSummaries:
                    resp.MedicalTranscriptionJobSummaries,
                NextToken: resp.NextToken,
                Status: resp.Status?.Value);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list medical transcription jobs");
        }
    }

    /// <summary>
    /// Delete a medical transcription job.
    /// </summary>
    public static async Task DeleteMedicalTranscriptionJobAsync(
        string medicalTranscriptionJobName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteMedicalTranscriptionJobRequest
        {
            MedicalTranscriptionJobName = medicalTranscriptionJobName
        };

        try
        {
            await client.DeleteMedicalTranscriptionJobAsync(request);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete medical transcription job '{medicalTranscriptionJobName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Vocabulary management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a custom vocabulary.
    /// </summary>
    public static async Task<CreateVocabularyResult> CreateVocabularyAsync(
        string vocabularyName,
        string languageCode,
        List<string>? phrases = null,
        string? vocabularyFileUri = null,
        string? dataAccessRoleArn = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateVocabularyRequest
        {
            VocabularyName = vocabularyName,
            LanguageCode = languageCode
        };
        if (phrases != null) request.Phrases = phrases;
        if (vocabularyFileUri != null) request.VocabularyFileUri = vocabularyFileUri;
        if (dataAccessRoleArn != null) request.DataAccessRoleArn = dataAccessRoleArn;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateVocabularyAsync(request);
            return new CreateVocabularyResult(
                VocabularyName: resp.VocabularyName,
                LanguageCode: resp.LanguageCode?.Value,
                VocabularyState: resp.VocabularyState?.Value,
                LastModifiedTime: resp.LastModifiedTime,
                FailureReason: resp.FailureReason);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create vocabulary '{vocabularyName}'");
        }
    }

    /// <summary>
    /// Delete a custom vocabulary.
    /// </summary>
    public static async Task DeleteVocabularyAsync(
        string vocabularyName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteVocabularyRequest
        {
            VocabularyName = vocabularyName
        };

        try
        {
            await client.DeleteVocabularyAsync(request);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete vocabulary '{vocabularyName}'");
        }
    }

    /// <summary>
    /// Get details of a custom vocabulary.
    /// </summary>
    public static async Task<GetVocabularyResult> GetVocabularyAsync(
        string vocabularyName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetVocabularyRequest
        {
            VocabularyName = vocabularyName
        };

        try
        {
            var resp = await client.GetVocabularyAsync(request);
            return new GetVocabularyResult(
                VocabularyName: resp.VocabularyName,
                LanguageCode: resp.LanguageCode?.Value,
                VocabularyState: resp.VocabularyState?.Value,
                LastModifiedTime: resp.LastModifiedTime,
                FailureReason: resp.FailureReason,
                DownloadUri: resp.DownloadUri);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get vocabulary '{vocabularyName}'");
        }
    }

    /// <summary>
    /// List custom vocabularies.
    /// </summary>
    public static async Task<ListVocabulariesResult> ListVocabulariesAsync(
        string? stateEquals = null,
        string? nameContains = null,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListVocabulariesRequest();
        if (stateEquals != null) request.StateEquals = stateEquals;
        if (nameContains != null) request.NameContains = nameContains;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListVocabulariesAsync(request);
            return new ListVocabulariesResult(
                Vocabularies: resp.Vocabularies,
                NextToken: resp.NextToken,
                Status: resp.Status?.Value);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list vocabularies");
        }
    }

    /// <summary>
    /// Update a custom vocabulary.
    /// </summary>
    public static async Task<UpdateVocabularyResult> UpdateVocabularyAsync(
        string vocabularyName,
        string languageCode,
        List<string>? phrases = null,
        string? vocabularyFileUri = null,
        string? dataAccessRoleArn = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateVocabularyRequest
        {
            VocabularyName = vocabularyName,
            LanguageCode = languageCode
        };
        if (phrases != null) request.Phrases = phrases;
        if (vocabularyFileUri != null) request.VocabularyFileUri = vocabularyFileUri;
        if (dataAccessRoleArn != null) request.DataAccessRoleArn = dataAccessRoleArn;

        try
        {
            var resp = await client.UpdateVocabularyAsync(request);
            return new UpdateVocabularyResult(
                VocabularyName: resp.VocabularyName,
                LanguageCode: resp.LanguageCode?.Value,
                VocabularyState: resp.VocabularyState?.Value,
                LastModifiedTime: resp.LastModifiedTime);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update vocabulary '{vocabularyName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Vocabulary filter management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a vocabulary filter.
    /// </summary>
    public static async Task<CreateVocabularyFilterResult>
        CreateVocabularyFilterAsync(
            string vocabularyFilterName,
            string languageCode,
            List<string>? words = null,
            string? vocabularyFilterFileUri = null,
            string? dataAccessRoleArn = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateVocabularyFilterRequest
        {
            VocabularyFilterName = vocabularyFilterName,
            LanguageCode = languageCode
        };
        if (words != null) request.Words = words;
        if (vocabularyFilterFileUri != null)
            request.VocabularyFilterFileUri = vocabularyFilterFileUri;
        if (dataAccessRoleArn != null) request.DataAccessRoleArn = dataAccessRoleArn;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateVocabularyFilterAsync(request);
            return new CreateVocabularyFilterResult(
                VocabularyFilterName: resp.VocabularyFilterName,
                LanguageCode: resp.LanguageCode?.Value,
                LastModifiedTime: resp.LastModifiedTime);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create vocabulary filter '{vocabularyFilterName}'");
        }
    }

    /// <summary>
    /// Delete a vocabulary filter.
    /// </summary>
    public static async Task DeleteVocabularyFilterAsync(
        string vocabularyFilterName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteVocabularyFilterRequest
        {
            VocabularyFilterName = vocabularyFilterName
        };

        try
        {
            await client.DeleteVocabularyFilterAsync(request);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete vocabulary filter '{vocabularyFilterName}'");
        }
    }

    /// <summary>
    /// Get details of a vocabulary filter.
    /// </summary>
    public static async Task<GetVocabularyFilterResult>
        GetVocabularyFilterAsync(
            string vocabularyFilterName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetVocabularyFilterRequest
        {
            VocabularyFilterName = vocabularyFilterName
        };

        try
        {
            var resp = await client.GetVocabularyFilterAsync(request);
            return new GetVocabularyFilterResult(
                VocabularyFilterName: resp.VocabularyFilterName,
                LanguageCode: resp.LanguageCode?.Value,
                LastModifiedTime: resp.LastModifiedTime,
                DownloadUri: resp.DownloadUri);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get vocabulary filter '{vocabularyFilterName}'");
        }
    }

    /// <summary>
    /// List vocabulary filters.
    /// </summary>
    public static async Task<ListVocabularyFiltersResult>
        ListVocabularyFiltersAsync(
            string? nameContains = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListVocabularyFiltersRequest();
        if (nameContains != null) request.NameContains = nameContains;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListVocabularyFiltersAsync(request);
            return new ListVocabularyFiltersResult(
                VocabularyFilters: resp.VocabularyFilters,
                NextToken: resp.NextToken);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list vocabulary filters");
        }
    }

    /// <summary>
    /// Update a vocabulary filter.
    /// </summary>
    public static async Task<UpdateVocabularyFilterResult>
        UpdateVocabularyFilterAsync(
            string vocabularyFilterName,
            List<string>? words = null,
            string? vocabularyFilterFileUri = null,
            string? dataAccessRoleArn = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateVocabularyFilterRequest
        {
            VocabularyFilterName = vocabularyFilterName
        };
        if (words != null) request.Words = words;
        if (vocabularyFilterFileUri != null)
            request.VocabularyFilterFileUri = vocabularyFilterFileUri;
        if (dataAccessRoleArn != null) request.DataAccessRoleArn = dataAccessRoleArn;

        try
        {
            var resp = await client.UpdateVocabularyFilterAsync(request);
            return new UpdateVocabularyFilterResult(
                VocabularyFilterName: resp.VocabularyFilterName,
                LanguageCode: resp.LanguageCode?.Value,
                LastModifiedTime: resp.LastModifiedTime);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update vocabulary filter '{vocabularyFilterName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Language model management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a custom language model.
    /// </summary>
    public static async Task<CreateLanguageModelResult>
        CreateLanguageModelAsync(
            string modelName,
            string languageCode,
            string baseModelName,
            InputDataConfig inputDataConfig,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateLanguageModelRequest
        {
            ModelName = modelName,
            LanguageCode = languageCode,
            BaseModelName = baseModelName,
            InputDataConfig = inputDataConfig
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateLanguageModelAsync(request);
            return new CreateLanguageModelResult(
                ModelName: resp.ModelName,
                LanguageCode: resp.LanguageCode?.Value,
                BaseModelName: resp.BaseModelName?.Value,
                ModelStatus: resp.ModelStatus?.Value);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create language model '{modelName}'");
        }
    }

    /// <summary>
    /// Delete a custom language model.
    /// </summary>
    public static async Task DeleteLanguageModelAsync(
        string modelName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteLanguageModelRequest { ModelName = modelName };

        try
        {
            await client.DeleteLanguageModelAsync(request);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete language model '{modelName}'");
        }
    }

    /// <summary>
    /// Describe a custom language model.
    /// </summary>
    public static async Task<DescribeLanguageModelResult>
        DescribeLanguageModelAsync(
            string modelName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeLanguageModelRequest
        {
            ModelName = modelName
        };

        try
        {
            var resp = await client.DescribeLanguageModelAsync(request);
            return new DescribeLanguageModelResult(
                LanguageModel: resp.LanguageModel);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe language model '{modelName}'");
        }
    }

    /// <summary>
    /// List custom language models.
    /// </summary>
    public static async Task<ListLanguageModelsResult>
        ListLanguageModelsAsync(
            string? statusEquals = null,
            string? nameContains = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListLanguageModelsRequest();
        if (statusEquals != null) request.StatusEquals = statusEquals;
        if (nameContains != null) request.NameContains = nameContains;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListLanguageModelsAsync(request);
            return new ListLanguageModelsResult(
                Models: resp.Models,
                NextToken: resp.NextToken);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list language models");
        }
    }

    // -----------------------------------------------------------------------
    // Call analytics categories
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a call analytics category.
    /// </summary>
    public static async Task<CreateCallAnalyticsCategoryResult>
        CreateCallAnalyticsCategoryAsync(
            string categoryName,
            List<Rule> rules,
            string? inputType = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateCallAnalyticsCategoryRequest
        {
            CategoryName = categoryName,
            Rules = rules
        };
        if (inputType != null) request.InputType = inputType;

        try
        {
            var resp = await client.CreateCallAnalyticsCategoryAsync(request);
            return new CreateCallAnalyticsCategoryResult(
                CategoryProperties: resp.CategoryProperties);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create call analytics category '{categoryName}'");
        }
    }

    /// <summary>
    /// Delete a call analytics category.
    /// </summary>
    public static async Task DeleteCallAnalyticsCategoryAsync(
        string categoryName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteCallAnalyticsCategoryRequest
        {
            CategoryName = categoryName
        };

        try
        {
            await client.DeleteCallAnalyticsCategoryAsync(request);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete call analytics category '{categoryName}'");
        }
    }

    /// <summary>
    /// Get a call analytics category.
    /// </summary>
    public static async Task<GetCallAnalyticsCategoryResult>
        GetCallAnalyticsCategoryAsync(
            string categoryName,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetCallAnalyticsCategoryRequest
        {
            CategoryName = categoryName
        };

        try
        {
            var resp = await client.GetCallAnalyticsCategoryAsync(request);
            return new GetCallAnalyticsCategoryResult(
                CategoryProperties: resp.CategoryProperties);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get call analytics category '{categoryName}'");
        }
    }

    /// <summary>
    /// List call analytics categories.
    /// </summary>
    public static async Task<ListCallAnalyticsCategoriesResult>
        ListCallAnalyticsCategoriesAsync(
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListCallAnalyticsCategoriesRequest();
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListCallAnalyticsCategoriesAsync(request);
            return new ListCallAnalyticsCategoriesResult(
                Categories: resp.Categories,
                NextToken: resp.NextToken);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list call analytics categories");
        }
    }

    /// <summary>
    /// Update a call analytics category.
    /// </summary>
    public static async Task<UpdateCallAnalyticsCategoryResult>
        UpdateCallAnalyticsCategoryAsync(
            string categoryName,
            List<Rule> rules,
            string? inputType = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateCallAnalyticsCategoryRequest
        {
            CategoryName = categoryName,
            Rules = rules
        };
        if (inputType != null) request.InputType = inputType;

        try
        {
            var resp = await client.UpdateCallAnalyticsCategoryAsync(request);
            return new UpdateCallAnalyticsCategoryResult(
                CategoryProperties: resp.CategoryProperties);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update call analytics category '{categoryName}'");
        }
    }

    // -----------------------------------------------------------------------
    // Tagging
    // -----------------------------------------------------------------------

    /// <summary>
    /// Add tags to a Transcribe resource.
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
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Transcribe resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Transcribe resource.
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
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Transcribe resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Transcribe resource.
    /// </summary>
    public static async Task<TranscribeListTagsResult>
        ListTagsForResourceAsync(
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
            return new TranscribeListTagsResult(
                ResourceArn: resp.ResourceArn,
                Tags: resp.Tags);
        }
        catch (AmazonTranscribeServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Transcribe resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="StartTranscriptionJobAsync"/>.</summary>
    public static StartTranscriptionJobResult StartTranscriptionJob(string transcriptionJobName, Media media, string? languageCode = null, string? mediaSampleRateHertz = null, string? mediaFormat = null, string? outputBucketName = null, string? outputKey = null, string? outputEncryptionKMSKeyId = null, Settings? settings = null, ModelSettings? modelSettings = null, JobExecutionSettings? jobExecutionSettings = null, ContentRedaction? contentRedaction = null, bool? identifyLanguage = null, bool? identifyMultipleLanguages = null, List<string>? languageOptions = null, Subtitles? subtitles = null, List<Tag>? tags = null, Dictionary<string, LanguageIdSettings>? languageIdSettings = null, List<ToxicityDetectionSettings>? toxicityDetection = null, RegionEndpoint? region = null)
        => StartTranscriptionJobAsync(transcriptionJobName, media, languageCode, mediaSampleRateHertz, mediaFormat, outputBucketName, outputKey, outputEncryptionKMSKeyId, settings, modelSettings, jobExecutionSettings, contentRedaction, identifyLanguage, identifyMultipleLanguages, languageOptions, subtitles, tags, languageIdSettings, toxicityDetection, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetTranscriptionJobAsync"/>.</summary>
    public static GetTranscriptionJobResult GetTranscriptionJob(string transcriptionJobName, RegionEndpoint? region = null)
        => GetTranscriptionJobAsync(transcriptionJobName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTranscriptionJobsAsync"/>.</summary>
    public static ListTranscriptionJobsResult ListTranscriptionJobs(string? status = null, string? jobNameContains = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListTranscriptionJobsAsync(status, jobNameContains, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteTranscriptionJobAsync"/>.</summary>
    public static void DeleteTranscriptionJob(string transcriptionJobName, RegionEndpoint? region = null)
        => DeleteTranscriptionJobAsync(transcriptionJobName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartMedicalTranscriptionJobAsync"/>.</summary>
    public static StartMedicalTranscriptionJobResult StartMedicalTranscriptionJob(string medicalTranscriptionJobName, Media media, string outputBucketName, string languageCode, string specialty, string type, string? mediaFormat = null, string? outputKey = null, string? outputEncryptionKMSKeyId = null, MedicalTranscriptionSetting? settings = null, ContentRedaction? contentRedaction = null, List<Tag>? tags = null, RegionEndpoint? region = null)
        => StartMedicalTranscriptionJobAsync(medicalTranscriptionJobName, media, outputBucketName, languageCode, specialty, type, mediaFormat, outputKey, outputEncryptionKMSKeyId, settings, contentRedaction, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetMedicalTranscriptionJobAsync"/>.</summary>
    public static GetMedicalTranscriptionJobResult GetMedicalTranscriptionJob(string medicalTranscriptionJobName, RegionEndpoint? region = null)
        => GetMedicalTranscriptionJobAsync(medicalTranscriptionJobName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListMedicalTranscriptionJobsAsync"/>.</summary>
    public static ListMedicalTranscriptionJobsResult ListMedicalTranscriptionJobs(string? status = null, string? jobNameContains = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListMedicalTranscriptionJobsAsync(status, jobNameContains, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteMedicalTranscriptionJobAsync"/>.</summary>
    public static void DeleteMedicalTranscriptionJob(string medicalTranscriptionJobName, RegionEndpoint? region = null)
        => DeleteMedicalTranscriptionJobAsync(medicalTranscriptionJobName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateVocabularyAsync"/>.</summary>
    public static CreateVocabularyResult CreateVocabulary(string vocabularyName, string languageCode, List<string>? phrases = null, string? vocabularyFileUri = null, string? dataAccessRoleArn = null, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateVocabularyAsync(vocabularyName, languageCode, phrases, vocabularyFileUri, dataAccessRoleArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteVocabularyAsync"/>.</summary>
    public static void DeleteVocabulary(string vocabularyName, RegionEndpoint? region = null)
        => DeleteVocabularyAsync(vocabularyName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetVocabularyAsync"/>.</summary>
    public static GetVocabularyResult GetVocabulary(string vocabularyName, RegionEndpoint? region = null)
        => GetVocabularyAsync(vocabularyName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListVocabulariesAsync"/>.</summary>
    public static ListVocabulariesResult ListVocabularies(string? stateEquals = null, string? nameContains = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListVocabulariesAsync(stateEquals, nameContains, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateVocabularyAsync"/>.</summary>
    public static UpdateVocabularyResult UpdateVocabulary(string vocabularyName, string languageCode, List<string>? phrases = null, string? vocabularyFileUri = null, string? dataAccessRoleArn = null, RegionEndpoint? region = null)
        => UpdateVocabularyAsync(vocabularyName, languageCode, phrases, vocabularyFileUri, dataAccessRoleArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateVocabularyFilterAsync"/>.</summary>
    public static CreateVocabularyFilterResult CreateVocabularyFilter(string vocabularyFilterName, string languageCode, List<string>? words = null, string? vocabularyFilterFileUri = null, string? dataAccessRoleArn = null, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateVocabularyFilterAsync(vocabularyFilterName, languageCode, words, vocabularyFilterFileUri, dataAccessRoleArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteVocabularyFilterAsync"/>.</summary>
    public static void DeleteVocabularyFilter(string vocabularyFilterName, RegionEndpoint? region = null)
        => DeleteVocabularyFilterAsync(vocabularyFilterName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetVocabularyFilterAsync"/>.</summary>
    public static GetVocabularyFilterResult GetVocabularyFilter(string vocabularyFilterName, RegionEndpoint? region = null)
        => GetVocabularyFilterAsync(vocabularyFilterName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListVocabularyFiltersAsync"/>.</summary>
    public static ListVocabularyFiltersResult ListVocabularyFilters(string? nameContains = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListVocabularyFiltersAsync(nameContains, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateVocabularyFilterAsync"/>.</summary>
    public static UpdateVocabularyFilterResult UpdateVocabularyFilter(string vocabularyFilterName, List<string>? words = null, string? vocabularyFilterFileUri = null, string? dataAccessRoleArn = null, RegionEndpoint? region = null)
        => UpdateVocabularyFilterAsync(vocabularyFilterName, words, vocabularyFilterFileUri, dataAccessRoleArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateLanguageModelAsync"/>.</summary>
    public static CreateLanguageModelResult CreateLanguageModel(string modelName, string languageCode, string baseModelName, InputDataConfig inputDataConfig, List<Tag>? tags = null, RegionEndpoint? region = null)
        => CreateLanguageModelAsync(modelName, languageCode, baseModelName, inputDataConfig, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteLanguageModelAsync"/>.</summary>
    public static void DeleteLanguageModel(string modelName, RegionEndpoint? region = null)
        => DeleteLanguageModelAsync(modelName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeLanguageModelAsync"/>.</summary>
    public static DescribeLanguageModelResult DescribeLanguageModel(string modelName, RegionEndpoint? region = null)
        => DescribeLanguageModelAsync(modelName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListLanguageModelsAsync"/>.</summary>
    public static ListLanguageModelsResult ListLanguageModels(string? statusEquals = null, string? nameContains = null, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListLanguageModelsAsync(statusEquals, nameContains, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateCallAnalyticsCategoryAsync"/>.</summary>
    public static CreateCallAnalyticsCategoryResult CreateCallAnalyticsCategory(string categoryName, List<Rule> rules, string? inputType = null, RegionEndpoint? region = null)
        => CreateCallAnalyticsCategoryAsync(categoryName, rules, inputType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteCallAnalyticsCategoryAsync"/>.</summary>
    public static void DeleteCallAnalyticsCategory(string categoryName, RegionEndpoint? region = null)
        => DeleteCallAnalyticsCategoryAsync(categoryName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetCallAnalyticsCategoryAsync"/>.</summary>
    public static GetCallAnalyticsCategoryResult GetCallAnalyticsCategory(string categoryName, RegionEndpoint? region = null)
        => GetCallAnalyticsCategoryAsync(categoryName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListCallAnalyticsCategoriesAsync"/>.</summary>
    public static ListCallAnalyticsCategoriesResult ListCallAnalyticsCategories(int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListCallAnalyticsCategoriesAsync(maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateCallAnalyticsCategoryAsync"/>.</summary>
    public static UpdateCallAnalyticsCategoryResult UpdateCallAnalyticsCategory(string categoryName, List<Rule> rules, string? inputType = null, RegionEndpoint? region = null)
        => UpdateCallAnalyticsCategoryAsync(categoryName, rules, inputType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, List<Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static TranscribeListTagsResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}

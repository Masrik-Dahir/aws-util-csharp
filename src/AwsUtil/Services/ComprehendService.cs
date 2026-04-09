using Amazon;
using Amazon.Comprehend;
using Amazon.Comprehend.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of a sentiment detection.</summary>
public sealed record DetectSentimentResult(
    string? Sentiment = null,
    SentimentScore? SentimentScore = null);

/// <summary>Result of an entity detection.</summary>
public sealed record DetectEntitiesResult(List<Entity>? Entities = null);

/// <summary>Result of a key phrases detection.</summary>
public sealed record DetectKeyPhrasesResult(List<KeyPhrase>? KeyPhrases = null);

/// <summary>Result of a dominant language detection.</summary>
public sealed record DetectDominantLanguageResult(
    List<DominantLanguage>? Languages = null);

/// <summary>Result of a PII entity detection.</summary>
public sealed record DetectPiiEntitiesResult(List<PiiEntity>? Entities = null);

/// <summary>Result of a syntax detection.</summary>
public sealed record DetectSyntaxResult(List<SyntaxToken>? SyntaxTokens = null);

/// <summary>Result of a targeted sentiment detection.</summary>
public sealed record DetectTargetedSentimentResult(
    List<TargetedSentimentEntity>? Entities = null);

/// <summary>Result of a toxic content detection.</summary>
public sealed record DetectToxicContentResult(
    List<ToxicLabels>? ResultList = null);

/// <summary>Result of a batch sentiment detection.</summary>
public sealed record BatchDetectSentimentResult(
    List<BatchDetectSentimentItemResult>? ResultList = null,
    List<BatchItemError>? ErrorList = null);

/// <summary>Result of a batch entity detection.</summary>
public sealed record BatchDetectEntitiesResult(
    List<BatchDetectEntitiesItemResult>? ResultList = null,
    List<BatchItemError>? ErrorList = null);

/// <summary>Result of a batch key phrases detection.</summary>
public sealed record BatchDetectKeyPhrasesResult(
    List<BatchDetectKeyPhrasesItemResult>? ResultList = null,
    List<BatchItemError>? ErrorList = null);

/// <summary>Result of a batch dominant language detection.</summary>
public sealed record BatchDetectDominantLanguageResult(
    List<BatchDetectDominantLanguageItemResult>? ResultList = null,
    List<BatchItemError>? ErrorList = null);

/// <summary>Result of a batch syntax detection.</summary>
public sealed record BatchDetectSyntaxResult(
    List<BatchDetectSyntaxItemResult>? ResultList = null,
    List<BatchItemError>? ErrorList = null);

/// <summary>Result of a batch targeted sentiment detection.</summary>
public sealed record BatchDetectTargetedSentimentResult(
    List<BatchDetectTargetedSentimentItemResult>? ResultList = null,
    List<BatchItemError>? ErrorList = null);

/// <summary>Result of a document classification.</summary>
public sealed record ClassifyDocumentResult(
    List<DocumentClass>? Classes = null,
    List<DocumentLabel>? Labels = null,
    List<DocumentTypeListItem>? DocumentType = null);

/// <summary>Result of a PII containment check.</summary>
public sealed record ContainsPiiEntitiesResult(
    List<EntityLabel>? Labels = null);

/// <summary>Result of starting a Comprehend async job.</summary>
public sealed record StartComprehendJobResult(
    string? JobId = null,
    string? JobArn = null,
    string? JobStatus = null);

/// <summary>Result of stopping a Comprehend async job.</summary>
public sealed record StopComprehendJobResult(
    string? JobId = null,
    string? JobStatus = null);

/// <summary>Result of describing a sentiment detection job.</summary>
public sealed record DescribeSentimentDetectionJobResult(
    SentimentDetectionJobProperties? JobProperties = null);

/// <summary>Result of listing sentiment detection jobs.</summary>
public sealed record ListSentimentDetectionJobsResult(
    List<SentimentDetectionJobProperties>? Jobs = null,
    string? NextToken = null);

/// <summary>Result of describing an entities detection job.</summary>
public sealed record DescribeEntitiesDetectionJobResult(
    EntitiesDetectionJobProperties? JobProperties = null);

/// <summary>Result of listing entities detection jobs.</summary>
public sealed record ListEntitiesDetectionJobsResult(
    List<EntitiesDetectionJobProperties>? Jobs = null,
    string? NextToken = null);

/// <summary>Result of creating a document classifier.</summary>
public sealed record CreateDocumentClassifierResult(
    string? DocumentClassifierArn = null);

/// <summary>Result of describing a document classifier.</summary>
public sealed record DescribeDocumentClassifierResult(
    DocumentClassifierProperties? Properties = null);

/// <summary>Result of listing document classifiers.</summary>
public sealed record ListDocumentClassifiersResult(
    List<DocumentClassifierProperties>? Classifiers = null,
    string? NextToken = null);

/// <summary>Result of creating an entity recognizer.</summary>
public sealed record CreateEntityRecognizerResult(
    string? EntityRecognizerArn = null);

/// <summary>Result of describing an entity recognizer.</summary>
public sealed record DescribeEntityRecognizerResult(
    EntityRecognizerProperties? Properties = null);

/// <summary>Result of listing entity recognizers.</summary>
public sealed record ListEntityRecognizersResult(
    List<EntityRecognizerProperties>? Recognizers = null,
    string? NextToken = null);

/// <summary>Result of listing tags for a Comprehend resource.</summary>
public sealed record ComprehendListTagsResult(
    string? ResourceArn = null,
    List<Tag>? Tags = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon Comprehend.
/// Port of the Python <c>aws_util.comprehend</c> module.
/// </summary>
public static class ComprehendService
{
    private static AmazonComprehendClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonComprehendClient>(region);

    // -----------------------------------------------------------------------
    // Real-time detection
    // -----------------------------------------------------------------------

    /// <summary>
    /// Detect the prevailing sentiment in text.
    /// </summary>
    public static async Task<DetectSentimentResult> DetectSentimentAsync(
        string text,
        string languageCode,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectSentimentRequest
        {
            Text = text,
            LanguageCode = languageCode
        };

        try
        {
            var resp = await client.DetectSentimentAsync(request);
            return new DetectSentimentResult(
                Sentiment: resp.Sentiment?.Value,
                SentimentScore: resp.SentimentScore);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to detect sentiment");
        }
    }

    /// <summary>
    /// Detect named entities in text.
    /// </summary>
    public static async Task<DetectEntitiesResult> DetectEntitiesAsync(
        string text,
        string? languageCode = null,
        string? endpointArn = null,
        byte[]? bytes = null,
        string? documentReaderConfig = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectEntitiesRequest { Text = text };
        if (languageCode != null) request.LanguageCode = languageCode;
        if (endpointArn != null) request.EndpointArn = endpointArn;

        try
        {
            var resp = await client.DetectEntitiesAsync(request);
            return new DetectEntitiesResult(Entities: resp.Entities);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to detect entities");
        }
    }

    /// <summary>
    /// Detect key phrases in text.
    /// </summary>
    public static async Task<DetectKeyPhrasesResult> DetectKeyPhrasesAsync(
        string text,
        string languageCode,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectKeyPhrasesRequest
        {
            Text = text,
            LanguageCode = languageCode
        };

        try
        {
            var resp = await client.DetectKeyPhrasesAsync(request);
            return new DetectKeyPhrasesResult(KeyPhrases: resp.KeyPhrases);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to detect key phrases");
        }
    }

    /// <summary>
    /// Detect the dominant language of text.
    /// </summary>
    public static async Task<DetectDominantLanguageResult>
        DetectDominantLanguageAsync(
            string text,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectDominantLanguageRequest { Text = text };

        try
        {
            var resp = await client.DetectDominantLanguageAsync(request);
            return new DetectDominantLanguageResult(Languages: resp.Languages);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to detect dominant language");
        }
    }

    /// <summary>
    /// Detect PII entities in text.
    /// </summary>
    public static async Task<DetectPiiEntitiesResult> DetectPiiEntitiesAsync(
        string text,
        string languageCode,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectPiiEntitiesRequest
        {
            Text = text,
            LanguageCode = languageCode
        };

        try
        {
            var resp = await client.DetectPiiEntitiesAsync(request);
            return new DetectPiiEntitiesResult(Entities: resp.Entities);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to detect PII entities");
        }
    }

    /// <summary>
    /// Detect syntax tokens in text.
    /// </summary>
    public static async Task<DetectSyntaxResult> DetectSyntaxAsync(
        string text,
        string languageCode,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectSyntaxRequest
        {
            Text = text,
            LanguageCode = languageCode
        };

        try
        {
            var resp = await client.DetectSyntaxAsync(request);
            return new DetectSyntaxResult(SyntaxTokens: resp.SyntaxTokens);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to detect syntax");
        }
    }

    /// <summary>
    /// Detect targeted sentiment for entities in text.
    /// </summary>
    public static async Task<DetectTargetedSentimentResult>
        DetectTargetedSentimentAsync(
            string text,
            string languageCode,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectTargetedSentimentRequest
        {
            Text = text,
            LanguageCode = languageCode
        };

        try
        {
            var resp = await client.DetectTargetedSentimentAsync(request);
            return new DetectTargetedSentimentResult(Entities: resp.Entities);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to detect targeted sentiment");
        }
    }

    /// <summary>
    /// Detect toxic content in text.
    /// </summary>
    public static async Task<DetectToxicContentResult> DetectToxicContentAsync(
        List<TextSegment> textSegments,
        string languageCode,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectToxicContentRequest
        {
            TextSegments = textSegments,
            LanguageCode = languageCode
        };

        try
        {
            var resp = await client.DetectToxicContentAsync(request);
            return new DetectToxicContentResult(ResultList: resp.ResultList);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to detect toxic content");
        }
    }

    // -----------------------------------------------------------------------
    // Batch detection
    // -----------------------------------------------------------------------

    /// <summary>
    /// Detect sentiment in a batch of texts.
    /// </summary>
    public static async Task<BatchDetectSentimentResult>
        BatchDetectSentimentAsync(
            List<string> textList,
            string languageCode,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchDetectSentimentRequest
        {
            TextList = textList,
            LanguageCode = languageCode
        };

        try
        {
            var resp = await client.BatchDetectSentimentAsync(request);
            return new BatchDetectSentimentResult(
                ResultList: resp.ResultList,
                ErrorList: resp.ErrorList);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch detect sentiment");
        }
    }

    /// <summary>
    /// Detect entities in a batch of texts.
    /// </summary>
    public static async Task<BatchDetectEntitiesResult>
        BatchDetectEntitiesAsync(
            List<string> textList,
            string languageCode,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchDetectEntitiesRequest
        {
            TextList = textList,
            LanguageCode = languageCode
        };

        try
        {
            var resp = await client.BatchDetectEntitiesAsync(request);
            return new BatchDetectEntitiesResult(
                ResultList: resp.ResultList,
                ErrorList: resp.ErrorList);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch detect entities");
        }
    }

    /// <summary>
    /// Detect key phrases in a batch of texts.
    /// </summary>
    public static async Task<BatchDetectKeyPhrasesResult>
        BatchDetectKeyPhrasesAsync(
            List<string> textList,
            string languageCode,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchDetectKeyPhrasesRequest
        {
            TextList = textList,
            LanguageCode = languageCode
        };

        try
        {
            var resp = await client.BatchDetectKeyPhrasesAsync(request);
            return new BatchDetectKeyPhrasesResult(
                ResultList: resp.ResultList,
                ErrorList: resp.ErrorList);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch detect key phrases");
        }
    }

    /// <summary>
    /// Detect dominant languages in a batch of texts.
    /// </summary>
    public static async Task<BatchDetectDominantLanguageResult>
        BatchDetectDominantLanguageAsync(
            List<string> textList,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchDetectDominantLanguageRequest
        {
            TextList = textList
        };

        try
        {
            var resp = await client.BatchDetectDominantLanguageAsync(request);
            return new BatchDetectDominantLanguageResult(
                ResultList: resp.ResultList,
                ErrorList: resp.ErrorList);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch detect dominant language");
        }
    }

    /// <summary>
    /// Detect syntax in a batch of texts.
    /// </summary>
    public static async Task<BatchDetectSyntaxResult>
        BatchDetectSyntaxAsync(
            List<string> textList,
            string languageCode,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchDetectSyntaxRequest
        {
            TextList = textList,
            LanguageCode = languageCode
        };

        try
        {
            var resp = await client.BatchDetectSyntaxAsync(request);
            return new BatchDetectSyntaxResult(
                ResultList: resp.ResultList,
                ErrorList: resp.ErrorList);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch detect syntax");
        }
    }

    /// <summary>
    /// Detect targeted sentiment in a batch of texts.
    /// </summary>
    public static async Task<BatchDetectTargetedSentimentResult>
        BatchDetectTargetedSentimentAsync(
            List<string> textList,
            string languageCode,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new BatchDetectTargetedSentimentRequest
        {
            TextList = textList,
            LanguageCode = languageCode
        };

        try
        {
            var resp = await client.BatchDetectTargetedSentimentAsync(request);
            return new BatchDetectTargetedSentimentResult(
                ResultList: resp.ResultList,
                ErrorList: resp.ErrorList);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch detect targeted sentiment");
        }
    }

    // -----------------------------------------------------------------------
    // Document classification
    // -----------------------------------------------------------------------

    /// <summary>
    /// Classify a document using a custom endpoint.
    /// </summary>
    public static async Task<ClassifyDocumentResult> ClassifyDocumentAsync(
        string endpointArn,
        string? text = null,
        byte[]? bytes = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ClassifyDocumentRequest
        {
            EndpointArn = endpointArn
        };
        if (text != null) request.Text = text;

        try
        {
            var resp = await client.ClassifyDocumentAsync(request);
            return new ClassifyDocumentResult(
                Classes: resp.Classes,
                Labels: resp.Labels,
                DocumentType: resp.DocumentType);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to classify document");
        }
    }

    /// <summary>
    /// Check whether text contains PII entities.
    /// </summary>
    public static async Task<ContainsPiiEntitiesResult>
        ContainsPiiEntitiesAsync(
            string text,
            string languageCode,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ContainsPiiEntitiesRequest
        {
            Text = text,
            LanguageCode = languageCode
        };

        try
        {
            var resp = await client.ContainsPiiEntitiesAsync(request);
            return new ContainsPiiEntitiesResult(Labels: resp.Labels);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to check for PII entities");
        }
    }

    // -----------------------------------------------------------------------
    // Sentiment detection jobs
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start an asynchronous sentiment detection job.
    /// </summary>
    public static async Task<StartComprehendJobResult>
        StartSentimentDetectionJobAsync(
            InputDataConfig inputDataConfig,
            OutputDataConfig outputDataConfig,
            string dataAccessRoleArn,
            string languageCode,
            string? jobName = null,
            string? clientRequestToken = null,
            VpcConfig? vpcConfig = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartSentimentDetectionJobRequest
        {
            InputDataConfig = inputDataConfig,
            OutputDataConfig = outputDataConfig,
            DataAccessRoleArn = dataAccessRoleArn,
            LanguageCode = languageCode
        };
        if (jobName != null) request.JobName = jobName;
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (vpcConfig != null) request.VpcConfig = vpcConfig;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.StartSentimentDetectionJobAsync(request);
            return new StartComprehendJobResult(
                JobId: resp.JobId,
                JobArn: resp.JobArn,
                JobStatus: resp.JobStatus?.Value);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start sentiment detection job");
        }
    }

    /// <summary>
    /// Stop an asynchronous sentiment detection job.
    /// </summary>
    public static async Task<StopComprehendJobResult>
        StopSentimentDetectionJobAsync(
            string jobId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopSentimentDetectionJobRequest { JobId = jobId };

        try
        {
            var resp = await client.StopSentimentDetectionJobAsync(request);
            return new StopComprehendJobResult(
                JobId: resp.JobId,
                JobStatus: resp.JobStatus?.Value);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop sentiment detection job '{jobId}'");
        }
    }

    /// <summary>
    /// Describe a sentiment detection job.
    /// </summary>
    public static async Task<DescribeSentimentDetectionJobResult>
        DescribeSentimentDetectionJobAsync(
            string jobId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeSentimentDetectionJobRequest
        {
            JobId = jobId
        };

        try
        {
            var resp = await client.DescribeSentimentDetectionJobAsync(request);
            return new DescribeSentimentDetectionJobResult(
                JobProperties: resp.SentimentDetectionJobProperties);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe sentiment detection job '{jobId}'");
        }
    }

    /// <summary>
    /// List sentiment detection jobs.
    /// </summary>
    public static async Task<ListSentimentDetectionJobsResult>
        ListSentimentDetectionJobsAsync(
            SentimentDetectionJobFilter? filter = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListSentimentDetectionJobsRequest();
        if (filter != null) request.Filter = filter;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListSentimentDetectionJobsAsync(request);
            return new ListSentimentDetectionJobsResult(
                Jobs: resp.SentimentDetectionJobPropertiesList,
                NextToken: resp.NextToken);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list sentiment detection jobs");
        }
    }

    // -----------------------------------------------------------------------
    // Entities detection jobs
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start an asynchronous entities detection job.
    /// </summary>
    public static async Task<StartComprehendJobResult>
        StartEntitiesDetectionJobAsync(
            InputDataConfig inputDataConfig,
            OutputDataConfig outputDataConfig,
            string dataAccessRoleArn,
            string languageCode,
            string? jobName = null,
            string? entityRecognizerArn = null,
            string? clientRequestToken = null,
            VpcConfig? vpcConfig = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartEntitiesDetectionJobRequest
        {
            InputDataConfig = inputDataConfig,
            OutputDataConfig = outputDataConfig,
            DataAccessRoleArn = dataAccessRoleArn,
            LanguageCode = languageCode
        };
        if (jobName != null) request.JobName = jobName;
        if (entityRecognizerArn != null) request.EntityRecognizerArn = entityRecognizerArn;
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (vpcConfig != null) request.VpcConfig = vpcConfig;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.StartEntitiesDetectionJobAsync(request);
            return new StartComprehendJobResult(
                JobId: resp.JobId,
                JobArn: resp.JobArn,
                JobStatus: resp.JobStatus?.Value);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start entities detection job");
        }
    }

    /// <summary>
    /// Stop an asynchronous entities detection job.
    /// </summary>
    public static async Task<StopComprehendJobResult>
        StopEntitiesDetectionJobAsync(
            string jobId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopEntitiesDetectionJobRequest { JobId = jobId };

        try
        {
            var resp = await client.StopEntitiesDetectionJobAsync(request);
            return new StopComprehendJobResult(
                JobId: resp.JobId,
                JobStatus: resp.JobStatus?.Value);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop entities detection job '{jobId}'");
        }
    }

    /// <summary>
    /// Describe an entities detection job.
    /// </summary>
    public static async Task<DescribeEntitiesDetectionJobResult>
        DescribeEntitiesDetectionJobAsync(
            string jobId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEntitiesDetectionJobRequest
        {
            JobId = jobId
        };

        try
        {
            var resp = await client.DescribeEntitiesDetectionJobAsync(request);
            return new DescribeEntitiesDetectionJobResult(
                JobProperties: resp.EntitiesDetectionJobProperties);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe entities detection job '{jobId}'");
        }
    }

    /// <summary>
    /// List entities detection jobs.
    /// </summary>
    public static async Task<ListEntitiesDetectionJobsResult>
        ListEntitiesDetectionJobsAsync(
            EntitiesDetectionJobFilter? filter = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListEntitiesDetectionJobsRequest();
        if (filter != null) request.Filter = filter;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListEntitiesDetectionJobsAsync(request);
            return new ListEntitiesDetectionJobsResult(
                Jobs: resp.EntitiesDetectionJobPropertiesList,
                NextToken: resp.NextToken);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list entities detection jobs");
        }
    }

    // -----------------------------------------------------------------------
    // Key phrases detection jobs
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start an asynchronous key phrases detection job.
    /// </summary>
    public static async Task<StartComprehendJobResult>
        StartKeyPhrasesDetectionJobAsync(
            InputDataConfig inputDataConfig,
            OutputDataConfig outputDataConfig,
            string dataAccessRoleArn,
            string languageCode,
            string? jobName = null,
            string? clientRequestToken = null,
            VpcConfig? vpcConfig = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartKeyPhrasesDetectionJobRequest
        {
            InputDataConfig = inputDataConfig,
            OutputDataConfig = outputDataConfig,
            DataAccessRoleArn = dataAccessRoleArn,
            LanguageCode = languageCode
        };
        if (jobName != null) request.JobName = jobName;
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (vpcConfig != null) request.VpcConfig = vpcConfig;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.StartKeyPhrasesDetectionJobAsync(request);
            return new StartComprehendJobResult(
                JobId: resp.JobId,
                JobArn: resp.JobArn,
                JobStatus: resp.JobStatus?.Value);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start key phrases detection job");
        }
    }

    /// <summary>
    /// Stop an asynchronous key phrases detection job.
    /// </summary>
    public static async Task<StopComprehendJobResult>
        StopKeyPhrasesDetectionJobAsync(
            string jobId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopKeyPhrasesDetectionJobRequest { JobId = jobId };

        try
        {
            var resp = await client.StopKeyPhrasesDetectionJobAsync(request);
            return new StopComprehendJobResult(
                JobId: resp.JobId,
                JobStatus: resp.JobStatus?.Value);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop key phrases detection job '{jobId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Dominant language detection jobs
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start an asynchronous dominant language detection job.
    /// </summary>
    public static async Task<StartComprehendJobResult>
        StartDominantLanguageDetectionJobAsync(
            InputDataConfig inputDataConfig,
            OutputDataConfig outputDataConfig,
            string dataAccessRoleArn,
            string? jobName = null,
            string? clientRequestToken = null,
            VpcConfig? vpcConfig = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartDominantLanguageDetectionJobRequest
        {
            InputDataConfig = inputDataConfig,
            OutputDataConfig = outputDataConfig,
            DataAccessRoleArn = dataAccessRoleArn
        };
        if (jobName != null) request.JobName = jobName;
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (vpcConfig != null) request.VpcConfig = vpcConfig;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.StartDominantLanguageDetectionJobAsync(request);
            return new StartComprehendJobResult(
                JobId: resp.JobId,
                JobArn: resp.JobArn,
                JobStatus: resp.JobStatus?.Value);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start dominant language detection job");
        }
    }

    /// <summary>
    /// Stop an asynchronous dominant language detection job.
    /// </summary>
    public static async Task<StopComprehendJobResult>
        StopDominantLanguageDetectionJobAsync(
            string jobId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopDominantLanguageDetectionJobRequest
        {
            JobId = jobId
        };

        try
        {
            var resp = await client.StopDominantLanguageDetectionJobAsync(request);
            return new StopComprehendJobResult(
                JobId: resp.JobId,
                JobStatus: resp.JobStatus?.Value);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop dominant language detection job '{jobId}'");
        }
    }

    // -----------------------------------------------------------------------
    // PII entities detection jobs
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start an asynchronous PII entities detection job.
    /// </summary>
    public static async Task<StartComprehendJobResult>
        StartPiiEntitiesDetectionJobAsync(
            InputDataConfig inputDataConfig,
            OutputDataConfig outputDataConfig,
            string dataAccessRoleArn,
            string languageCode,
            string mode,
            string? jobName = null,
            string? clientRequestToken = null,
            RedactionConfig? redactionConfig = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartPiiEntitiesDetectionJobRequest
        {
            InputDataConfig = inputDataConfig,
            OutputDataConfig = outputDataConfig,
            DataAccessRoleArn = dataAccessRoleArn,
            LanguageCode = languageCode,
            Mode = mode
        };
        if (jobName != null) request.JobName = jobName;
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;
        if (redactionConfig != null) request.RedactionConfig = redactionConfig;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.StartPiiEntitiesDetectionJobAsync(request);
            return new StartComprehendJobResult(
                JobId: resp.JobId,
                JobArn: resp.JobArn,
                JobStatus: resp.JobStatus?.Value);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start PII entities detection job");
        }
    }

    /// <summary>
    /// Stop an asynchronous PII entities detection job.
    /// </summary>
    public static async Task<StopComprehendJobResult>
        StopPiiEntitiesDetectionJobAsync(
            string jobId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StopPiiEntitiesDetectionJobRequest { JobId = jobId };

        try
        {
            var resp = await client.StopPiiEntitiesDetectionJobAsync(request);
            return new StopComprehendJobResult(
                JobId: resp.JobId,
                JobStatus: resp.JobStatus?.Value);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop PII entities detection job '{jobId}'");
        }
    }

    // -----------------------------------------------------------------------
    // Document classifier management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create a document classifier.
    /// </summary>
    public static async Task<CreateDocumentClassifierResult>
        CreateDocumentClassifierAsync(
            string documentClassifierName,
            string dataAccessRoleArn,
            DocumentClassifierInputDataConfig inputDataConfig,
            string languageCode,
            DocumentClassifierOutputDataConfig? outputDataConfig = null,
            string? mode = null,
            string? modelKmsKeyId = null,
            string? modelPolicy = null,
            string? versionName = null,
            string? volumeKmsKeyId = null,
            VpcConfig? vpcConfig = null,
            List<Tag>? tags = null,
            string? clientRequestToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDocumentClassifierRequest
        {
            DocumentClassifierName = documentClassifierName,
            DataAccessRoleArn = dataAccessRoleArn,
            InputDataConfig = inputDataConfig,
            LanguageCode = languageCode
        };
        if (outputDataConfig != null) request.OutputDataConfig = outputDataConfig;
        if (mode != null) request.Mode = mode;
        if (modelKmsKeyId != null) request.ModelKmsKeyId = modelKmsKeyId;
        if (modelPolicy != null) request.ModelPolicy = modelPolicy;
        if (versionName != null) request.VersionName = versionName;
        if (volumeKmsKeyId != null) request.VolumeKmsKeyId = volumeKmsKeyId;
        if (vpcConfig != null) request.VpcConfig = vpcConfig;
        if (tags != null) request.Tags = tags;
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;

        try
        {
            var resp = await client.CreateDocumentClassifierAsync(request);
            return new CreateDocumentClassifierResult(
                DocumentClassifierArn: resp.DocumentClassifierArn);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create document classifier '{documentClassifierName}'");
        }
    }

    /// <summary>
    /// Delete a document classifier.
    /// </summary>
    public static async Task DeleteDocumentClassifierAsync(
        string documentClassifierArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteDocumentClassifierRequest
        {
            DocumentClassifierArn = documentClassifierArn
        };

        try
        {
            await client.DeleteDocumentClassifierAsync(request);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete document classifier '{documentClassifierArn}'");
        }
    }

    /// <summary>
    /// Describe a document classifier.
    /// </summary>
    public static async Task<DescribeDocumentClassifierResult>
        DescribeDocumentClassifierAsync(
            string documentClassifierArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeDocumentClassifierRequest
        {
            DocumentClassifierArn = documentClassifierArn
        };

        try
        {
            var resp = await client.DescribeDocumentClassifierAsync(request);
            return new DescribeDocumentClassifierResult(
                Properties: resp.DocumentClassifierProperties);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe document classifier '{documentClassifierArn}'");
        }
    }

    /// <summary>
    /// List document classifiers.
    /// </summary>
    public static async Task<ListDocumentClassifiersResult>
        ListDocumentClassifiersAsync(
            DocumentClassifierFilter? filter = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDocumentClassifiersRequest();
        if (filter != null) request.Filter = filter;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListDocumentClassifiersAsync(request);
            return new ListDocumentClassifiersResult(
                Classifiers: resp.DocumentClassifierPropertiesList,
                NextToken: resp.NextToken);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list document classifiers");
        }
    }

    // -----------------------------------------------------------------------
    // Entity recognizer management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Create an entity recognizer.
    /// </summary>
    public static async Task<CreateEntityRecognizerResult>
        CreateEntityRecognizerAsync(
            string recognizerName,
            string dataAccessRoleArn,
            EntityRecognizerInputDataConfig inputDataConfig,
            string languageCode,
            string? modelKmsKeyId = null,
            string? modelPolicy = null,
            string? versionName = null,
            string? volumeKmsKeyId = null,
            VpcConfig? vpcConfig = null,
            List<Tag>? tags = null,
            string? clientRequestToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateEntityRecognizerRequest
        {
            RecognizerName = recognizerName,
            DataAccessRoleArn = dataAccessRoleArn,
            InputDataConfig = inputDataConfig,
            LanguageCode = languageCode
        };
        if (modelKmsKeyId != null) request.ModelKmsKeyId = modelKmsKeyId;
        if (modelPolicy != null) request.ModelPolicy = modelPolicy;
        if (versionName != null) request.VersionName = versionName;
        if (volumeKmsKeyId != null) request.VolumeKmsKeyId = volumeKmsKeyId;
        if (vpcConfig != null) request.VpcConfig = vpcConfig;
        if (tags != null) request.Tags = tags;
        if (clientRequestToken != null) request.ClientRequestToken = clientRequestToken;

        try
        {
            var resp = await client.CreateEntityRecognizerAsync(request);
            return new CreateEntityRecognizerResult(
                EntityRecognizerArn: resp.EntityRecognizerArn);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create entity recognizer '{recognizerName}'");
        }
    }

    /// <summary>
    /// Delete an entity recognizer.
    /// </summary>
    public static async Task DeleteEntityRecognizerAsync(
        string entityRecognizerArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteEntityRecognizerRequest
        {
            EntityRecognizerArn = entityRecognizerArn
        };

        try
        {
            await client.DeleteEntityRecognizerAsync(request);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete entity recognizer '{entityRecognizerArn}'");
        }
    }

    /// <summary>
    /// Describe an entity recognizer.
    /// </summary>
    public static async Task<DescribeEntityRecognizerResult>
        DescribeEntityRecognizerAsync(
            string entityRecognizerArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeEntityRecognizerRequest
        {
            EntityRecognizerArn = entityRecognizerArn
        };

        try
        {
            var resp = await client.DescribeEntityRecognizerAsync(request);
            return new DescribeEntityRecognizerResult(
                Properties: resp.EntityRecognizerProperties);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe entity recognizer '{entityRecognizerArn}'");
        }
    }

    /// <summary>
    /// List entity recognizers.
    /// </summary>
    public static async Task<ListEntityRecognizersResult>
        ListEntityRecognizersAsync(
            EntityRecognizerFilter? filter = null,
            int? maxResults = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListEntityRecognizersRequest();
        if (filter != null) request.Filter = filter;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListEntityRecognizersAsync(request);
            return new ListEntityRecognizersResult(
                Recognizers: resp.EntityRecognizerPropertiesList,
                NextToken: resp.NextToken);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list entity recognizers");
        }
    }

    // -----------------------------------------------------------------------
    // Tagging
    // -----------------------------------------------------------------------

    /// <summary>
    /// Add tags to a Comprehend resource.
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
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Comprehend resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Comprehend resource.
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
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Comprehend resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Comprehend resource.
    /// </summary>
    public static async Task<ComprehendListTagsResult>
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
            return new ComprehendListTagsResult(
                ResourceArn: resp.ResourceArn,
                Tags: resp.Tags);
        }
        catch (AmazonComprehendException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Comprehend resource '{resourceArn}'");
        }
    }
}

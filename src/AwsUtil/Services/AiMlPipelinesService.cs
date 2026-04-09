using System.Text.Json;
using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of a Bedrock serverless inference chain.</summary>
public sealed record BedrockServerlessChainResult(
    List<BedrockChainStep> Steps,
    string FinalOutput,
    double TotalLatencyMs,
    int TotalInputTokens,
    int TotalOutputTokens);

/// <summary>A single step in a Bedrock inference chain.</summary>
public sealed record BedrockChainStep(
    int StepIndex,
    string ModelId,
    string Input,
    string Output,
    double LatencyMs);

/// <summary>Result of processing documents from S3 via Textract.</summary>
public sealed record S3DocumentProcessorResult(
    string Bucket,
    string Key,
    List<string> ExtractedText,
    int PagesProcessed,
    Dictionary<string, string>? KeyValuePairs = null,
    List<List<string>>? Tables = null);

/// <summary>Result of running an image through the moderation pipeline.</summary>
public sealed record ImageModerationPipelineResult(
    string Bucket,
    string Key,
    bool IsSafe,
    List<ModerationLabel> Labels,
    double ConfidenceThreshold);

/// <summary>A moderation label detected in an image.</summary>
public sealed record ModerationLabel(
    string Name,
    string ParentName,
    double Confidence);

/// <summary>Result of translating documents.</summary>
public sealed record TranslationPipelineResult(
    string SourceLanguage,
    string TargetLanguage,
    string OriginalText,
    string TranslatedText,
    string? StoredS3Key = null);

/// <summary>Result of generating and indexing text embeddings.</summary>
public sealed record EmbeddingIndexerResult(
    string DocumentId,
    int ChunkCount,
    string ModelId,
    string IndexTableName,
    int EmbeddingDimension);

/// <summary>
/// AI/ML pipeline orchestration combining Bedrock, Textract, Rekognition,
/// Translate, S3, and DynamoDB for serverless AI workflows.
/// </summary>
public static class AiMlPipelinesService
{
    /// <summary>
    /// Execute a multi-step Bedrock inference chain, where each step's output
    /// becomes the next step's input context.
    /// </summary>
    public static async Task<BedrockServerlessChainResult> BedrockServerlessChainAsync(
        List<(string ModelId, string PromptTemplate)> steps,
        string initialInput,
        RegionEndpoint? region = null)
    {
        try
        {
            var chainSteps = new List<BedrockChainStep>();
            var currentInput = initialInput;
            var totalInputTokens = 0;
            var totalOutputTokens = 0;
            var totalLatency = 0.0;

            for (var i = 0; i < steps.Count; i++)
            {
                var (modelId, template) = steps[i];
                var prompt = template.Replace("{input}", currentInput);

                var sw = System.Diagnostics.Stopwatch.StartNew();

                var response = await BedrockService.InvokeModelAsync(
                    modelId: modelId,
                    body: JsonSerializer.Serialize(new
                    {
                        prompt,
                        max_tokens = 2048,
                        temperature = 0.7
                    }),
                    region: region);

                sw.Stop();

                var output = response.Body ?? "";

                // Estimate token counts (rough approximation)
                var inputTokens = prompt.Length / 4;
                var outputTokens = output.Length / 4;

                totalInputTokens += inputTokens;
                totalOutputTokens += outputTokens;
                totalLatency += sw.Elapsed.TotalMilliseconds;

                chainSteps.Add(new BedrockChainStep(
                    StepIndex: i,
                    ModelId: modelId,
                    Input: prompt,
                    Output: output,
                    LatencyMs: sw.Elapsed.TotalMilliseconds));

                currentInput = output;
            }

            return new BedrockServerlessChainResult(
                Steps: chainSteps,
                FinalOutput: currentInput,
                TotalLatencyMs: totalLatency,
                TotalInputTokens: totalInputTokens,
                TotalOutputTokens: totalOutputTokens);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Bedrock serverless chain failed");
        }
    }

    /// <summary>
    /// Process a document stored in S3 using Textract to extract text,
    /// key-value pairs, and tables.
    /// </summary>
    public static async Task<S3DocumentProcessorResult> S3DocumentProcessorAsync(
        string bucket,
        string key,
        bool extractTables = false,
        bool extractKeyValues = false,
        RegionEndpoint? region = null)
    {
        try
        {
            var featureTypes = new List<string>();
            if (extractTables) featureTypes.Add("TABLES");
            if (extractKeyValues) featureTypes.Add("FORMS");

            var result = await TextractService.AnalyzeDocumentAsync(
                document: new Amazon.Textract.Model.Document
                {
                    S3Object = new Amazon.Textract.Model.S3Object
                    {
                        Bucket = bucket,
                        Name = key
                    }
                },
                featureTypes: featureTypes.Count > 0 ? featureTypes : new List<string> { "FORMS" },
                region: region);

            var extractedText = new List<string>();
            var kvPairs = new Dictionary<string, string>();
            var tables = new List<List<string>>();
            var pageCount = 0;

            if (result.Blocks != null)
            {
                foreach (var block in result.Blocks)
                {
                    if (block.BlockType == Amazon.Textract.BlockType.PAGE)
                        pageCount++;
                    else if (block.BlockType == Amazon.Textract.BlockType.LINE)
                        extractedText.Add(block.Text ?? "");
                    else if (block.BlockType == Amazon.Textract.BlockType.KEY_VALUE_SET
                             && block.Text != null)
                        kvPairs.TryAdd(block.Text, block.Text);
                }
            }

            return new S3DocumentProcessorResult(
                Bucket: bucket,
                Key: key,
                ExtractedText: extractedText,
                PagesProcessed: Math.Max(1, pageCount),
                KeyValuePairs: extractKeyValues ? kvPairs : null,
                Tables: extractTables ? tables : null);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "S3 document processing failed");
        }
    }

    /// <summary>
    /// Run an image stored in S3 through Rekognition content moderation
    /// and return safety labels with confidence scores.
    /// </summary>
    public static async Task<ImageModerationPipelineResult> ImageModerationPipelineAsync(
        string bucket,
        string key,
        double confidenceThreshold = 75.0,
        RegionEndpoint? region = null)
    {
        try
        {
            var result = await RekognitionService.DetectModerationLabelsAsync(
                image: new Amazon.Rekognition.Model.Image
                {
                    S3Object = new Amazon.Rekognition.Model.S3Object
                    {
                        Bucket = bucket,
                        Name = key
                    }
                },
                minConfidence: (float)confidenceThreshold,
                region: region);

            var labels = result.ModerationLabels?
                .Select(l => new ModerationLabel(
                    Name: l.Name ?? "Unknown",
                    ParentName: l.ParentName ?? "",
                    Confidence: (double)(l.Confidence ?? 0f)))
                .ToList() ?? new List<ModerationLabel>();

            var isSafe = !labels.Any(l => l.Confidence >= confidenceThreshold);

            return new ImageModerationPipelineResult(
                Bucket: bucket,
                Key: key,
                IsSafe: isSafe,
                Labels: labels,
                ConfidenceThreshold: confidenceThreshold);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Image moderation pipeline failed");
        }
    }

    /// <summary>
    /// Translate text using Amazon Translate, optionally storing the result in S3.
    /// </summary>
    public static async Task<TranslationPipelineResult> TranslationPipelineAsync(
        string text,
        string sourceLanguage,
        string targetLanguage,
        string? outputBucket = null,
        string? outputKeyPrefix = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var result = await TranslateService.TranslateTextAsync(
                text: text,
                sourceLanguageCode: sourceLanguage,
                targetLanguageCode: targetLanguage,
                region: region);

            var translatedText = result.TranslatedText ?? "";
            string? storedKey = null;

            // Optionally store in S3
            if (outputBucket != null && outputKeyPrefix != null)
            {
                storedKey = $"{outputKeyPrefix}/{targetLanguage}/{DateTime.UtcNow:yyyyMMddHHmmss}.txt";
                await S3Service.PutObjectAsync(
                    outputBucket, storedKey, System.Text.Encoding.UTF8.GetBytes(translatedText), region: region);
            }

            return new TranslationPipelineResult(
                SourceLanguage: sourceLanguage,
                TargetLanguage: targetLanguage,
                OriginalText: text,
                TranslatedText: translatedText,
                StoredS3Key: storedKey);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Translation pipeline failed");
        }
    }

    /// <summary>
    /// Split a document into chunks, generate embeddings via Bedrock, and index
    /// them in a DynamoDB table for later retrieval.
    /// </summary>
    public static async Task<EmbeddingIndexerResult> EmbeddingIndexerAsync(
        string documentId,
        string text,
        string indexTableName,
        string modelId = "amazon.titan-embed-text-v1",
        int chunkSize = 512,
        RegionEndpoint? region = null)
    {
        try
        {
            // Split text into chunks
            var chunks = new List<string>();
            for (var i = 0; i < text.Length; i += chunkSize)
            {
                var length = Math.Min(chunkSize, text.Length - i);
                chunks.Add(text.Substring(i, length));
            }

            var embeddingDimension = 0;

            for (var i = 0; i < chunks.Count; i++)
            {
                var embeddingRequest = JsonSerializer.Serialize(new
                {
                    inputText = chunks[i]
                });

                var response = await BedrockService.InvokeModelAsync(
                    modelId: modelId,
                    body: embeddingRequest,
                    region: region);

                // Store chunk and embedding reference in DynamoDB
                await DynamoDbService.PutItemAsync(
                    indexTableName,
                    new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                    {
                        ["pk"] = new() { S = $"doc#{documentId}" },
                        ["sk"] = new() { S = $"chunk#{i:D5}" },
                        ["text"] = new() { S = chunks[i] },
                        ["embedding"] = new() { S = response.Body ?? "" },
                        ["modelId"] = new() { S = modelId },
                        ["createdAt"] = new() { S = DateTime.UtcNow.ToString("o") }
                    },
                    region: region);

                if (embeddingDimension == 0)
                    embeddingDimension = 1536; // Default Titan embedding dimension
            }

            return new EmbeddingIndexerResult(
                DocumentId: documentId,
                ChunkCount: chunks.Count,
                ModelId: modelId,
                IndexTableName: indexTableName,
                EmbeddingDimension: embeddingDimension);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Embedding indexing failed");
        }
    }
}

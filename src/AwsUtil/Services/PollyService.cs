using Amazon;
using Amazon.Polly;
using Amazon.Polly.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of a speech synthesis.</summary>
public sealed record SynthesizeSpeechResult(
    Stream? AudioStream = null,
    string? ContentType = null,
    int RequestCharacters = 0);

/// <summary>Result of describing available voices.</summary>
public sealed record DescribeVoicesResult(
    List<Voice>? Voices = null,
    string? NextToken = null);

/// <summary>Result of getting a lexicon.</summary>
public sealed record GetLexiconResult(
    Lexicon? Lexicon = null,
    LexiconAttributes? LexiconAttributes = null);

/// <summary>Result of listing lexicons.</summary>
public sealed record ListLexiconsResult(
    List<LexiconDescription>? Lexicons = null,
    string? NextToken = null);

/// <summary>Result of starting a speech synthesis task.</summary>
public sealed record StartSpeechSynthesisTaskResult(
    SynthesisTask? SynthesisTask = null);

/// <summary>Result of getting a speech synthesis task.</summary>
public sealed record GetSpeechSynthesisTaskResult(
    SynthesisTask? SynthesisTask = null);

/// <summary>Result of listing speech synthesis tasks.</summary>
public sealed record ListSpeechSynthesisTasksResult(
    List<SynthesisTask>? SynthesisTasks = null,
    string? NextToken = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon Polly.
/// Port of the Python <c>aws_util.polly</c> module.
/// </summary>
public static class PollyService
{
    private static AmazonPollyClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonPollyClient>(region);

    // -----------------------------------------------------------------------
    // Speech synthesis
    // -----------------------------------------------------------------------

    /// <summary>
    /// Synthesize speech from text.
    /// </summary>
    public static async Task<SynthesizeSpeechResult> SynthesizeSpeechAsync(
        string text,
        string outputFormat,
        string voiceId,
        string? engine = null,
        string? languageCode = null,
        List<string>? lexiconNames = null,
        string? sampleRate = null,
        string? speechMarkTypes = null,
        string? textType = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SynthesizeSpeechRequest
        {
            Text = text,
            OutputFormat = outputFormat,
            VoiceId = voiceId
        };
        if (engine != null) request.Engine = engine;
        if (languageCode != null) request.LanguageCode = languageCode;
        if (lexiconNames != null) request.LexiconNames = lexiconNames;
        if (sampleRate != null) request.SampleRate = sampleRate;
        if (textType != null) request.TextType = textType;

        try
        {
            var resp = await client.SynthesizeSpeechAsync(request);
            return new SynthesizeSpeechResult(
                AudioStream: resp.AudioStream,
                ContentType: resp.ContentType,
                RequestCharacters: resp.RequestCharacters ?? 0);
        }
        catch (AmazonPollyException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to synthesize speech");
        }
    }

    // -----------------------------------------------------------------------
    // Voices
    // -----------------------------------------------------------------------

    /// <summary>
    /// Describe available Polly voices.
    /// </summary>
    public static async Task<DescribeVoicesResult> DescribeVoicesAsync(
        string? engine = null,
        bool? includeAdditionalLanguageCodes = null,
        string? languageCode = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeVoicesRequest();
        if (engine != null) request.Engine = engine;
        if (includeAdditionalLanguageCodes.HasValue)
            request.IncludeAdditionalLanguageCodes = includeAdditionalLanguageCodes.Value;
        if (languageCode != null) request.LanguageCode = languageCode;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.DescribeVoicesAsync(request);
            return new DescribeVoicesResult(
                Voices: resp.Voices,
                NextToken: resp.NextToken);
        }
        catch (AmazonPollyException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to describe voices");
        }
    }

    // -----------------------------------------------------------------------
    // Lexicon management
    // -----------------------------------------------------------------------

    /// <summary>
    /// Get a pronunciation lexicon.
    /// </summary>
    public static async Task<GetLexiconResult> GetLexiconAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetLexiconRequest { Name = name };

        try
        {
            var resp = await client.GetLexiconAsync(request);
            return new GetLexiconResult(
                Lexicon: resp.Lexicon,
                LexiconAttributes: resp.LexiconAttributes);
        }
        catch (AmazonPollyException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get lexicon '{name}'");
        }
    }

    /// <summary>
    /// Store a pronunciation lexicon.
    /// </summary>
    public static async Task PutLexiconAsync(
        string name,
        string content,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutLexiconRequest
        {
            Name = name,
            Content = content
        };

        try
        {
            await client.PutLexiconAsync(request);
        }
        catch (AmazonPollyException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put lexicon '{name}'");
        }
    }

    /// <summary>
    /// Delete a pronunciation lexicon.
    /// </summary>
    public static async Task DeleteLexiconAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteLexiconRequest { Name = name };

        try
        {
            await client.DeleteLexiconAsync(request);
        }
        catch (AmazonPollyException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete lexicon '{name}'");
        }
    }

    /// <summary>
    /// List pronunciation lexicons.
    /// </summary>
    public static async Task<ListLexiconsResult> ListLexiconsAsync(
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListLexiconsRequest();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListLexiconsAsync(request);
            return new ListLexiconsResult(
                Lexicons: resp.Lexicons,
                NextToken: resp.NextToken);
        }
        catch (AmazonPollyException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list lexicons");
        }
    }

    // -----------------------------------------------------------------------
    // Speech synthesis tasks
    // -----------------------------------------------------------------------

    /// <summary>
    /// Start an asynchronous speech synthesis task.
    /// </summary>
    public static async Task<StartSpeechSynthesisTaskResult>
        StartSpeechSynthesisTaskAsync(
            string text,
            string outputFormat,
            string outputS3BucketName,
            string voiceId,
            string? engine = null,
            string? languageCode = null,
            List<string>? lexiconNames = null,
            string? outputS3KeyPrefix = null,
            string? sampleRate = null,
            string? snsTopicArn = null,
            string? textType = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartSpeechSynthesisTaskRequest
        {
            Text = text,
            OutputFormat = outputFormat,
            OutputS3BucketName = outputS3BucketName,
            VoiceId = voiceId
        };
        if (engine != null) request.Engine = engine;
        if (languageCode != null) request.LanguageCode = languageCode;
        if (lexiconNames != null) request.LexiconNames = lexiconNames;
        if (outputS3KeyPrefix != null) request.OutputS3KeyPrefix = outputS3KeyPrefix;
        if (sampleRate != null) request.SampleRate = sampleRate;
        if (snsTopicArn != null) request.SnsTopicArn = snsTopicArn;
        if (textType != null) request.TextType = textType;

        try
        {
            var resp = await client.StartSpeechSynthesisTaskAsync(request);
            return new StartSpeechSynthesisTaskResult(
                SynthesisTask: resp.SynthesisTask);
        }
        catch (AmazonPollyException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start speech synthesis task");
        }
    }

    /// <summary>
    /// Get details of a speech synthesis task.
    /// </summary>
    public static async Task<GetSpeechSynthesisTaskResult>
        GetSpeechSynthesisTaskAsync(
            string taskId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetSpeechSynthesisTaskRequest { TaskId = taskId };

        try
        {
            var resp = await client.GetSpeechSynthesisTaskAsync(request);
            return new GetSpeechSynthesisTaskResult(
                SynthesisTask: resp.SynthesisTask);
        }
        catch (AmazonPollyException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get speech synthesis task '{taskId}'");
        }
    }

    /// <summary>
    /// List speech synthesis tasks.
    /// </summary>
    public static async Task<ListSpeechSynthesisTasksResult>
        ListSpeechSynthesisTasksAsync(
            string? status = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListSpeechSynthesisTasksRequest();
        if (status != null) request.Status = status;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListSpeechSynthesisTasksAsync(request);
            return new ListSpeechSynthesisTasksResult(
                SynthesisTasks: resp.SynthesisTasks,
                NextToken: resp.NextToken);
        }
        catch (AmazonPollyException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list speech synthesis tasks");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="SynthesizeSpeechAsync"/>.</summary>
    public static SynthesizeSpeechResult SynthesizeSpeech(string text, string outputFormat, string voiceId, string? engine = null, string? languageCode = null, List<string>? lexiconNames = null, string? sampleRate = null, string? speechMarkTypes = null, string? textType = null, RegionEndpoint? region = null)
        => SynthesizeSpeechAsync(text, outputFormat, voiceId, engine, languageCode, lexiconNames, sampleRate, speechMarkTypes, textType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeVoicesAsync"/>.</summary>
    public static DescribeVoicesResult DescribeVoices(string? engine = null, bool? includeAdditionalLanguageCodes = null, string? languageCode = null, string? nextToken = null, RegionEndpoint? region = null)
        => DescribeVoicesAsync(engine, includeAdditionalLanguageCodes, languageCode, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetLexiconAsync"/>.</summary>
    public static GetLexiconResult GetLexicon(string name, RegionEndpoint? region = null)
        => GetLexiconAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutLexiconAsync"/>.</summary>
    public static void PutLexicon(string name, string content, RegionEndpoint? region = null)
        => PutLexiconAsync(name, content, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteLexiconAsync"/>.</summary>
    public static void DeleteLexicon(string name, RegionEndpoint? region = null)
        => DeleteLexiconAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListLexiconsAsync"/>.</summary>
    public static ListLexiconsResult ListLexicons(string? nextToken = null, RegionEndpoint? region = null)
        => ListLexiconsAsync(nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartSpeechSynthesisTaskAsync"/>.</summary>
    public static StartSpeechSynthesisTaskResult StartSpeechSynthesisTask(string text, string outputFormat, string outputS3BucketName, string voiceId, string? engine = null, string? languageCode = null, List<string>? lexiconNames = null, string? outputS3KeyPrefix = null, string? sampleRate = null, string? snsTopicArn = null, string? textType = null, RegionEndpoint? region = null)
        => StartSpeechSynthesisTaskAsync(text, outputFormat, outputS3BucketName, voiceId, engine, languageCode, lexiconNames, outputS3KeyPrefix, sampleRate, snsTopicArn, textType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetSpeechSynthesisTaskAsync"/>.</summary>
    public static GetSpeechSynthesisTaskResult GetSpeechSynthesisTask(string taskId, RegionEndpoint? region = null)
        => GetSpeechSynthesisTaskAsync(taskId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListSpeechSynthesisTasksAsync"/>.</summary>
    public static ListSpeechSynthesisTasksResult ListSpeechSynthesisTasks(string? status = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => ListSpeechSynthesisTasksAsync(status, nextToken, maxResults, region).GetAwaiter().GetResult();

}

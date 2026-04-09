using Amazon;
using Amazon.Lex;
using Amazon.Lex.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

public sealed record LexPostTextResult(
    string? IntentName = null,
    string? DialogState = null,
    string? Message = null,
    string? MessageFormat = null,
    Dictionary<string, string>? Slots = null,
    Dictionary<string, string>? SessionAttributes = null,
    string? SlotToElicit = null,
    SentimentResponse? SentimentResponse = null,
    List<string>? AlternativeIntents = null,
    string? BotVersion = null,
    string? SessionId = null);

public sealed record LexPostContentResult(
    string? IntentName = null,
    string? DialogState = null,
    string? Message = null,
    string? MessageFormat = null,
    string? InputTranscript = null,
    string? ContentType = null,
    string? SlotToElicit = null,
    string? Slots = null,
    string? SessionAttributes = null,
    string? SentimentResponse = null,
    string? BotVersion = null,
    string? SessionId = null,
    Stream? AudioStream = null);

public sealed record LexPutSessionResult(
    string? IntentName = null,
    string? DialogState = null,
    string? Message = null,
    string? MessageFormat = null,
    string? ContentType = null,
    string? SlotToElicit = null,
    string? Slots = null,
    string? SessionAttributes = null,
    string? SessionId = null,
    Stream? AudioStream = null);

public sealed record LexGetSessionResult(
    string? SessionId = null,
    List<IntentSummary>? RecentIntentSummaryView = null,
    Dictionary<string, string>? SessionAttributes = null,
    DialogAction? DialogAction = null);

public sealed record LexDeleteSessionResult(
    string? BotName = null,
    string? BotAlias = null,
    string? UserId = null,
    string? SessionId = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon Lex Runtime Service (V1).
/// </summary>
public static class LexRuntimeService
{
    private static AmazonLexClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonLexClient>(region);

    /// <summary>Send text input to a Lex bot.</summary>
    public static async Task<LexPostTextResult> PostTextAsync(
        string botName,
        string botAlias,
        string userId,
        string inputText,
        Dictionary<string, string>? sessionAttributes = null,
        Dictionary<string, string>? requestAttributes = null,
        List<ActiveContext>? activeContexts = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PostTextRequest
        {
            BotName = botName,
            BotAlias = botAlias,
            UserId = userId,
            InputText = inputText
        };
        if (sessionAttributes != null)
            request.SessionAttributes = sessionAttributes;
        if (requestAttributes != null)
            request.RequestAttributes = requestAttributes;
        if (activeContexts != null) request.ActiveContexts = activeContexts;

        try
        {
            var resp = await client.PostTextAsync(request);
            return new LexPostTextResult(
                IntentName: resp.IntentName,
                DialogState: resp.DialogState?.Value,
                Message: resp.Message,
                MessageFormat: resp.MessageFormat?.Value,
                Slots: resp.Slots,
                SessionAttributes: resp.SessionAttributes,
                SlotToElicit: resp.SlotToElicit,
                SentimentResponse: resp.SentimentResponse,
                BotVersion: resp.BotVersion,
                SessionId: resp.SessionId);
        }
        catch (AmazonLexException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to post text to bot '{botName}'");
        }
    }

    /// <summary>Send audio/text content to a Lex bot.</summary>
    public static async Task<LexPostContentResult> PostContentAsync(
        string botName,
        string botAlias,
        string userId,
        string contentType,
        Stream inputStream,
        string? accept = null,
        string? sessionAttributes = null,
        string? requestAttributes = null,
        string? activeContexts = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PostContentRequest
        {
            BotName = botName,
            BotAlias = botAlias,
            UserId = userId,
            ContentType = contentType,
            InputStream = inputStream
        };
        if (accept != null) request.Accept = accept;
        if (sessionAttributes != null)
            request.SessionAttributes = sessionAttributes;
        if (requestAttributes != null)
            request.RequestAttributes = requestAttributes;
        if (activeContexts != null)
            request.ActiveContexts = activeContexts;

        try
        {
            var resp = await client.PostContentAsync(request);
            return new LexPostContentResult(
                IntentName: resp.IntentName,
                DialogState: resp.DialogState?.Value,
                Message: resp.Message,
                MessageFormat: resp.MessageFormat?.Value,
                InputTranscript: resp.InputTranscript,
                ContentType: resp.ContentType,
                SlotToElicit: resp.SlotToElicit,
                Slots: resp.Slots,
                SessionAttributes: resp.SessionAttributes,
                SentimentResponse: resp.SentimentResponse,
                BotVersion: resp.BotVersion,
                SessionId: resp.SessionId,
                AudioStream: resp.AudioStream);
        }
        catch (AmazonLexException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to post content to bot '{botName}'");
        }
    }

    /// <summary>Create or update a session for a Lex bot.</summary>
    public static async Task<LexPutSessionResult> PutSessionAsync(
        string botName,
        string botAlias,
        string userId,
        DialogAction? dialogAction = null,
        List<IntentSummary>? recentIntentSummaryView = null,
        Dictionary<string, string>? sessionAttributes = null,
        string? accept = null,
        List<ActiveContext>? activeContexts = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutSessionRequest
        {
            BotName = botName,
            BotAlias = botAlias,
            UserId = userId
        };
        if (dialogAction != null) request.DialogAction = dialogAction;
        if (recentIntentSummaryView != null)
            request.RecentIntentSummaryView = recentIntentSummaryView;
        if (sessionAttributes != null)
            request.SessionAttributes = sessionAttributes;
        if (accept != null) request.Accept = accept;
        if (activeContexts != null) request.ActiveContexts = activeContexts;

        try
        {
            var resp = await client.PutSessionAsync(request);
            return new LexPutSessionResult(
                IntentName: resp.IntentName,
                DialogState: resp.DialogState?.Value,
                Message: resp.Message,
                MessageFormat: resp.MessageFormat?.Value,
                ContentType: resp.ContentType,
                SlotToElicit: resp.SlotToElicit,
                Slots: resp.Slots,
                SessionAttributes: resp.SessionAttributes,
                SessionId: resp.SessionId,
                AudioStream: resp.AudioStream);
        }
        catch (AmazonLexException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put session for bot '{botName}'");
        }
    }

    /// <summary>Get the current session state for a Lex bot.</summary>
    public static async Task<LexGetSessionResult> GetSessionAsync(
        string botName,
        string botAlias,
        string userId,
        string? checkpointLabelFilter = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetSessionRequest
        {
            BotName = botName,
            BotAlias = botAlias,
            UserId = userId
        };
        if (checkpointLabelFilter != null)
            request.CheckpointLabelFilter = checkpointLabelFilter;

        try
        {
            var resp = await client.GetSessionAsync(request);
            return new LexGetSessionResult(
                SessionId: resp.SessionId,
                RecentIntentSummaryView: resp.RecentIntentSummaryView,
                SessionAttributes: resp.SessionAttributes,
                DialogAction: resp.DialogAction);
        }
        catch (AmazonLexException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get session for bot '{botName}'");
        }
    }

    /// <summary>Delete a session for a Lex bot.</summary>
    public static async Task<LexDeleteSessionResult> DeleteSessionAsync(
        string botName,
        string botAlias,
        string userId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteSessionAsync(
                new DeleteSessionRequest
                {
                    BotName = botName,
                    BotAlias = botAlias,
                    UserId = userId
                });
            return new LexDeleteSessionResult(
                BotName: resp.BotName,
                BotAlias: resp.BotAlias,
                UserId: resp.UserId,
                SessionId: resp.SessionId);
        }
        catch (AmazonLexException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete session for bot '{botName}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="PostTextAsync"/>.</summary>
    public static LexPostTextResult PostText(string botName, string botAlias, string userId, string inputText, Dictionary<string, string>? sessionAttributes = null, Dictionary<string, string>? requestAttributes = null, List<ActiveContext>? activeContexts = null, RegionEndpoint? region = null)
        => PostTextAsync(botName, botAlias, userId, inputText, sessionAttributes, requestAttributes, activeContexts, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PostContentAsync"/>.</summary>
    public static LexPostContentResult PostContent(string botName, string botAlias, string userId, string contentType, Stream inputStream, string? accept = null, string? sessionAttributes = null, string? requestAttributes = null, string? activeContexts = null, RegionEndpoint? region = null)
        => PostContentAsync(botName, botAlias, userId, contentType, inputStream, accept, sessionAttributes, requestAttributes, activeContexts, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutSessionAsync"/>.</summary>
    public static LexPutSessionResult PutSession(string botName, string botAlias, string userId, DialogAction? dialogAction = null, List<IntentSummary>? recentIntentSummaryView = null, Dictionary<string, string>? sessionAttributes = null, string? accept = null, List<ActiveContext>? activeContexts = null, RegionEndpoint? region = null)
        => PutSessionAsync(botName, botAlias, userId, dialogAction, recentIntentSummaryView, sessionAttributes, accept, activeContexts, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetSessionAsync"/>.</summary>
    public static LexGetSessionResult GetSession(string botName, string botAlias, string userId, string? checkpointLabelFilter = null, RegionEndpoint? region = null)
        => GetSessionAsync(botName, botAlias, userId, checkpointLabelFilter, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSessionAsync"/>.</summary>
    public static LexDeleteSessionResult DeleteSession(string botName, string botAlias, string userId, RegionEndpoint? region = null)
        => DeleteSessionAsync(botName, botAlias, userId, region).GetAwaiter().GetResult();

}

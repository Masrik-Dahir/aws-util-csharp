using Amazon;
using Amazon.LexModelBuildingService;
using Amazon.LexModelBuildingService.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

public sealed record LexBotResult(
    string? Name = null,
    string? Description = null,
    string? Status = null,
    string? Version = null,
    string? Checksum = null,
    DateTime? CreatedDate = null,
    DateTime? LastUpdatedDate = null);

public sealed record LexListBotsResult(
    List<BotMetadata>? Bots = null,
    string? NextToken = null);

public sealed record LexDeleteBotResult(string? RequestId = null);

public sealed record LexIntentResult(
    string? Name = null,
    string? Description = null,
    string? Version = null,
    string? Checksum = null,
    DateTime? CreatedDate = null,
    DateTime? LastUpdatedDate = null);

public sealed record LexListIntentsResult(
    List<IntentMetadata>? Intents = null,
    string? NextToken = null);

public sealed record LexSlotTypeResult(
    string? Name = null,
    string? Description = null,
    string? Version = null,
    string? Checksum = null,
    DateTime? CreatedDate = null,
    DateTime? LastUpdatedDate = null);

public sealed record LexListSlotTypesResult(
    List<SlotTypeMetadata>? SlotTypes = null,
    string? NextToken = null);

public sealed record LexBotVersionResult(
    string? Name = null,
    string? Version = null,
    string? Checksum = null);

public sealed record LexListBotVersionsResult(
    List<BotMetadata>? Bots = null,
    string? NextToken = null);

public sealed record LexBotAliasResult(
    string? Name = null,
    string? Description = null,
    string? BotVersion = null,
    string? BotName = null,
    string? Checksum = null);

public sealed record LexListBotAliasesResult(
    List<BotAliasMetadata>? BotAliases = null,
    string? NextToken = null);

public sealed record LexBotChannelAssociationResult(
    string? Name = null,
    string? BotName = null,
    string? BotAlias = null,
    string? Type = null,
    string? Status = null);

public sealed record LexListBotChannelAssociationsResult(
    List<BotChannelAssociation>? BotChannelAssociations = null,
    string? NextToken = null);

public sealed record LexListBuiltinIntentsResult(
    List<BuiltinIntentMetadata>? Intents = null,
    string? NextToken = null);

public sealed record LexListBuiltinSlotTypesResult(
    List<BuiltinSlotTypeMetadata>? SlotTypes = null,
    string? NextToken = null);

public sealed record LexIntentVersionResult(
    string? Name = null,
    string? Version = null,
    string? Checksum = null);

public sealed record LexSlotTypeVersionResult(
    string? Name = null,
    string? Version = null,
    string? Checksum = null);

public sealed record LexExportResult(
    string? Name = null,
    string? Version = null,
    string? ResourceType = null,
    string? ExportType = null,
    string? ExportStatus = null,
    string? Url = null);

public sealed record LexImportResult(
    string? Name = null,
    string? ResourceType = null,
    string? ImportId = null,
    string? ImportStatus = null,
    DateTime? CreatedDate = null);

public sealed record LexGetImportResult(
    string? Name = null,
    string? ResourceType = null,
    string? ImportId = null,
    string? ImportStatus = null,
    DateTime? CreatedDate = null);

public sealed record LexUtterancesViewResult(
    string? BotName = null,
    List<UtteranceList>? Utterances = null);

public sealed record LexDeleteUtterancesResult(string? RequestId = null);

public sealed record LexTagResult(string? RequestId = null);

public sealed record LexListTagsResult(
    List<Tag>? Tags = null,
    string? RequestId = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon Lex Model Building Service (V1).
/// </summary>
public static class LexModelsService
{
    private static AmazonLexModelBuildingServiceClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonLexModelBuildingServiceClient>(
            region);

    // ── Bot ──────────────────────────────────────────────────────────

    /// <summary>Create or update a Lex bot.</summary>
    public static async Task<LexBotResult> PutBotAsync(
        string name,
        Locale locale,
        bool childDirected,
        List<Intent>? intents = null,
        string? description = null,
        Prompt? clarificationPrompt = null,
        Statement? abortStatement = null,
        int? idleSessionTTLInSeconds = null,
        string? voiceId = null,
        string? checksum = null,
        bool? createVersion = null,
        ProcessBehavior? processBehavior = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutBotRequest
        {
            Name = name,
            Locale = locale,
            ChildDirected = childDirected
        };
        if (intents != null) request.Intents = intents;
        if (description != null) request.Description = description;
        if (clarificationPrompt != null)
            request.ClarificationPrompt = clarificationPrompt;
        if (abortStatement != null) request.AbortStatement = abortStatement;
        if (idleSessionTTLInSeconds.HasValue)
            request.IdleSessionTTLInSeconds = idleSessionTTLInSeconds.Value;
        if (voiceId != null) request.VoiceId = voiceId;
        if (checksum != null) request.Checksum = checksum;
        if (createVersion.HasValue)
            request.CreateVersion = createVersion.Value;
        if (processBehavior != null)
            request.ProcessBehavior = processBehavior;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.PutBotAsync(request);
            return new LexBotResult(
                Name: resp.Name,
                Description: resp.Description,
                Status: resp.Status?.Value,
                Version: resp.Version,
                Checksum: resp.Checksum,
                CreatedDate: resp.CreatedDate,
                LastUpdatedDate: resp.LastUpdatedDate);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put bot '{name}'");
        }
    }

    /// <summary>Get a Lex bot.</summary>
    public static async Task<LexBotResult> GetBotAsync(
        string name,
        string versionOrAlias,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetBotAsync(new GetBotRequest
            {
                Name = name,
                VersionOrAlias = versionOrAlias
            });
            return new LexBotResult(
                Name: resp.Name,
                Description: resp.Description,
                Status: resp.Status?.Value,
                Version: resp.Version,
                Checksum: resp.Checksum,
                CreatedDate: resp.CreatedDate,
                LastUpdatedDate: resp.LastUpdatedDate);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get bot '{name}'");
        }
    }

    /// <summary>List Lex bots.</summary>
    public static async Task<LexListBotsResult> GetBotsAsync(
        string? nameContains = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetBotsRequest();
        if (nameContains != null) request.NameContains = nameContains;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.GetBotsAsync(request);
            return new LexListBotsResult(
                Bots: resp.Bots,
                NextToken: resp.NextToken);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get bots");
        }
    }

    /// <summary>Delete a Lex bot.</summary>
    public static async Task<LexDeleteBotResult> DeleteBotAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteBotAsync(
                new DeleteBotRequest { Name = name });
            return new LexDeleteBotResult();
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete bot '{name}'");
        }
    }

    // ── Intent ───────────────────────────────────────────────────────

    /// <summary>Create or update a Lex intent.</summary>
    public static async Task<LexIntentResult> PutIntentAsync(
        string name,
        FulfillmentActivity? fulfillmentActivity = null,
        string? description = null,
        List<Slot>? slots = null,
        List<string>? sampleUtterances = null,
        Prompt? confirmationPrompt = null,
        Statement? rejectionStatement = null,
        FollowUpPrompt? followUpPrompt = null,
        Statement? conclusionStatement = null,
        CodeHook? dialogCodeHook = null,
        string? parentIntentSignature = null,
        string? checksum = null,
        bool? createVersion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutIntentRequest { Name = name };
        if (fulfillmentActivity != null)
            request.FulfillmentActivity = fulfillmentActivity;
        if (description != null) request.Description = description;
        if (slots != null) request.Slots = slots;
        if (sampleUtterances != null)
            request.SampleUtterances = sampleUtterances;
        if (confirmationPrompt != null)
            request.ConfirmationPrompt = confirmationPrompt;
        if (rejectionStatement != null)
            request.RejectionStatement = rejectionStatement;
        if (followUpPrompt != null) request.FollowUpPrompt = followUpPrompt;
        if (conclusionStatement != null)
            request.ConclusionStatement = conclusionStatement;
        if (dialogCodeHook != null) request.DialogCodeHook = dialogCodeHook;
        if (parentIntentSignature != null)
            request.ParentIntentSignature = parentIntentSignature;
        if (checksum != null) request.Checksum = checksum;
        if (createVersion.HasValue)
            request.CreateVersion = createVersion.Value;

        try
        {
            var resp = await client.PutIntentAsync(request);
            return new LexIntentResult(
                Name: resp.Name,
                Description: resp.Description,
                Version: resp.Version,
                Checksum: resp.Checksum,
                CreatedDate: resp.CreatedDate,
                LastUpdatedDate: resp.LastUpdatedDate);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put intent '{name}'");
        }
    }

    /// <summary>Get a Lex intent.</summary>
    public static async Task<LexIntentResult> GetIntentAsync(
        string name,
        string version,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetIntentAsync(new GetIntentRequest
            {
                Name = name,
                Version = version
            });
            return new LexIntentResult(
                Name: resp.Name,
                Description: resp.Description,
                Version: resp.Version,
                Checksum: resp.Checksum,
                CreatedDate: resp.CreatedDate,
                LastUpdatedDate: resp.LastUpdatedDate);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get intent '{name}'");
        }
    }

    /// <summary>List Lex intents.</summary>
    public static async Task<LexListIntentsResult> GetIntentsAsync(
        string? nameContains = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetIntentsRequest();
        if (nameContains != null) request.NameContains = nameContains;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.GetIntentsAsync(request);
            return new LexListIntentsResult(
                Intents: resp.Intents,
                NextToken: resp.NextToken);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get intents");
        }
    }

    /// <summary>Delete a Lex intent.</summary>
    public static async Task<LexDeleteBotResult> DeleteIntentAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteIntentAsync(
                new DeleteIntentRequest { Name = name });
            return new LexDeleteBotResult();
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete intent '{name}'");
        }
    }

    // ── Slot Type ────────────────────────────────────────────────────

    /// <summary>Create or update a Lex slot type.</summary>
    public static async Task<LexSlotTypeResult> PutSlotTypeAsync(
        string name,
        List<EnumerationValue>? enumerationValues = null,
        string? description = null,
        SlotValueSelectionStrategy? valueSelectionStrategy = null,
        string? checksum = null,
        bool? createVersion = null,
        string? parentSlotTypeSignature = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutSlotTypeRequest { Name = name };
        if (enumerationValues != null)
            request.EnumerationValues = enumerationValues;
        if (description != null) request.Description = description;
        if (valueSelectionStrategy != null)
            request.ValueSelectionStrategy = valueSelectionStrategy;
        if (checksum != null) request.Checksum = checksum;
        if (createVersion.HasValue)
            request.CreateVersion = createVersion.Value;
        if (parentSlotTypeSignature != null)
            request.ParentSlotTypeSignature = parentSlotTypeSignature;

        try
        {
            var resp = await client.PutSlotTypeAsync(request);
            return new LexSlotTypeResult(
                Name: resp.Name,
                Description: resp.Description,
                Version: resp.Version,
                Checksum: resp.Checksum,
                CreatedDate: resp.CreatedDate,
                LastUpdatedDate: resp.LastUpdatedDate);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put slot type '{name}'");
        }
    }

    /// <summary>Get a Lex slot type.</summary>
    public static async Task<LexSlotTypeResult> GetSlotTypeAsync(
        string name,
        string version,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetSlotTypeAsync(new GetSlotTypeRequest
            {
                Name = name,
                Version = version
            });
            return new LexSlotTypeResult(
                Name: resp.Name,
                Description: resp.Description,
                Version: resp.Version,
                Checksum: resp.Checksum,
                CreatedDate: resp.CreatedDate,
                LastUpdatedDate: resp.LastUpdatedDate);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get slot type '{name}'");
        }
    }

    /// <summary>List Lex slot types.</summary>
    public static async Task<LexListSlotTypesResult> GetSlotTypesAsync(
        string? nameContains = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetSlotTypesRequest();
        if (nameContains != null) request.NameContains = nameContains;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.GetSlotTypesAsync(request);
            return new LexListSlotTypesResult(
                SlotTypes: resp.SlotTypes,
                NextToken: resp.NextToken);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get slot types");
        }
    }

    /// <summary>Delete a Lex slot type.</summary>
    public static async Task<LexDeleteBotResult> DeleteSlotTypeAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteSlotTypeAsync(
                new DeleteSlotTypeRequest { Name = name });
            return new LexDeleteBotResult();
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete slot type '{name}'");
        }
    }

    // ── Bot Versions ─────────────────────────────────────────────────

    /// <summary>Create a new version of a Lex bot.</summary>
    public static async Task<LexBotVersionResult> CreateBotVersionAsync(
        string name,
        string? checksum = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateBotVersionRequest { Name = name };
        if (checksum != null) request.Checksum = checksum;

        try
        {
            var resp = await client.CreateBotVersionAsync(request);
            return new LexBotVersionResult(
                Name: resp.Name,
                Version: resp.Version,
                Checksum: resp.Checksum);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create bot version for '{name}'");
        }
    }

    /// <summary>List versions of a Lex bot.</summary>
    public static async Task<LexListBotVersionsResult>
        GetBotVersionsAsync(
            string name,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetBotVersionsRequest { Name = name };
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.GetBotVersionsAsync(request);
            return new LexListBotVersionsResult(
                Bots: resp.Bots,
                NextToken: resp.NextToken);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get bot versions for '{name}'");
        }
    }

    // ── Bot Alias ────────────────────────────────────────────────────

    /// <summary>Create or update a Lex bot alias.</summary>
    public static async Task<LexBotAliasResult> PutBotAliasAsync(
        string name,
        string botName,
        string botVersion,
        string? description = null,
        string? checksum = null,
        List<Tag>? tags = null,
        ConversationLogsRequest? conversationLogs = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutBotAliasRequest
        {
            Name = name,
            BotName = botName,
            BotVersion = botVersion
        };
        if (description != null) request.Description = description;
        if (checksum != null) request.Checksum = checksum;
        if (tags != null) request.Tags = tags;
        if (conversationLogs != null)
            request.ConversationLogs = conversationLogs;

        try
        {
            var resp = await client.PutBotAliasAsync(request);
            return new LexBotAliasResult(
                Name: resp.Name,
                Description: resp.Description,
                BotVersion: resp.BotVersion,
                BotName: resp.BotName,
                Checksum: resp.Checksum);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put bot alias '{name}'");
        }
    }

    /// <summary>Get a Lex bot alias.</summary>
    public static async Task<LexBotAliasResult> GetBotAliasAsync(
        string name,
        string botName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetBotAliasAsync(new GetBotAliasRequest
            {
                Name = name,
                BotName = botName
            });
            return new LexBotAliasResult(
                Name: resp.Name,
                Description: resp.Description,
                BotVersion: resp.BotVersion,
                BotName: resp.BotName,
                Checksum: resp.Checksum);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get bot alias '{name}'");
        }
    }

    /// <summary>List Lex bot aliases.</summary>
    public static async Task<LexListBotAliasesResult> GetBotAliasesAsync(
        string botName,
        string? nameContains = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetBotAliasesRequest { BotName = botName };
        if (nameContains != null) request.NameContains = nameContains;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.GetBotAliasesAsync(request);
            return new LexListBotAliasesResult(
                BotAliases: resp.BotAliases,
                NextToken: resp.NextToken);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get bot aliases for '{botName}'");
        }
    }

    /// <summary>Delete a Lex bot alias.</summary>
    public static async Task<LexDeleteBotResult> DeleteBotAliasAsync(
        string name,
        string botName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteBotAliasAsync(new DeleteBotAliasRequest
            {
                Name = name,
                BotName = botName
            });
            return new LexDeleteBotResult();
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete bot alias '{name}'");
        }
    }

    // ── Bot Channel Associations ─────────────────────────────────────

    /// <summary>List bot channel associations.</summary>
    public static async Task<LexListBotChannelAssociationsResult>
        GetBotChannelAssociationsAsync(
            string botName,
            string botAlias,
            string? nameContains = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetBotChannelAssociationsRequest
        {
            BotName = botName,
            BotAlias = botAlias
        };
        if (nameContains != null) request.NameContains = nameContains;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.GetBotChannelAssociationsAsync(request);
            return new LexListBotChannelAssociationsResult(
                BotChannelAssociations: resp.BotChannelAssociations,
                NextToken: resp.NextToken);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get bot channel associations for '{botName}'");
        }
    }

    /// <summary>Get a specific bot channel association.</summary>
    public static async Task<LexBotChannelAssociationResult>
        GetBotChannelAssociationAsync(
            string name,
            string botName,
            string botAlias,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetBotChannelAssociationAsync(
                new GetBotChannelAssociationRequest
                {
                    Name = name,
                    BotName = botName,
                    BotAlias = botAlias
                });
            return new LexBotChannelAssociationResult(
                Name: resp.Name,
                BotName: resp.BotName,
                BotAlias: resp.BotAlias,
                Type: resp.Type?.Value,
                Status: resp.Status?.Value);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get bot channel association '{name}'");
        }
    }

    // ── Built-in Resources ───────────────────────────────────────────

    /// <summary>List built-in Lex intents.</summary>
    public static async Task<LexListBuiltinIntentsResult>
        GetBuiltinIntentsAsync(
            Locale? locale = null,
            string? signatureContains = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetBuiltinIntentsRequest();
        if (locale != null) request.Locale = locale;
        if (signatureContains != null)
            request.SignatureContains = signatureContains;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.GetBuiltinIntentsAsync(request);
            return new LexListBuiltinIntentsResult(
                Intents: resp.Intents,
                NextToken: resp.NextToken);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get builtin intents");
        }
    }

    /// <summary>List built-in Lex slot types.</summary>
    public static async Task<LexListBuiltinSlotTypesResult>
        GetBuiltinSlotTypesAsync(
            Locale? locale = null,
            string? signatureContains = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetBuiltinSlotTypesRequest();
        if (locale != null) request.Locale = locale;
        if (signatureContains != null)
            request.SignatureContains = signatureContains;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.GetBuiltinSlotTypesAsync(request);
            return new LexListBuiltinSlotTypesResult(
                SlotTypes: resp.SlotTypes,
                NextToken: resp.NextToken);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get builtin slot types");
        }
    }

    // ── Versioning ───────────────────────────────────────────────────

    /// <summary>Create a new version of a Lex intent.</summary>
    public static async Task<LexIntentVersionResult>
        CreateIntentVersionAsync(
            string name,
            string? checksum = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateIntentVersionRequest { Name = name };
        if (checksum != null) request.Checksum = checksum;

        try
        {
            var resp = await client.CreateIntentVersionAsync(request);
            return new LexIntentVersionResult(
                Name: resp.Name,
                Version: resp.Version,
                Checksum: resp.Checksum);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create intent version for '{name}'");
        }
    }

    /// <summary>Create a new version of a Lex slot type.</summary>
    public static async Task<LexSlotTypeVersionResult>
        CreateSlotTypeVersionAsync(
            string name,
            string? checksum = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSlotTypeVersionRequest { Name = name };
        if (checksum != null) request.Checksum = checksum;

        try
        {
            var resp = await client.CreateSlotTypeVersionAsync(request);
            return new LexSlotTypeVersionResult(
                Name: resp.Name,
                Version: resp.Version,
                Checksum: resp.Checksum);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create slot type version for '{name}'");
        }
    }

    // ── Export / Import ──────────────────────────────────────────────

    /// <summary>Get an export of a Lex resource.</summary>
    public static async Task<LexExportResult> GetExportAsync(
        string name,
        string version,
        ResourceType resourceType,
        ExportType exportType,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetExportAsync(new GetExportRequest
            {
                Name = name,
                Version = version,
                ResourceType = resourceType,
                ExportType = exportType
            });
            return new LexExportResult(
                Name: resp.Name,
                Version: resp.Version,
                ResourceType: resp.ResourceType?.Value,
                ExportType: resp.ExportType?.Value,
                ExportStatus: resp.ExportStatus?.Value,
                Url: resp.Url);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get export for '{name}'");
        }
    }

    /// <summary>Start an import of a Lex resource.</summary>
    public static async Task<LexImportResult> StartImportAsync(
        MemoryStream payload,
        ResourceType resourceType,
        MergeStrategy mergeStrategy,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartImportRequest
        {
            Payload = payload,
            ResourceType = resourceType,
            MergeStrategy = mergeStrategy
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.StartImportAsync(request);
            return new LexImportResult(
                Name: resp.Name,
                ResourceType: resp.ResourceType?.Value,
                ImportId: resp.ImportId,
                ImportStatus: resp.ImportStatus?.Value,
                CreatedDate: resp.CreatedDate);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to start import");
        }
    }

    /// <summary>Get the status of a Lex import.</summary>
    public static async Task<LexGetImportResult> GetImportAsync(
        string importId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetImportAsync(new GetImportRequest
            {
                ImportId = importId
            });
            return new LexGetImportResult(
                Name: resp.Name,
                ResourceType: resp.ResourceType?.Value,
                ImportId: resp.ImportId,
                ImportStatus: resp.ImportStatus?.Value,
                CreatedDate: resp.CreatedDate);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get import '{importId}'");
        }
    }

    // ── Utterances ───────────────────────────────────────────────────

    /// <summary>Get utterance statistics for a Lex bot.</summary>
    public static async Task<LexUtterancesViewResult>
        GetUtterancesViewAsync(
            string botName,
            List<string> botVersions,
            StatusType statusType,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetUtterancesViewAsync(
                new GetUtterancesViewRequest
                {
                    BotName = botName,
                    BotVersions = botVersions,
                    StatusType = statusType
                });
            return new LexUtterancesViewResult(
                BotName: resp.BotName,
                Utterances: resp.Utterances);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get utterances view for '{botName}'");
        }
    }

    /// <summary>Delete stored utterances for a Lex bot.</summary>
    public static async Task<LexDeleteUtterancesResult>
        DeleteUtterancesAsync(
            string botName,
            string userId,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteUtterancesAsync(
                new DeleteUtterancesRequest
                {
                    BotName = botName,
                    UserId = userId
                });
            return new LexDeleteUtterancesResult();
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete utterances for bot '{botName}'");
        }
    }

    // ── Tags ─────────────────────────────────────────────────────────

    /// <summary>Tag a Lex resource.</summary>
    public static async Task<LexTagResult> TagResourceAsync(
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
            return new LexTagResult();
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>Remove tags from a Lex resource.</summary>
    public static async Task<LexTagResult> UntagResourceAsync(
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
            return new LexTagResult();
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>List tags for a Lex resource.</summary>
    public static async Task<LexListTagsResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest
                {
                    ResourceArn = resourceArn
                });
            return new LexListTagsResult(Tags: resp.Tags);
        }
        catch (AmazonLexModelBuildingServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="PutBotAsync"/>.</summary>
    public static LexBotResult PutBot(string name, Locale locale, bool childDirected, List<Intent>? intents = null, string? description = null, Prompt? clarificationPrompt = null, Statement? abortStatement = null, int? idleSessionTTLInSeconds = null, string? voiceId = null, string? checksum = null, bool? createVersion = null, ProcessBehavior? processBehavior = null, List<Tag>? tags = null, RegionEndpoint? region = null)
        => PutBotAsync(name, locale, childDirected, intents, description, clarificationPrompt, abortStatement, idleSessionTTLInSeconds, voiceId, checksum, createVersion, processBehavior, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBotAsync"/>.</summary>
    public static LexBotResult GetBot(string name, string versionOrAlias, RegionEndpoint? region = null)
        => GetBotAsync(name, versionOrAlias, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBotsAsync"/>.</summary>
    public static LexListBotsResult GetBots(string? nameContains = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => GetBotsAsync(nameContains, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteBotAsync"/>.</summary>
    public static LexDeleteBotResult DeleteBot(string name, RegionEndpoint? region = null)
        => DeleteBotAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutIntentAsync"/>.</summary>
    public static LexIntentResult PutIntent(string name, FulfillmentActivity? fulfillmentActivity = null, string? description = null, List<Slot>? slots = null, List<string>? sampleUtterances = null, Prompt? confirmationPrompt = null, Statement? rejectionStatement = null, FollowUpPrompt? followUpPrompt = null, Statement? conclusionStatement = null, CodeHook? dialogCodeHook = null, string? parentIntentSignature = null, string? checksum = null, bool? createVersion = null, RegionEndpoint? region = null)
        => PutIntentAsync(name, fulfillmentActivity, description, slots, sampleUtterances, confirmationPrompt, rejectionStatement, followUpPrompt, conclusionStatement, dialogCodeHook, parentIntentSignature, checksum, createVersion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetIntentAsync"/>.</summary>
    public static LexIntentResult GetIntent(string name, string version, RegionEndpoint? region = null)
        => GetIntentAsync(name, version, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetIntentsAsync"/>.</summary>
    public static LexListIntentsResult GetIntents(string? nameContains = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => GetIntentsAsync(nameContains, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteIntentAsync"/>.</summary>
    public static LexDeleteBotResult DeleteIntent(string name, RegionEndpoint? region = null)
        => DeleteIntentAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutSlotTypeAsync"/>.</summary>
    public static LexSlotTypeResult PutSlotType(string name, List<EnumerationValue>? enumerationValues = null, string? description = null, SlotValueSelectionStrategy? valueSelectionStrategy = null, string? checksum = null, bool? createVersion = null, string? parentSlotTypeSignature = null, RegionEndpoint? region = null)
        => PutSlotTypeAsync(name, enumerationValues, description, valueSelectionStrategy, checksum, createVersion, parentSlotTypeSignature, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetSlotTypeAsync"/>.</summary>
    public static LexSlotTypeResult GetSlotType(string name, string version, RegionEndpoint? region = null)
        => GetSlotTypeAsync(name, version, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetSlotTypesAsync"/>.</summary>
    public static LexListSlotTypesResult GetSlotTypes(string? nameContains = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => GetSlotTypesAsync(nameContains, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteSlotTypeAsync"/>.</summary>
    public static LexDeleteBotResult DeleteSlotType(string name, RegionEndpoint? region = null)
        => DeleteSlotTypeAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateBotVersionAsync"/>.</summary>
    public static LexBotVersionResult CreateBotVersion(string name, string? checksum = null, RegionEndpoint? region = null)
        => CreateBotVersionAsync(name, checksum, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBotVersionsAsync"/>.</summary>
    public static LexListBotVersionsResult GetBotVersions(string name, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => GetBotVersionsAsync(name, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutBotAliasAsync"/>.</summary>
    public static LexBotAliasResult PutBotAlias(string name, string botName, string botVersion, string? description = null, string? checksum = null, List<Tag>? tags = null, ConversationLogsRequest? conversationLogs = null, RegionEndpoint? region = null)
        => PutBotAliasAsync(name, botName, botVersion, description, checksum, tags, conversationLogs, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBotAliasAsync"/>.</summary>
    public static LexBotAliasResult GetBotAlias(string name, string botName, RegionEndpoint? region = null)
        => GetBotAliasAsync(name, botName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBotAliasesAsync"/>.</summary>
    public static LexListBotAliasesResult GetBotAliases(string botName, string? nameContains = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => GetBotAliasesAsync(botName, nameContains, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteBotAliasAsync"/>.</summary>
    public static LexDeleteBotResult DeleteBotAlias(string name, string botName, RegionEndpoint? region = null)
        => DeleteBotAliasAsync(name, botName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBotChannelAssociationsAsync"/>.</summary>
    public static LexListBotChannelAssociationsResult GetBotChannelAssociations(string botName, string botAlias, string? nameContains = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => GetBotChannelAssociationsAsync(botName, botAlias, nameContains, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBotChannelAssociationAsync"/>.</summary>
    public static LexBotChannelAssociationResult GetBotChannelAssociation(string name, string botName, string botAlias, RegionEndpoint? region = null)
        => GetBotChannelAssociationAsync(name, botName, botAlias, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBuiltinIntentsAsync"/>.</summary>
    public static LexListBuiltinIntentsResult GetBuiltinIntents(Locale? locale = null, string? signatureContains = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => GetBuiltinIntentsAsync(locale, signatureContains, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetBuiltinSlotTypesAsync"/>.</summary>
    public static LexListBuiltinSlotTypesResult GetBuiltinSlotTypes(Locale? locale = null, string? signatureContains = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => GetBuiltinSlotTypesAsync(locale, signatureContains, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateIntentVersionAsync"/>.</summary>
    public static LexIntentVersionResult CreateIntentVersion(string name, string? checksum = null, RegionEndpoint? region = null)
        => CreateIntentVersionAsync(name, checksum, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateSlotTypeVersionAsync"/>.</summary>
    public static LexSlotTypeVersionResult CreateSlotTypeVersion(string name, string? checksum = null, RegionEndpoint? region = null)
        => CreateSlotTypeVersionAsync(name, checksum, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetExportAsync"/>.</summary>
    public static LexExportResult GetExport(string name, string version, ResourceType resourceType, ExportType exportType, RegionEndpoint? region = null)
        => GetExportAsync(name, version, resourceType, exportType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartImportAsync"/>.</summary>
    public static LexImportResult StartImport(MemoryStream payload, ResourceType resourceType, MergeStrategy mergeStrategy, List<Tag>? tags = null, RegionEndpoint? region = null)
        => StartImportAsync(payload, resourceType, mergeStrategy, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetImportAsync"/>.</summary>
    public static LexGetImportResult GetImport(string importId, RegionEndpoint? region = null)
        => GetImportAsync(importId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetUtterancesViewAsync"/>.</summary>
    public static LexUtterancesViewResult GetUtterancesView(string botName, List<string> botVersions, StatusType statusType, RegionEndpoint? region = null)
        => GetUtterancesViewAsync(botName, botVersions, statusType, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteUtterancesAsync"/>.</summary>
    public static LexDeleteUtterancesResult DeleteUtterances(string botName, string userId, RegionEndpoint? region = null)
        => DeleteUtterancesAsync(botName, userId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static LexTagResult TagResource(string resourceArn, List<Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static LexTagResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static LexListTagsResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}

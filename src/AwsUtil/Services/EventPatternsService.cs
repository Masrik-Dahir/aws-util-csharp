using System.Text.Json;
using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of matching events against a pattern.</summary>
public sealed record EventPatternMatcherResult(
    string Pattern,
    int TotalEvents,
    int MatchedEvents,
    List<MatchedEvent> Matches);

/// <summary>An event that matched a pattern.</summary>
public sealed record MatchedEvent(
    string EventId,
    string Source,
    string DetailType,
    Dictionary<string, object?> Detail);

/// <summary>Result of building a complex event filter.</summary>
public sealed record EventFilterBuilderResult(
    string FilterName,
    string FilterPatternJson,
    List<string> Sources,
    List<string> DetailTypes,
    Dictionary<string, List<string>>? DetailFilters = null);

/// <summary>Result of transforming events.</summary>
public sealed record EventTransformerResult(
    int TotalEvents,
    int TransformedEvents,
    int FailedEvents,
    List<TransformedEvent> Results);

/// <summary>A single transformed event.</summary>
public sealed record TransformedEvent(
    string OriginalSource,
    string TransformedSource,
    string OriginalDetailType,
    string TransformedDetailType,
    string TransformedDetail);

/// <summary>
/// Event pattern matching, filtering, and transformation utilities
/// orchestrating EventBridge, Lambda, and DynamoDB.
/// </summary>
public static class EventPatternsService
{
    /// <summary>
    /// Test a set of events against an EventBridge event pattern, returning
    /// which events match. Useful for validating filter rules before deployment.
    /// </summary>
    public static async Task<EventPatternMatcherResult> EventPatternMatcherAsync(
        string eventPattern,
        List<Dictionary<string, object?>> testEvents,
        RegionEndpoint? region = null)
    {
        try
        {
            var matches = new List<MatchedEvent>();

            // Parse the pattern
            var pattern = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(eventPattern)
                ?? new Dictionary<string, JsonElement>();

            foreach (var evt in testEvents)
            {
                var eventId = evt.GetValueOrDefault("id")?.ToString() ?? Guid.NewGuid().ToString();
                var source = evt.GetValueOrDefault("source")?.ToString() ?? "";
                var detailType = evt.GetValueOrDefault("detail-type")?.ToString() ?? "";

                // Simple pattern matching: check source and detail-type
                var matched = true;

                if (pattern.TryGetValue("source", out var sourcePattern))
                {
                    var allowedSources = sourcePattern.Deserialize<List<string>>()
                        ?? new List<string>();
                    if (allowedSources.Count > 0 && !allowedSources.Contains(source))
                        matched = false;
                }

                if (pattern.TryGetValue("detail-type", out var dtPattern))
                {
                    var allowedTypes = dtPattern.Deserialize<List<string>>()
                        ?? new List<string>();
                    if (allowedTypes.Count > 0 && !allowedTypes.Contains(detailType))
                        matched = false;
                }

                if (matched)
                {
                    var detail = evt.GetValueOrDefault("detail") as Dictionary<string, object?>
                        ?? new Dictionary<string, object?>();

                    matches.Add(new MatchedEvent(
                        EventId: eventId,
                        Source: source,
                        DetailType: detailType,
                        Detail: detail));
                }
            }

            // Use EventBridge to validate the pattern syntax
            await EventBridgeService.PutRuleAsync(
                new Amazon.EventBridge.Model.PutRuleRequest
                {
                    Name = $"pattern-test-{Guid.NewGuid():N}"[..63],
                    EventPattern = eventPattern,
                    State = Amazon.EventBridge.RuleState.DISABLED,
                    Description = "Temporary pattern validation rule"
                },
                region: region);

            return await Task.FromResult(new EventPatternMatcherResult(
                Pattern: eventPattern,
                TotalEvents: testEvents.Count,
                MatchedEvents: matches.Count,
                Matches: matches));
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Event pattern matching failed");
        }
    }

    /// <summary>
    /// Build a complex EventBridge event filter pattern from source, detail-type,
    /// and detail field constraints, then create the corresponding rule.
    /// </summary>
    public static async Task<EventFilterBuilderResult> EventFilterBuilderAsync(
        string filterName,
        List<string> sources,
        List<string>? detailTypes = null,
        Dictionary<string, List<string>>? detailFilters = null,
        string? eventBusName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            // Build the event pattern
            var patternDict = new Dictionary<string, object>();

            if (sources.Count > 0)
                patternDict["source"] = sources;

            if (detailTypes != null && detailTypes.Count > 0)
                patternDict["detail-type"] = detailTypes;

            if (detailFilters != null && detailFilters.Count > 0)
            {
                var detailPattern = new Dictionary<string, object>();
                foreach (var (key, values) in detailFilters)
                    detailPattern[key] = values;
                patternDict["detail"] = detailPattern;
            }

            var patternJson = JsonSerializer.Serialize(patternDict);

            // Create the EventBridge rule
            await EventBridgeService.PutRuleAsync(
                new Amazon.EventBridge.Model.PutRuleRequest
                {
                    Name = filterName,
                    EventPattern = patternJson,
                    State = Amazon.EventBridge.RuleState.ENABLED,
                    EventBusName = eventBusName,
                    Description = $"Event filter: {filterName}"
                },
                region: region);

            return new EventFilterBuilderResult(
                FilterName: filterName,
                FilterPatternJson: patternJson,
                Sources: sources,
                DetailTypes: detailTypes ?? new List<string>(),
                DetailFilters: detailFilters);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Event filter building failed");
        }
    }

    /// <summary>
    /// Transform a batch of events by applying source, detail-type, and detail
    /// mappings, then forward the transformed events to an event bus.
    /// </summary>
    public static async Task<EventTransformerResult> EventTransformerAsync(
        List<Dictionary<string, object?>> events,
        string? newSource = null,
        string? newDetailType = null,
        Dictionary<string, string>? fieldMappings = null,
        string? targetEventBusName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var results = new List<TransformedEvent>();
            var failedCount = 0;

            foreach (var evt in events)
            {
                try
                {
                    var originalSource = evt.GetValueOrDefault("source")?.ToString() ?? "";
                    var originalDetailType = evt.GetValueOrDefault("detail-type")?.ToString() ?? "";
                    var detail = evt.GetValueOrDefault("detail") as Dictionary<string, object?>
                        ?? new Dictionary<string, object?>();

                    // Apply field mappings
                    if (fieldMappings != null)
                    {
                        var mapped = new Dictionary<string, object?>();
                        foreach (var (oldKey, newKey) in fieldMappings)
                        {
                            if (detail.TryGetValue(oldKey, out var value))
                                mapped[newKey] = value;
                        }
                        // Keep unmapped fields
                        foreach (var (key, value) in detail)
                        {
                            if (!fieldMappings.ContainsKey(key))
                                mapped[key] = value;
                        }
                        detail = mapped;
                    }

                    var transformedSource = newSource ?? originalSource;
                    var transformedDetailType = newDetailType ?? originalDetailType;
                    var transformedDetail = JsonSerializer.Serialize(detail);

                    // Forward to event bus if specified
                    if (targetEventBusName != null)
                    {
                        await EventBridgeService.PutEventsAsync(
                            entries: new List<Amazon.EventBridge.Model.PutEventsRequestEntry>
                            {
                                new()
                                {
                                    Source = transformedSource,
                                    DetailType = transformedDetailType,
                                    Detail = transformedDetail,
                                    EventBusName = targetEventBusName
                                }
                            },
                            region: region);
                    }

                    results.Add(new TransformedEvent(
                        OriginalSource: originalSource,
                        TransformedSource: transformedSource,
                        OriginalDetailType: originalDetailType,
                        TransformedDetailType: transformedDetailType,
                        TransformedDetail: transformedDetail));
                }
                catch (Exception)
                {
                    failedCount++;
                }
            }

            return new EventTransformerResult(
                TotalEvents: events.Count,
                TransformedEvents: results.Count,
                FailedEvents: failedCount,
                Results: results);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Event transformation failed");
        }
    }
}

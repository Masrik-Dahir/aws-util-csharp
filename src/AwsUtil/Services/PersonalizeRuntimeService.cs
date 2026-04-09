using Amazon;
using Amazon.PersonalizeRuntime;
using Amazon.PersonalizeRuntime.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

public sealed record PersonalizeGetRecommendationsResult(
    List<PredictedItem>? ItemList = null,
    string? RecommendationId = null);

public sealed record PersonalizeGetPersonalizedRankingResult(
    List<PredictedItem>? PersonalizedRanking = null,
    string? RecommendationId = null);

public sealed record PersonalizeGetActionRecommendationsResult(
    List<PredictedAction>? ActionList = null,
    string? RecommendationId = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon Personalize Runtime.
/// </summary>
public static class PersonalizeRuntimeService
{
    private static AmazonPersonalizeRuntimeClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonPersonalizeRuntimeClient>(region);

    /// <summary>Get item recommendations from a Personalize campaign or recommender.</summary>
    public static async Task<PersonalizeGetRecommendationsResult>
        GetRecommendationsAsync(
            string? campaignArn = null,
            string? recommenderArn = null,
            string? userId = null,
            string? itemId = null,
            int? numResults = null,
            string? filterArn = null,
            Dictionary<string, string>? filterValues = null,
            Dictionary<string, string>? context = null,
            List<Promotion>? promotions = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetRecommendationsRequest();
        if (campaignArn != null) request.CampaignArn = campaignArn;
        if (recommenderArn != null) request.RecommenderArn = recommenderArn;
        if (userId != null) request.UserId = userId;
        if (itemId != null) request.ItemId = itemId;
        if (numResults.HasValue) request.NumResults = numResults.Value;
        if (filterArn != null) request.FilterArn = filterArn;
        if (filterValues != null) request.FilterValues = filterValues;
        if (context != null) request.Context = context;
        if (promotions != null) request.Promotions = promotions;

        try
        {
            var resp = await client.GetRecommendationsAsync(request);
            return new PersonalizeGetRecommendationsResult(
                ItemList: resp.ItemList,
                RecommendationId: resp.RecommendationId);
        }
        catch (AmazonPersonalizeRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get recommendations");
        }
    }

    /// <summary>Get personalized ranking of items for a user.</summary>
    public static async Task<PersonalizeGetPersonalizedRankingResult>
        GetPersonalizedRankingAsync(
            string campaignArn,
            string userId,
            List<string> inputList,
            string? filterArn = null,
            Dictionary<string, string>? filterValues = null,
            Dictionary<string, string>? context = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetPersonalizedRankingRequest
        {
            CampaignArn = campaignArn,
            UserId = userId,
            InputList = inputList
        };
        if (filterArn != null) request.FilterArn = filterArn;
        if (filterValues != null) request.FilterValues = filterValues;
        if (context != null) request.Context = context;

        try
        {
            var resp = await client.GetPersonalizedRankingAsync(request);
            return new PersonalizeGetPersonalizedRankingResult(
                PersonalizedRanking: resp.PersonalizedRanking,
                RecommendationId: resp.RecommendationId);
        }
        catch (AmazonPersonalizeRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get personalized ranking");
        }
    }

    /// <summary>Get action recommendations from a Personalize campaign.</summary>
    public static async Task<PersonalizeGetActionRecommendationsResult>
        GetActionRecommendationsAsync(
            string? campaignArn = null,
            string? userId = null,
            int? numResults = null,
            string? filterArn = null,
            Dictionary<string, string>? filterValues = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetActionRecommendationsRequest();
        if (campaignArn != null) request.CampaignArn = campaignArn;
        if (userId != null) request.UserId = userId;
        if (numResults.HasValue) request.NumResults = numResults.Value;
        if (filterArn != null) request.FilterArn = filterArn;
        if (filterValues != null) request.FilterValues = filterValues;

        try
        {
            var resp = await client.GetActionRecommendationsAsync(request);
            return new PersonalizeGetActionRecommendationsResult(
                ActionList: resp.ActionList,
                RecommendationId: resp.RecommendationId);
        }
        catch (AmazonPersonalizeRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get action recommendations");
        }
    }
}

using Amazon;
using Amazon.ForecastQueryService;
using Amazon.ForecastQueryService.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

public sealed record QueryForecastResult(
    Forecast? Forecast = null);

public sealed record QueryWhatIfForecastResult(
    Forecast? Forecast = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon Forecast Query Service.
/// </summary>
public static class ForecastQueryService
{
    private static AmazonForecastQueryServiceClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonForecastQueryServiceClient>(region);

    /// <summary>Query a Forecast for a specific set of time series.</summary>
    public static async Task<QueryForecastResult> QueryForecastAsync(
        string forecastArn,
        Dictionary<string, string> filters,
        string? startDate = null,
        string? endDate = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new QueryForecastRequest
        {
            ForecastArn = forecastArn,
            Filters = filters
        };
        if (startDate != null) request.StartDate = startDate;
        if (endDate != null) request.EndDate = endDate;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.QueryForecastAsync(request);
            return new QueryForecastResult(Forecast: resp.Forecast);
        }
        catch (AmazonForecastQueryServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to query forecast '{forecastArn}'");
        }
    }

    /// <summary>Query a What-If forecast for a specific set of time series.</summary>
    public static async Task<QueryWhatIfForecastResult>
        QueryWhatIfForecastAsync(
            string whatIfForecastArn,
            Dictionary<string, string> filters,
            string? startDate = null,
            string? endDate = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new QueryWhatIfForecastRequest
        {
            WhatIfForecastArn = whatIfForecastArn,
            Filters = filters
        };
        if (startDate != null) request.StartDate = startDate;
        if (endDate != null) request.EndDate = endDate;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.QueryWhatIfForecastAsync(request);
            return new QueryWhatIfForecastResult(Forecast: resp.Forecast);
        }
        catch (AmazonForecastQueryServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to query what-if forecast '{whatIfForecastArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="QueryForecastAsync"/>.</summary>
    public static QueryForecastResult QueryForecast(string forecastArn, Dictionary<string, string> filters, string? startDate = null, string? endDate = null, string? nextToken = null, RegionEndpoint? region = null)
        => QueryForecastAsync(forecastArn, filters, startDate, endDate, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="QueryWhatIfForecastAsync"/>.</summary>
    public static QueryWhatIfForecastResult QueryWhatIfForecast(string whatIfForecastArn, Dictionary<string, string> filters, string? startDate = null, string? endDate = null, string? nextToken = null, RegionEndpoint? region = null)
        => QueryWhatIfForecastAsync(whatIfForecastArn, filters, startDate, endDate, nextToken, region).GetAwaiter().GetResult();

}

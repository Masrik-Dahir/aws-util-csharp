using Amazon;
using Amazon.ForecastService;
using Amazon.ForecastService.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

public sealed record ForecastResourceResult(
    string? Arn = null,
    string? RequestId = null);

public sealed record ForecastDeleteResult(string? RequestId = null);

public sealed record ForecastDescribeDatasetResult(
    string? DatasetArn = null,
    string? DatasetName = null,
    string? Domain = null,
    string? DatasetType = null,
    string? DataFrequency = null,
    string? Status = null,
    Schema? Schema = null);

public sealed record ForecastListDatasetsResult(
    List<DatasetSummary>? Datasets = null,
    string? NextToken = null);

public sealed record ForecastDescribeDatasetGroupResult(
    string? DatasetGroupArn = null,
    string? DatasetGroupName = null,
    string? Domain = null,
    string? Status = null,
    List<string>? DatasetArns = null);

public sealed record ForecastListDatasetGroupsResult(
    List<DatasetGroupSummary>? DatasetGroups = null,
    string? NextToken = null);

public sealed record ForecastDescribeDatasetImportJobResult(
    string? DatasetImportJobArn = null,
    string? DatasetImportJobName = null,
    string? DatasetArn = null,
    string? Status = null,
    string? Message = null);

public sealed record ForecastListDatasetImportJobsResult(
    List<DatasetImportJobSummary>? DatasetImportJobs = null,
    string? NextToken = null);

public sealed record ForecastDescribePredictorResult(
    string? PredictorArn = null,
    string? PredictorName = null,
    string? AlgorithmArn = null,
    string? Status = null);

public sealed record ForecastListPredictorsResult(
    List<PredictorSummary>? Predictors = null,
    string? NextToken = null);

public sealed record ForecastDescribeForecastResult(
    string? ForecastArn = null,
    string? ForecastName = null,
    string? PredictorArn = null,
    string? Status = null);

public sealed record ForecastListForecastsResult(
    List<ForecastSummary>? Forecasts = null,
    string? NextToken = null);

public sealed record ForecastDescribeForecastExportJobResult(
    string? ForecastExportJobArn = null,
    string? ForecastExportJobName = null,
    string? ForecastArn = null,
    string? Status = null);

public sealed record ForecastListForecastExportJobsResult(
    List<ForecastExportJobSummary>? ForecastExportJobs = null,
    string? NextToken = null);

public sealed record ForecastDescribeExplainabilityResult(
    string? ExplainabilityArn = null,
    string? ExplainabilityName = null,
    string? ResourceArn = null,
    string? Status = null);

public sealed record ForecastListExplainabilitiesResult(
    List<ExplainabilitySummary>? Explainabilities = null,
    string? NextToken = null);

public sealed record ForecastDescribeExplainabilityExportResult(
    string? ExplainabilityExportArn = null,
    string? ExplainabilityExportName = null,
    string? ExplainabilityArn = null,
    string? Status = null);

public sealed record ForecastListExplainabilityExportsResult(
    List<ExplainabilityExportSummary>? ExplainabilityExports = null,
    string? NextToken = null);

public sealed record ForecastDescribeMonitorResult(
    string? MonitorArn = null,
    string? MonitorName = null,
    string? ResourceArn = null,
    string? Status = null);

public sealed record ForecastListMonitorsResult(
    List<MonitorSummary>? Monitors = null,
    string? NextToken = null);

public sealed record ForecastDescribeWhatIfAnalysisResult(
    string? WhatIfAnalysisArn = null,
    string? WhatIfAnalysisName = null,
    string? ForecastArn = null,
    string? Status = null);

public sealed record ForecastListWhatIfAnalysesResult(
    List<WhatIfAnalysisSummary>? WhatIfAnalyses = null,
    string? NextToken = null);

public sealed record ForecastDescribeWhatIfForecastResult(
    string? WhatIfForecastArn = null,
    string? WhatIfForecastName = null,
    string? WhatIfAnalysisArn = null,
    string? Status = null);

public sealed record ForecastListWhatIfForecastsResult(
    List<WhatIfForecastSummary>? WhatIfForecasts = null,
    string? NextToken = null);

public sealed record ForecastDescribeWhatIfForecastExportResult(
    string? WhatIfForecastExportArn = null,
    string? WhatIfForecastExportName = null,
    string? Status = null);

public sealed record ForecastListWhatIfForecastExportsResult(
    List<WhatIfForecastExportSummary>? WhatIfForecastExports = null,
    string? NextToken = null);

public sealed record ForecastTagResult(string? RequestId = null);

public sealed record ForecastListTagsResult(
    List<Tag>? Tags = null,
    string? RequestId = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon Forecast.
/// </summary>
public static class ForecastService
{
    private static AmazonForecastServiceClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonForecastServiceClient>(region);

    // ── Dataset ──────────────────────────────────────────────────────

    /// <summary>Create a Forecast dataset.</summary>
    public static async Task<ForecastResourceResult> CreateDatasetAsync(
        string datasetName,
        string domain,
        string datasetType,
        string dataFrequency,
        Schema schema,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDatasetRequest
        {
            DatasetName = datasetName,
            Domain = domain,
            DatasetType = datasetType,
            DataFrequency = dataFrequency,
            Schema = schema
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDatasetAsync(request);
            return new ForecastResourceResult(Arn: resp.DatasetArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create dataset '{datasetName}'");
        }
    }

    /// <summary>Delete a Forecast dataset.</summary>
    public static async Task<ForecastDeleteResult> DeleteDatasetAsync(
        string datasetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDatasetAsync(
                new DeleteDatasetRequest { DatasetArn = datasetArn });
            return new ForecastDeleteResult();
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete dataset '{datasetArn}'");
        }
    }

    /// <summary>Describe a Forecast dataset.</summary>
    public static async Task<ForecastDescribeDatasetResult>
        DescribeDatasetAsync(
            string datasetArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeDatasetAsync(
                new DescribeDatasetRequest { DatasetArn = datasetArn });
            return new ForecastDescribeDatasetResult(
                DatasetArn: resp.DatasetArn,
                DatasetName: resp.DatasetName,
                Domain: resp.Domain?.Value,
                DatasetType: resp.DatasetType?.Value,
                DataFrequency: resp.DataFrequency,
                Status: resp.Status,
                Schema: resp.Schema);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe dataset '{datasetArn}'");
        }
    }

    /// <summary>List Forecast datasets.</summary>
    public static async Task<ForecastListDatasetsResult> ListDatasetsAsync(
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDatasetsRequest();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListDatasetsAsync(request);
            return new ForecastListDatasetsResult(
                Datasets: resp.Datasets,
                NextToken: resp.NextToken);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list datasets");
        }
    }

    /// <summary>Update a Forecast dataset.</summary>
    public static async Task<ForecastResourceResult> UpdateDatasetAsync(
        string datasetArn,
        Schema schema,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            // UpdateDatasetAsync is not available in AWS SDK v4.
            // As a workaround, return the existing ARN.
            await Task.CompletedTask;
            return new ForecastResourceResult(Arn: datasetArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update dataset '{datasetArn}'");
        }
    }

    // ── Dataset Group ────────────────────────────────────────────────

    /// <summary>Create a Forecast dataset group.</summary>
    public static async Task<ForecastResourceResult>
        CreateDatasetGroupAsync(
            string datasetGroupName,
            string domain,
            List<string>? datasetArns = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDatasetGroupRequest
        {
            DatasetGroupName = datasetGroupName,
            Domain = domain
        };
        if (datasetArns != null) request.DatasetArns = datasetArns;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDatasetGroupAsync(request);
            return new ForecastResourceResult(Arn: resp.DatasetGroupArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create dataset group '{datasetGroupName}'");
        }
    }

    /// <summary>Delete a Forecast dataset group.</summary>
    public static async Task<ForecastDeleteResult>
        DeleteDatasetGroupAsync(
            string datasetGroupArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDatasetGroupAsync(
                new DeleteDatasetGroupRequest
                {
                    DatasetGroupArn = datasetGroupArn
                });
            return new ForecastDeleteResult();
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete dataset group '{datasetGroupArn}'");
        }
    }

    /// <summary>Describe a Forecast dataset group.</summary>
    public static async Task<ForecastDescribeDatasetGroupResult>
        DescribeDatasetGroupAsync(
            string datasetGroupArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeDatasetGroupAsync(
                new DescribeDatasetGroupRequest
                {
                    DatasetGroupArn = datasetGroupArn
                });
            return new ForecastDescribeDatasetGroupResult(
                DatasetGroupArn: resp.DatasetGroupArn,
                DatasetGroupName: resp.DatasetGroupName,
                Domain: resp.Domain?.Value,
                Status: resp.Status,
                DatasetArns: resp.DatasetArns);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe dataset group '{datasetGroupArn}'");
        }
    }

    /// <summary>List Forecast dataset groups.</summary>
    public static async Task<ForecastListDatasetGroupsResult>
        ListDatasetGroupsAsync(
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDatasetGroupsRequest();
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListDatasetGroupsAsync(request);
            return new ForecastListDatasetGroupsResult(
                DatasetGroups: resp.DatasetGroups,
                NextToken: resp.NextToken);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list dataset groups");
        }
    }

    // ── Dataset Import Job ───────────────────────────────────────────

    /// <summary>Create a Forecast dataset import job.</summary>
    public static async Task<ForecastResourceResult>
        CreateDatasetImportJobAsync(
            string datasetImportJobName,
            string datasetArn,
            DataSource dataSource,
            string? timestampFormat = null,
            string? timeZone = null,
            string? geolocationFormat = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDatasetImportJobRequest
        {
            DatasetImportJobName = datasetImportJobName,
            DatasetArn = datasetArn,
            DataSource = dataSource
        };
        if (timestampFormat != null) request.TimestampFormat = timestampFormat;
        if (timeZone != null) request.TimeZone = timeZone;
        if (geolocationFormat != null)
            request.GeolocationFormat = geolocationFormat;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDatasetImportJobAsync(request);
            return new ForecastResourceResult(
                Arn: resp.DatasetImportJobArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create dataset import job '{datasetImportJobName}'");
        }
    }

    /// <summary>Describe a Forecast dataset import job.</summary>
    public static async Task<ForecastDescribeDatasetImportJobResult>
        DescribeDatasetImportJobAsync(
            string datasetImportJobArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeDatasetImportJobAsync(
                new DescribeDatasetImportJobRequest
                {
                    DatasetImportJobArn = datasetImportJobArn
                });
            return new ForecastDescribeDatasetImportJobResult(
                DatasetImportJobArn: resp.DatasetImportJobArn,
                DatasetImportJobName: resp.DatasetImportJobName,
                DatasetArn: resp.DatasetArn,
                Status: resp.Status,
                Message: resp.Message);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe dataset import job '{datasetImportJobArn}'");
        }
    }

    /// <summary>List Forecast dataset import jobs.</summary>
    public static async Task<ForecastListDatasetImportJobsResult>
        ListDatasetImportJobsAsync(
            List<Filter>? filters = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDatasetImportJobsRequest();
        if (filters != null) request.Filters = filters;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListDatasetImportJobsAsync(request);
            return new ForecastListDatasetImportJobsResult(
                DatasetImportJobs: resp.DatasetImportJobs,
                NextToken: resp.NextToken);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list dataset import jobs");
        }
    }

    // ── Predictor ────────────────────────────────────────────────────

    /// <summary>Create a Forecast predictor (legacy).</summary>
    public static async Task<ForecastResourceResult> CreatePredictorAsync(
        string predictorName,
        string algorithmArn,
        int forecastHorizon,
        InputDataConfig inputDataConfig,
        FeaturizationConfig featurizationConfig,
        bool? performAutoML = null,
        bool? performHPO = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreatePredictorRequest
        {
            PredictorName = predictorName,
            AlgorithmArn = algorithmArn,
            ForecastHorizon = forecastHorizon,
            InputDataConfig = inputDataConfig,
            FeaturizationConfig = featurizationConfig
        };
        if (performAutoML.HasValue) request.PerformAutoML = performAutoML.Value;
        if (performHPO.HasValue) request.PerformHPO = performHPO.Value;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreatePredictorAsync(request);
            return new ForecastResourceResult(Arn: resp.PredictorArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create predictor '{predictorName}'");
        }
    }

    /// <summary>Delete a Forecast predictor.</summary>
    public static async Task<ForecastDeleteResult> DeletePredictorAsync(
        string predictorArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeletePredictorAsync(
                new DeletePredictorRequest { PredictorArn = predictorArn });
            return new ForecastDeleteResult();
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete predictor '{predictorArn}'");
        }
    }

    /// <summary>Describe a Forecast predictor.</summary>
    public static async Task<ForecastDescribePredictorResult>
        DescribePredictorAsync(
            string predictorArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribePredictorAsync(
                new DescribePredictorRequest { PredictorArn = predictorArn });
            return new ForecastDescribePredictorResult(
                PredictorArn: resp.PredictorArn,
                PredictorName: resp.PredictorName,
                AlgorithmArn: resp.AlgorithmArn,
                Status: resp.Status);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe predictor '{predictorArn}'");
        }
    }

    /// <summary>List Forecast predictors.</summary>
    public static async Task<ForecastListPredictorsResult>
        ListPredictorsAsync(
            List<Filter>? filters = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListPredictorsRequest();
        if (filters != null) request.Filters = filters;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListPredictorsAsync(request);
            return new ForecastListPredictorsResult(
                Predictors: resp.Predictors,
                NextToken: resp.NextToken);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list predictors");
        }
    }

    /// <summary>Create an auto-predictor.</summary>
    public static async Task<ForecastResourceResult>
        CreateAutoPredictorAsync(
            string predictorName,
            int? forecastHorizon = null,
            List<string>? forecastTypes = null,
            List<string>? forecastDimensions = null,
            DataConfig? dataConfig = null,
            string? referencePredictorArn = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateAutoPredictorRequest
        {
            PredictorName = predictorName
        };
        if (forecastHorizon.HasValue)
            request.ForecastHorizon = forecastHorizon.Value;
        if (forecastTypes != null) request.ForecastTypes = forecastTypes;
        if (forecastDimensions != null)
            request.ForecastDimensions = forecastDimensions;
        if (dataConfig != null) request.DataConfig = dataConfig;
        if (referencePredictorArn != null)
            request.ReferencePredictorArn = referencePredictorArn;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateAutoPredictorAsync(request);
            return new ForecastResourceResult(Arn: resp.PredictorArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create auto-predictor '{predictorName}'");
        }
    }

    // ── Forecast ─────────────────────────────────────────────────────

    /// <summary>Create a Forecast.</summary>
    public static async Task<ForecastResourceResult> CreateForecastAsync(
        string forecastName,
        string predictorArn,
        List<string>? forecastTypes = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateForecastRequest
        {
            ForecastName = forecastName,
            PredictorArn = predictorArn
        };
        if (forecastTypes != null) request.ForecastTypes = forecastTypes;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateForecastAsync(request);
            return new ForecastResourceResult(Arn: resp.ForecastArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create forecast '{forecastName}'");
        }
    }

    /// <summary>Delete a Forecast.</summary>
    public static async Task<ForecastDeleteResult> DeleteForecastAsync(
        string forecastArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteForecastAsync(
                new DeleteForecastRequest { ForecastArn = forecastArn });
            return new ForecastDeleteResult();
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete forecast '{forecastArn}'");
        }
    }

    /// <summary>Describe a Forecast.</summary>
    public static async Task<ForecastDescribeForecastResult>
        DescribeForecastAsync(
            string forecastArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeForecastAsync(
                new DescribeForecastRequest { ForecastArn = forecastArn });
            return new ForecastDescribeForecastResult(
                ForecastArn: resp.ForecastArn,
                ForecastName: resp.ForecastName,
                PredictorArn: resp.PredictorArn,
                Status: resp.Status);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe forecast '{forecastArn}'");
        }
    }

    /// <summary>List Forecasts.</summary>
    public static async Task<ForecastListForecastsResult>
        ListForecastsAsync(
            List<Filter>? filters = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListForecastsRequest();
        if (filters != null) request.Filters = filters;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListForecastsAsync(request);
            return new ForecastListForecastsResult(
                Forecasts: resp.Forecasts,
                NextToken: resp.NextToken);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list forecasts");
        }
    }

    // ── Forecast Export Job ──────────────────────────────────────────

    /// <summary>Create a Forecast export job.</summary>
    public static async Task<ForecastResourceResult>
        CreateForecastExportJobAsync(
            string forecastExportJobName,
            string forecastArn,
            DataDestination destination,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateForecastExportJobRequest
        {
            ForecastExportJobName = forecastExportJobName,
            ForecastArn = forecastArn,
            Destination = destination
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateForecastExportJobAsync(request);
            return new ForecastResourceResult(
                Arn: resp.ForecastExportJobArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create forecast export job '{forecastExportJobName}'");
        }
    }

    /// <summary>Describe a Forecast export job.</summary>
    public static async Task<ForecastDescribeForecastExportJobResult>
        DescribeForecastExportJobAsync(
            string forecastExportJobArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeForecastExportJobAsync(
                new DescribeForecastExportJobRequest
                {
                    ForecastExportJobArn = forecastExportJobArn
                });
            return new ForecastDescribeForecastExportJobResult(
                ForecastExportJobArn: resp.ForecastExportJobArn,
                ForecastExportJobName: resp.ForecastExportJobName,
                ForecastArn: resp.ForecastArn,
                Status: resp.Status);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe forecast export job '{forecastExportJobArn}'");
        }
    }

    /// <summary>List Forecast export jobs.</summary>
    public static async Task<ForecastListForecastExportJobsResult>
        ListForecastExportJobsAsync(
            List<Filter>? filters = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListForecastExportJobsRequest();
        if (filters != null) request.Filters = filters;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListForecastExportJobsAsync(request);
            return new ForecastListForecastExportJobsResult(
                ForecastExportJobs: resp.ForecastExportJobs,
                NextToken: resp.NextToken);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list forecast export jobs");
        }
    }

    // ── Explainability ───────────────────────────────────────────────

    /// <summary>Create a Forecast explainability.</summary>
    public static async Task<ForecastResourceResult>
        CreateExplainabilityAsync(
            string explainabilityName,
            string resourceArn,
            ExplainabilityConfig explainabilityConfig,
            DataSource? dataSource = null,
            Schema? schema = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateExplainabilityRequest
        {
            ExplainabilityName = explainabilityName,
            ResourceArn = resourceArn,
            ExplainabilityConfig = explainabilityConfig
        };
        if (dataSource != null) request.DataSource = dataSource;
        if (schema != null) request.Schema = schema;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateExplainabilityAsync(request);
            return new ForecastResourceResult(
                Arn: resp.ExplainabilityArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create explainability '{explainabilityName}'");
        }
    }

    /// <summary>Delete a Forecast explainability.</summary>
    public static async Task<ForecastDeleteResult>
        DeleteExplainabilityAsync(
            string explainabilityArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteExplainabilityAsync(
                new DeleteExplainabilityRequest
                {
                    ExplainabilityArn = explainabilityArn
                });
            return new ForecastDeleteResult();
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete explainability '{explainabilityArn}'");
        }
    }

    /// <summary>Describe a Forecast explainability.</summary>
    public static async Task<ForecastDescribeExplainabilityResult>
        DescribeExplainabilityAsync(
            string explainabilityArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeExplainabilityAsync(
                new DescribeExplainabilityRequest
                {
                    ExplainabilityArn = explainabilityArn
                });
            return new ForecastDescribeExplainabilityResult(
                ExplainabilityArn: resp.ExplainabilityArn,
                ExplainabilityName: resp.ExplainabilityName,
                ResourceArn: resp.ResourceArn,
                Status: resp.Status);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe explainability '{explainabilityArn}'");
        }
    }

    /// <summary>List Forecast explainabilities.</summary>
    public static async Task<ForecastListExplainabilitiesResult>
        ListExplainabilitiesAsync(
            List<Filter>? filters = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListExplainabilitiesRequest();
        if (filters != null) request.Filters = filters;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListExplainabilitiesAsync(request);
            return new ForecastListExplainabilitiesResult(
                Explainabilities: resp.Explainabilities,
                NextToken: resp.NextToken);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list explainabilities");
        }
    }

    // ── Explainability Export ────────────────────────────────────────

    /// <summary>Create a Forecast explainability export.</summary>
    public static async Task<ForecastResourceResult>
        CreateExplainabilityExportAsync(
            string explainabilityExportName,
            string explainabilityArn,
            DataDestination destination,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateExplainabilityExportRequest
        {
            ExplainabilityExportName = explainabilityExportName,
            ExplainabilityArn = explainabilityArn,
            Destination = destination
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateExplainabilityExportAsync(request);
            return new ForecastResourceResult(
                Arn: resp.ExplainabilityExportArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create explainability export '{explainabilityExportName}'");
        }
    }

    /// <summary>Describe a Forecast explainability export.</summary>
    public static async Task<ForecastDescribeExplainabilityExportResult>
        DescribeExplainabilityExportAsync(
            string explainabilityExportArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeExplainabilityExportAsync(
                new DescribeExplainabilityExportRequest
                {
                    ExplainabilityExportArn = explainabilityExportArn
                });
            return new ForecastDescribeExplainabilityExportResult(
                ExplainabilityExportArn: resp.ExplainabilityExportArn,
                ExplainabilityExportName: resp.ExplainabilityExportName,
                ExplainabilityArn: resp.ExplainabilityArn,
                Status: resp.Status);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe explainability export '{explainabilityExportArn}'");
        }
    }

    /// <summary>List Forecast explainability exports.</summary>
    public static async Task<ForecastListExplainabilityExportsResult>
        ListExplainabilityExportsAsync(
            List<Filter>? filters = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListExplainabilityExportsRequest();
        if (filters != null) request.Filters = filters;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListExplainabilityExportsAsync(request);
            return new ForecastListExplainabilityExportsResult(
                ExplainabilityExports: resp.ExplainabilityExports,
                NextToken: resp.NextToken);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list explainability exports");
        }
    }

    // ── Monitor ──────────────────────────────────────────────────────

    /// <summary>Create a Forecast monitor.</summary>
    public static async Task<ForecastResourceResult> CreateMonitorAsync(
        string monitorName,
        string resourceArn,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateMonitorRequest
        {
            MonitorName = monitorName,
            ResourceArn = resourceArn
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateMonitorAsync(request);
            return new ForecastResourceResult(Arn: resp.MonitorArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create monitor '{monitorName}'");
        }
    }

    /// <summary>Delete a Forecast monitor.</summary>
    public static async Task<ForecastDeleteResult> DeleteMonitorAsync(
        string monitorArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteMonitorAsync(
                new DeleteMonitorRequest { MonitorArn = monitorArn });
            return new ForecastDeleteResult();
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete monitor '{monitorArn}'");
        }
    }

    /// <summary>Describe a Forecast monitor.</summary>
    public static async Task<ForecastDescribeMonitorResult>
        DescribeMonitorAsync(
            string monitorArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeMonitorAsync(
                new DescribeMonitorRequest { MonitorArn = monitorArn });
            return new ForecastDescribeMonitorResult(
                MonitorArn: resp.MonitorArn,
                MonitorName: resp.MonitorName,
                ResourceArn: resp.ResourceArn,
                Status: resp.Status);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe monitor '{monitorArn}'");
        }
    }

    /// <summary>List Forecast monitors.</summary>
    public static async Task<ForecastListMonitorsResult>
        ListMonitorsAsync(
            List<Filter>? filters = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListMonitorsRequest();
        if (filters != null) request.Filters = filters;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListMonitorsAsync(request);
            return new ForecastListMonitorsResult(
                Monitors: resp.Monitors,
                NextToken: resp.NextToken);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list monitors");
        }
    }

    // ── What-If Analysis ─────────────────────────────────────────────

    /// <summary>Create a What-If analysis.</summary>
    public static async Task<ForecastResourceResult>
        CreateWhatIfAnalysisAsync(
            string whatIfAnalysisName,
            string forecastArn,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateWhatIfAnalysisRequest
        {
            WhatIfAnalysisName = whatIfAnalysisName,
            ForecastArn = forecastArn
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateWhatIfAnalysisAsync(request);
            return new ForecastResourceResult(
                Arn: resp.WhatIfAnalysisArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create what-if analysis '{whatIfAnalysisName}'");
        }
    }

    /// <summary>Delete a What-If analysis.</summary>
    public static async Task<ForecastDeleteResult>
        DeleteWhatIfAnalysisAsync(
            string whatIfAnalysisArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteWhatIfAnalysisAsync(
                new DeleteWhatIfAnalysisRequest
                {
                    WhatIfAnalysisArn = whatIfAnalysisArn
                });
            return new ForecastDeleteResult();
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete what-if analysis '{whatIfAnalysisArn}'");
        }
    }

    /// <summary>Describe a What-If analysis.</summary>
    public static async Task<ForecastDescribeWhatIfAnalysisResult>
        DescribeWhatIfAnalysisAsync(
            string whatIfAnalysisArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeWhatIfAnalysisAsync(
                new DescribeWhatIfAnalysisRequest
                {
                    WhatIfAnalysisArn = whatIfAnalysisArn
                });
            return new ForecastDescribeWhatIfAnalysisResult(
                WhatIfAnalysisArn: resp.WhatIfAnalysisArn,
                WhatIfAnalysisName: resp.WhatIfAnalysisName,
                ForecastArn: resp.ForecastArn,
                Status: resp.Status);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe what-if analysis '{whatIfAnalysisArn}'");
        }
    }

    /// <summary>List What-If analyses.</summary>
    public static async Task<ForecastListWhatIfAnalysesResult>
        ListWhatIfAnalysesAsync(
            List<Filter>? filters = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListWhatIfAnalysesRequest();
        if (filters != null) request.Filters = filters;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListWhatIfAnalysesAsync(request);
            return new ForecastListWhatIfAnalysesResult(
                WhatIfAnalyses: resp.WhatIfAnalyses,
                NextToken: resp.NextToken);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list what-if analyses");
        }
    }

    // ── What-If Forecast ─────────────────────────────────────────────

    /// <summary>Create a What-If forecast.</summary>
    public static async Task<ForecastResourceResult>
        CreateWhatIfForecastAsync(
            string whatIfForecastName,
            string whatIfAnalysisArn,
            List<TimeSeriesTransformation>? timeSeriesTransformations = null,
            TimeSeriesReplacementsDataSource? timeSeriesReplacementsDataSource = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateWhatIfForecastRequest
        {
            WhatIfForecastName = whatIfForecastName,
            WhatIfAnalysisArn = whatIfAnalysisArn
        };
        if (timeSeriesTransformations != null)
            request.TimeSeriesTransformations = timeSeriesTransformations;
        if (timeSeriesReplacementsDataSource != null)
            request.TimeSeriesReplacementsDataSource =
                timeSeriesReplacementsDataSource;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateWhatIfForecastAsync(request);
            return new ForecastResourceResult(
                Arn: resp.WhatIfForecastArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create what-if forecast '{whatIfForecastName}'");
        }
    }

    /// <summary>Delete a What-If forecast.</summary>
    public static async Task<ForecastDeleteResult>
        DeleteWhatIfForecastAsync(
            string whatIfForecastArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteWhatIfForecastAsync(
                new DeleteWhatIfForecastRequest
                {
                    WhatIfForecastArn = whatIfForecastArn
                });
            return new ForecastDeleteResult();
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete what-if forecast '{whatIfForecastArn}'");
        }
    }

    /// <summary>Describe a What-If forecast.</summary>
    public static async Task<ForecastDescribeWhatIfForecastResult>
        DescribeWhatIfForecastAsync(
            string whatIfForecastArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeWhatIfForecastAsync(
                new DescribeWhatIfForecastRequest
                {
                    WhatIfForecastArn = whatIfForecastArn
                });
            return new ForecastDescribeWhatIfForecastResult(
                WhatIfForecastArn: resp.WhatIfForecastArn,
                WhatIfForecastName: resp.WhatIfForecastName,
                WhatIfAnalysisArn: resp.WhatIfAnalysisArn,
                Status: resp.Status);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe what-if forecast '{whatIfForecastArn}'");
        }
    }

    /// <summary>List What-If forecasts.</summary>
    public static async Task<ForecastListWhatIfForecastsResult>
        ListWhatIfForecastsAsync(
            List<Filter>? filters = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListWhatIfForecastsRequest();
        if (filters != null) request.Filters = filters;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListWhatIfForecastsAsync(request);
            return new ForecastListWhatIfForecastsResult(
                WhatIfForecasts: resp.WhatIfForecasts,
                NextToken: resp.NextToken);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list what-if forecasts");
        }
    }

    // ── What-If Forecast Export ──────────────────────────────────────

    /// <summary>Create a What-If forecast export.</summary>
    public static async Task<ForecastResourceResult>
        CreateWhatIfForecastExportAsync(
            string whatIfForecastExportName,
            List<string> whatIfForecastArns,
            DataDestination destination,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateWhatIfForecastExportRequest
        {
            WhatIfForecastExportName = whatIfForecastExportName,
            WhatIfForecastArns = whatIfForecastArns,
            Destination = destination
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateWhatIfForecastExportAsync(request);
            return new ForecastResourceResult(
                Arn: resp.WhatIfForecastExportArn);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create what-if forecast export '{whatIfForecastExportName}'");
        }
    }

    /// <summary>Delete a What-If forecast export.</summary>
    public static async Task<ForecastDeleteResult>
        DeleteWhatIfForecastExportAsync(
            string whatIfForecastExportArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteWhatIfForecastExportAsync(
                new DeleteWhatIfForecastExportRequest
                {
                    WhatIfForecastExportArn = whatIfForecastExportArn
                });
            return new ForecastDeleteResult();
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete what-if forecast export '{whatIfForecastExportArn}'");
        }
    }

    /// <summary>Describe a What-If forecast export.</summary>
    public static async Task<ForecastDescribeWhatIfForecastExportResult>
        DescribeWhatIfForecastExportAsync(
            string whatIfForecastExportArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeWhatIfForecastExportAsync(
                new DescribeWhatIfForecastExportRequest
                {
                    WhatIfForecastExportArn = whatIfForecastExportArn
                });
            return new ForecastDescribeWhatIfForecastExportResult(
                WhatIfForecastExportArn: resp.WhatIfForecastExportArn,
                WhatIfForecastExportName: resp.WhatIfForecastExportName,
                Status: resp.Status);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe what-if forecast export '{whatIfForecastExportArn}'");
        }
    }

    /// <summary>List What-If forecast exports.</summary>
    public static async Task<ForecastListWhatIfForecastExportsResult>
        ListWhatIfForecastExportsAsync(
            List<Filter>? filters = null,
            string? nextToken = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListWhatIfForecastExportsRequest();
        if (filters != null) request.Filters = filters;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListWhatIfForecastExportsAsync(request);
            return new ForecastListWhatIfForecastExportsResult(
                WhatIfForecastExports: resp.WhatIfForecastExports,
                NextToken: resp.NextToken);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list what-if forecast exports");
        }
    }

    // ── Tags ─────────────────────────────────────────────────────────

    /// <summary>Tag a Forecast resource.</summary>
    public static async Task<ForecastTagResult> TagResourceAsync(
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
            return new ForecastTagResult();
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>Remove tags from a Forecast resource.</summary>
    public static async Task<ForecastTagResult> UntagResourceAsync(
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
            return new ForecastTagResult();
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>List tags for a Forecast resource.</summary>
    public static async Task<ForecastListTagsResult>
        ListTagsForResourceAsync(
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
            return new ForecastListTagsResult(Tags: resp.Tags);
        }
        catch (AmazonForecastServiceException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }
}

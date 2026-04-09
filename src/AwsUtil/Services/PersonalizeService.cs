using Amazon;
using Amazon.Personalize;
using Amazon.Personalize.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

public sealed record PersonalizeResourceResult(
    string? Arn = null,
    string? RequestId = null);

public sealed record PersonalizeDeleteResult(string? RequestId = null);

public sealed record PersonalizeDescribeDatasetGroupResult(
    DatasetGroup? DatasetGroup = null,
    string? RequestId = null);

public sealed record PersonalizeListDatasetGroupsResult(
    List<DatasetGroupSummary>? DatasetGroups = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeDescribeDatasetResult(
    Dataset? Dataset = null,
    string? RequestId = null);

public sealed record PersonalizeListDatasetsResult(
    List<DatasetSummary>? Datasets = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeDescribeSchemaResult(
    DatasetSchema? Schema = null,
    string? RequestId = null);

public sealed record PersonalizeListSchemasResult(
    List<DatasetSchemaSummary>? Schemas = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeDescribeSolutionResult(
    Solution? Solution = null,
    string? RequestId = null);

public sealed record PersonalizeListSolutionsResult(
    List<SolutionSummary>? Solutions = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeDescribeSolutionVersionResult(
    SolutionVersion? SolutionVersion = null,
    string? RequestId = null);

public sealed record PersonalizeListSolutionVersionsResult(
    List<SolutionVersionSummary>? SolutionVersions = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeDescribeCampaignResult(
    Campaign? Campaign = null,
    string? RequestId = null);

public sealed record PersonalizeListCampaignsResult(
    List<CampaignSummary>? Campaigns = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeDescribeBatchInferenceJobResult(
    BatchInferenceJob? BatchInferenceJob = null,
    string? RequestId = null);

public sealed record PersonalizeListBatchInferenceJobsResult(
    List<BatchInferenceJobSummary>? BatchInferenceJobs = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeDescribeBatchSegmentJobResult(
    BatchSegmentJob? BatchSegmentJob = null,
    string? RequestId = null);

public sealed record PersonalizeListBatchSegmentJobsResult(
    List<BatchSegmentJobSummary>? BatchSegmentJobs = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeDescribeDatasetImportJobResult(
    DatasetImportJob? DatasetImportJob = null,
    string? RequestId = null);

public sealed record PersonalizeListDatasetImportJobsResult(
    List<DatasetImportJobSummary>? DatasetImportJobs = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeDescribeDatasetExportJobResult(
    DatasetExportJob? DatasetExportJob = null,
    string? RequestId = null);

public sealed record PersonalizeListDatasetExportJobsResult(
    List<DatasetExportJobSummary>? DatasetExportJobs = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeDescribeFilterResult(
    Filter? Filter = null,
    string? RequestId = null);

public sealed record PersonalizeListFiltersResult(
    List<FilterSummary>? Filters = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeDescribeRecommenderResult(
    Recommender? Recommender = null,
    string? RequestId = null);

public sealed record PersonalizeListRecommendersResult(
    List<RecommenderSummary>? Recommenders = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeDescribeEventTrackerResult(
    EventTracker? EventTracker = null,
    string? RequestId = null);

public sealed record PersonalizeListEventTrackersResult(
    List<EventTrackerSummary>? EventTrackers = null,
    string? NextToken = null,
    string? RequestId = null);

public sealed record PersonalizeTagResult(string? RequestId = null);

public sealed record PersonalizeListTagsResult(
    List<Tag>? Tags = null,
    string? RequestId = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Utility helpers for Amazon Personalize.
/// </summary>
public static class PersonalizeService
{
    private static AmazonPersonalizeClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonPersonalizeClient>(region);

    // ── Dataset Group ────────────────────────────────────────────────

    /// <summary>Create a Personalize dataset group.</summary>
    public static async Task<PersonalizeResourceResult>
        CreateDatasetGroupAsync(
            string name,
            string? roleArn = null,
            string? kmsKeyArn = null,
            string? domain = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDatasetGroupRequest { Name = name };
        if (roleArn != null) request.RoleArn = roleArn;
        if (kmsKeyArn != null) request.KmsKeyArn = kmsKeyArn;
        if (domain != null) request.Domain = domain;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDatasetGroupAsync(request);
            return new PersonalizeResourceResult(
                Arn: resp.DatasetGroupArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create dataset group '{name}'");
        }
    }

    /// <summary>Delete a Personalize dataset group.</summary>
    public static async Task<PersonalizeDeleteResult>
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
            return new PersonalizeDeleteResult();
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete dataset group '{datasetGroupArn}'");
        }
    }

    /// <summary>Describe a Personalize dataset group.</summary>
    public static async Task<PersonalizeDescribeDatasetGroupResult>
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
            return new PersonalizeDescribeDatasetGroupResult(
                DatasetGroup: resp.DatasetGroup);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe dataset group '{datasetGroupArn}'");
        }
    }

    /// <summary>List Personalize dataset groups.</summary>
    public static async Task<PersonalizeListDatasetGroupsResult>
        ListDatasetGroupsAsync(
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDatasetGroupsRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListDatasetGroupsAsync(request);
            return new PersonalizeListDatasetGroupsResult(
                DatasetGroups: resp.DatasetGroups,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list dataset groups");
        }
    }

    // ── Dataset ──────────────────────────────────────────────────────

    /// <summary>Create a Personalize dataset.</summary>
    public static async Task<PersonalizeResourceResult> CreateDatasetAsync(
        string name,
        string schemaArn,
        string datasetGroupArn,
        string datasetType,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDatasetRequest
        {
            Name = name,
            SchemaArn = schemaArn,
            DatasetGroupArn = datasetGroupArn,
            DatasetType = datasetType
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDatasetAsync(request);
            return new PersonalizeResourceResult(Arn: resp.DatasetArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create dataset '{name}'");
        }
    }

    /// <summary>Delete a Personalize dataset.</summary>
    public static async Task<PersonalizeDeleteResult> DeleteDatasetAsync(
        string datasetArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDatasetAsync(
                new DeleteDatasetRequest { DatasetArn = datasetArn });
            return new PersonalizeDeleteResult();
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete dataset '{datasetArn}'");
        }
    }

    /// <summary>Describe a Personalize dataset.</summary>
    public static async Task<PersonalizeDescribeDatasetResult>
        DescribeDatasetAsync(
            string datasetArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeDatasetAsync(
                new DescribeDatasetRequest { DatasetArn = datasetArn });
            return new PersonalizeDescribeDatasetResult(
                Dataset: resp.Dataset);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe dataset '{datasetArn}'");
        }
    }

    /// <summary>List Personalize datasets.</summary>
    public static async Task<PersonalizeListDatasetsResult>
        ListDatasetsAsync(
            string? datasetGroupArn = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDatasetsRequest();
        if (datasetGroupArn != null) request.DatasetGroupArn = datasetGroupArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListDatasetsAsync(request);
            return new PersonalizeListDatasetsResult(
                Datasets: resp.Datasets,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list datasets");
        }
    }

    /// <summary>Update a Personalize dataset.</summary>
    public static async Task<PersonalizeResourceResult> UpdateDatasetAsync(
        string datasetArn,
        string schemaArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateDatasetAsync(
                new UpdateDatasetRequest
                {
                    DatasetArn = datasetArn,
                    SchemaArn = schemaArn
                });
            return new PersonalizeResourceResult(Arn: resp.DatasetArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update dataset '{datasetArn}'");
        }
    }

    // ── Schema ───────────────────────────────────────────────────────

    /// <summary>Create a Personalize schema.</summary>
    public static async Task<PersonalizeResourceResult> CreateSchemaAsync(
        string name,
        string schema,
        string? domain = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSchemaRequest
        {
            Name = name,
            Schema = schema
        };
        if (domain != null) request.Domain = domain;

        try
        {
            var resp = await client.CreateSchemaAsync(request);
            return new PersonalizeResourceResult(Arn: resp.SchemaArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create schema '{name}'");
        }
    }

    /// <summary>Delete a Personalize schema.</summary>
    public static async Task<PersonalizeDeleteResult> DeleteSchemaAsync(
        string schemaArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteSchemaAsync(
                new DeleteSchemaRequest { SchemaArn = schemaArn });
            return new PersonalizeDeleteResult();
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete schema '{schemaArn}'");
        }
    }

    /// <summary>Describe a Personalize schema.</summary>
    public static async Task<PersonalizeDescribeSchemaResult>
        DescribeSchemaAsync(
            string schemaArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeSchemaAsync(
                new DescribeSchemaRequest { SchemaArn = schemaArn });
            return new PersonalizeDescribeSchemaResult(Schema: resp.Schema);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe schema '{schemaArn}'");
        }
    }

    /// <summary>List Personalize schemas.</summary>
    public static async Task<PersonalizeListSchemasResult>
        ListSchemasAsync(
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListSchemasRequest();
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListSchemasAsync(request);
            return new PersonalizeListSchemasResult(
                Schemas: resp.Schemas,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list schemas");
        }
    }

    // ── Solution ─────────────────────────────────────────────────────

    /// <summary>Create a Personalize solution.</summary>
    public static async Task<PersonalizeResourceResult> CreateSolutionAsync(
        string name,
        string datasetGroupArn,
        string? recipeArn = null,
        string? eventType = null,
        SolutionConfig? solutionConfig = null,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSolutionRequest
        {
            Name = name,
            DatasetGroupArn = datasetGroupArn
        };
        if (recipeArn != null) request.RecipeArn = recipeArn;
        if (eventType != null) request.EventType = eventType;
        if (solutionConfig != null) request.SolutionConfig = solutionConfig;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateSolutionAsync(request);
            return new PersonalizeResourceResult(Arn: resp.SolutionArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create solution '{name}'");
        }
    }

    /// <summary>Delete a Personalize solution.</summary>
    public static async Task<PersonalizeDeleteResult> DeleteSolutionAsync(
        string solutionArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteSolutionAsync(
                new DeleteSolutionRequest { SolutionArn = solutionArn });
            return new PersonalizeDeleteResult();
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete solution '{solutionArn}'");
        }
    }

    /// <summary>Describe a Personalize solution.</summary>
    public static async Task<PersonalizeDescribeSolutionResult>
        DescribeSolutionAsync(
            string solutionArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeSolutionAsync(
                new DescribeSolutionRequest { SolutionArn = solutionArn });
            return new PersonalizeDescribeSolutionResult(
                Solution: resp.Solution);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe solution '{solutionArn}'");
        }
    }

    /// <summary>List Personalize solutions.</summary>
    public static async Task<PersonalizeListSolutionsResult>
        ListSolutionsAsync(
            string? datasetGroupArn = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListSolutionsRequest();
        if (datasetGroupArn != null) request.DatasetGroupArn = datasetGroupArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListSolutionsAsync(request);
            return new PersonalizeListSolutionsResult(
                Solutions: resp.Solutions,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list solutions");
        }
    }

    // ── Solution Version ─────────────────────────────────────────────

    /// <summary>Create a Personalize solution version.</summary>
    public static async Task<PersonalizeResourceResult>
        CreateSolutionVersionAsync(
            string solutionArn,
            string? trainingMode = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateSolutionVersionRequest
        {
            SolutionArn = solutionArn
        };
        if (trainingMode != null) request.TrainingMode = trainingMode;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateSolutionVersionAsync(request);
            return new PersonalizeResourceResult(
                Arn: resp.SolutionVersionArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create solution version for '{solutionArn}'");
        }
    }

    /// <summary>Describe a Personalize solution version.</summary>
    public static async Task<PersonalizeDescribeSolutionVersionResult>
        DescribeSolutionVersionAsync(
            string solutionVersionArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeSolutionVersionAsync(
                new DescribeSolutionVersionRequest
                {
                    SolutionVersionArn = solutionVersionArn
                });
            return new PersonalizeDescribeSolutionVersionResult(
                SolutionVersion: resp.SolutionVersion);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe solution version '{solutionVersionArn}'");
        }
    }

    /// <summary>List Personalize solution versions.</summary>
    public static async Task<PersonalizeListSolutionVersionsResult>
        ListSolutionVersionsAsync(
            string? solutionArn = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListSolutionVersionsRequest();
        if (solutionArn != null) request.SolutionArn = solutionArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListSolutionVersionsAsync(request);
            return new PersonalizeListSolutionVersionsResult(
                SolutionVersions: resp.SolutionVersions,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list solution versions");
        }
    }

    // ── Campaign ─────────────────────────────────────────────────────

    /// <summary>Create a Personalize campaign.</summary>
    public static async Task<PersonalizeResourceResult>
        CreateCampaignAsync(
            string name,
            string solutionVersionArn,
            int? minProvisionedTPS = null,
            CampaignConfig? campaignConfig = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateCampaignRequest
        {
            Name = name,
            SolutionVersionArn = solutionVersionArn
        };
        if (minProvisionedTPS.HasValue)
            request.MinProvisionedTPS = minProvisionedTPS.Value;
        if (campaignConfig != null) request.CampaignConfig = campaignConfig;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateCampaignAsync(request);
            return new PersonalizeResourceResult(Arn: resp.CampaignArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create campaign '{name}'");
        }
    }

    /// <summary>Delete a Personalize campaign.</summary>
    public static async Task<PersonalizeDeleteResult> DeleteCampaignAsync(
        string campaignArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteCampaignAsync(
                new DeleteCampaignRequest { CampaignArn = campaignArn });
            return new PersonalizeDeleteResult();
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete campaign '{campaignArn}'");
        }
    }

    /// <summary>Describe a Personalize campaign.</summary>
    public static async Task<PersonalizeDescribeCampaignResult>
        DescribeCampaignAsync(
            string campaignArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeCampaignAsync(
                new DescribeCampaignRequest { CampaignArn = campaignArn });
            return new PersonalizeDescribeCampaignResult(
                Campaign: resp.Campaign);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe campaign '{campaignArn}'");
        }
    }

    /// <summary>List Personalize campaigns.</summary>
    public static async Task<PersonalizeListCampaignsResult>
        ListCampaignsAsync(
            string? solutionArn = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListCampaignsRequest();
        if (solutionArn != null) request.SolutionArn = solutionArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListCampaignsAsync(request);
            return new PersonalizeListCampaignsResult(
                Campaigns: resp.Campaigns,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list campaigns");
        }
    }

    /// <summary>Update a Personalize campaign.</summary>
    public static async Task<PersonalizeResourceResult>
        UpdateCampaignAsync(
            string campaignArn,
            string? solutionVersionArn = null,
            int? minProvisionedTPS = null,
            CampaignConfig? campaignConfig = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new UpdateCampaignRequest
        {
            CampaignArn = campaignArn
        };
        if (solutionVersionArn != null)
            request.SolutionVersionArn = solutionVersionArn;
        if (minProvisionedTPS.HasValue)
            request.MinProvisionedTPS = minProvisionedTPS.Value;
        if (campaignConfig != null) request.CampaignConfig = campaignConfig;

        try
        {
            var resp = await client.UpdateCampaignAsync(request);
            return new PersonalizeResourceResult(Arn: resp.CampaignArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update campaign '{campaignArn}'");
        }
    }

    // ── Batch Inference Job ──────────────────────────────────────────

    /// <summary>Create a Personalize batch inference job.</summary>
    public static async Task<PersonalizeResourceResult>
        CreateBatchInferenceJobAsync(
            string jobName,
            string solutionVersionArn,
            BatchInferenceJobInput jobInput,
            BatchInferenceJobOutput jobOutput,
            string roleArn,
            BatchInferenceJobConfig? batchInferenceJobConfig = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateBatchInferenceJobRequest
        {
            JobName = jobName,
            SolutionVersionArn = solutionVersionArn,
            JobInput = jobInput,
            JobOutput = jobOutput,
            RoleArn = roleArn
        };
        if (batchInferenceJobConfig != null)
            request.BatchInferenceJobConfig = batchInferenceJobConfig;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateBatchInferenceJobAsync(request);
            return new PersonalizeResourceResult(
                Arn: resp.BatchInferenceJobArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create batch inference job '{jobName}'");
        }
    }

    /// <summary>Describe a Personalize batch inference job.</summary>
    public static async Task<PersonalizeDescribeBatchInferenceJobResult>
        DescribeBatchInferenceJobAsync(
            string batchInferenceJobArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeBatchInferenceJobAsync(
                new DescribeBatchInferenceJobRequest
                {
                    BatchInferenceJobArn = batchInferenceJobArn
                });
            return new PersonalizeDescribeBatchInferenceJobResult(
                BatchInferenceJob: resp.BatchInferenceJob);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe batch inference job '{batchInferenceJobArn}'");
        }
    }

    /// <summary>List Personalize batch inference jobs.</summary>
    public static async Task<PersonalizeListBatchInferenceJobsResult>
        ListBatchInferenceJobsAsync(
            string? solutionVersionArn = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListBatchInferenceJobsRequest();
        if (solutionVersionArn != null)
            request.SolutionVersionArn = solutionVersionArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListBatchInferenceJobsAsync(request);
            return new PersonalizeListBatchInferenceJobsResult(
                BatchInferenceJobs: resp.BatchInferenceJobs,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list batch inference jobs");
        }
    }

    // ── Batch Segment Job ────────────────────────────────────────────

    /// <summary>Create a Personalize batch segment job.</summary>
    public static async Task<PersonalizeResourceResult>
        CreateBatchSegmentJobAsync(
            string jobName,
            string solutionVersionArn,
            BatchSegmentJobInput jobInput,
            BatchSegmentJobOutput jobOutput,
            string roleArn,
            int? numResults = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateBatchSegmentJobRequest
        {
            JobName = jobName,
            SolutionVersionArn = solutionVersionArn,
            JobInput = jobInput,
            JobOutput = jobOutput,
            RoleArn = roleArn
        };
        if (numResults.HasValue) request.NumResults = numResults.Value;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateBatchSegmentJobAsync(request);
            return new PersonalizeResourceResult(
                Arn: resp.BatchSegmentJobArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create batch segment job '{jobName}'");
        }
    }

    /// <summary>Describe a Personalize batch segment job.</summary>
    public static async Task<PersonalizeDescribeBatchSegmentJobResult>
        DescribeBatchSegmentJobAsync(
            string batchSegmentJobArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeBatchSegmentJobAsync(
                new DescribeBatchSegmentJobRequest
                {
                    BatchSegmentJobArn = batchSegmentJobArn
                });
            return new PersonalizeDescribeBatchSegmentJobResult(
                BatchSegmentJob: resp.BatchSegmentJob);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe batch segment job '{batchSegmentJobArn}'");
        }
    }

    /// <summary>List Personalize batch segment jobs.</summary>
    public static async Task<PersonalizeListBatchSegmentJobsResult>
        ListBatchSegmentJobsAsync(
            string? solutionVersionArn = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListBatchSegmentJobsRequest();
        if (solutionVersionArn != null)
            request.SolutionVersionArn = solutionVersionArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListBatchSegmentJobsAsync(request);
            return new PersonalizeListBatchSegmentJobsResult(
                BatchSegmentJobs: resp.BatchSegmentJobs,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list batch segment jobs");
        }
    }

    // ── Dataset Import Job ───────────────────────────────────────────

    /// <summary>Create a Personalize dataset import job.</summary>
    public static async Task<PersonalizeResourceResult>
        CreateDatasetImportJobAsync(
            string jobName,
            string datasetArn,
            DataSource dataSource,
            string roleArn,
            string? importMode = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDatasetImportJobRequest
        {
            JobName = jobName,
            DatasetArn = datasetArn,
            DataSource = dataSource,
            RoleArn = roleArn
        };
        if (importMode != null) request.ImportMode = importMode;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDatasetImportJobAsync(request);
            return new PersonalizeResourceResult(
                Arn: resp.DatasetImportJobArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create dataset import job '{jobName}'");
        }
    }

    /// <summary>Describe a Personalize dataset import job.</summary>
    public static async Task<PersonalizeDescribeDatasetImportJobResult>
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
            return new PersonalizeDescribeDatasetImportJobResult(
                DatasetImportJob: resp.DatasetImportJob);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe dataset import job '{datasetImportJobArn}'");
        }
    }

    /// <summary>List Personalize dataset import jobs.</summary>
    public static async Task<PersonalizeListDatasetImportJobsResult>
        ListDatasetImportJobsAsync(
            string? datasetArn = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDatasetImportJobsRequest();
        if (datasetArn != null) request.DatasetArn = datasetArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListDatasetImportJobsAsync(request);
            return new PersonalizeListDatasetImportJobsResult(
                DatasetImportJobs: resp.DatasetImportJobs,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list dataset import jobs");
        }
    }

    // ── Dataset Export Job ───────────────────────────────────────────

    /// <summary>Create a Personalize dataset export job.</summary>
    public static async Task<PersonalizeResourceResult>
        CreateDatasetExportJobAsync(
            string jobName,
            string datasetArn,
            DatasetExportJobOutput jobOutput,
            string roleArn,
            string? ingestionMode = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateDatasetExportJobRequest
        {
            JobName = jobName,
            DatasetArn = datasetArn,
            JobOutput = jobOutput,
            RoleArn = roleArn
        };
        if (ingestionMode != null) request.IngestionMode = ingestionMode;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateDatasetExportJobAsync(request);
            return new PersonalizeResourceResult(
                Arn: resp.DatasetExportJobArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create dataset export job '{jobName}'");
        }
    }

    /// <summary>Describe a Personalize dataset export job.</summary>
    public static async Task<PersonalizeDescribeDatasetExportJobResult>
        DescribeDatasetExportJobAsync(
            string datasetExportJobArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeDatasetExportJobAsync(
                new DescribeDatasetExportJobRequest
                {
                    DatasetExportJobArn = datasetExportJobArn
                });
            return new PersonalizeDescribeDatasetExportJobResult(
                DatasetExportJob: resp.DatasetExportJob);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe dataset export job '{datasetExportJobArn}'");
        }
    }

    /// <summary>List Personalize dataset export jobs.</summary>
    public static async Task<PersonalizeListDatasetExportJobsResult>
        ListDatasetExportJobsAsync(
            string? datasetArn = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDatasetExportJobsRequest();
        if (datasetArn != null) request.DatasetArn = datasetArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListDatasetExportJobsAsync(request);
            return new PersonalizeListDatasetExportJobsResult(
                DatasetExportJobs: resp.DatasetExportJobs,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list dataset export jobs");
        }
    }

    // ── Filter ───────────────────────────────────────────────────────

    /// <summary>Create a Personalize filter.</summary>
    public static async Task<PersonalizeResourceResult> CreateFilterAsync(
        string name,
        string datasetGroupArn,
        string filterExpression,
        List<Tag>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateFilterRequest
        {
            Name = name,
            DatasetGroupArn = datasetGroupArn,
            FilterExpression = filterExpression
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateFilterAsync(request);
            return new PersonalizeResourceResult(Arn: resp.FilterArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create filter '{name}'");
        }
    }

    /// <summary>Delete a Personalize filter.</summary>
    public static async Task<PersonalizeDeleteResult> DeleteFilterAsync(
        string filterArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteFilterAsync(
                new DeleteFilterRequest { FilterArn = filterArn });
            return new PersonalizeDeleteResult();
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete filter '{filterArn}'");
        }
    }

    /// <summary>Describe a Personalize filter.</summary>
    public static async Task<PersonalizeDescribeFilterResult>
        DescribeFilterAsync(
            string filterArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeFilterAsync(
                new DescribeFilterRequest { FilterArn = filterArn });
            return new PersonalizeDescribeFilterResult(Filter: resp.Filter);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe filter '{filterArn}'");
        }
    }

    /// <summary>List Personalize filters.</summary>
    public static async Task<PersonalizeListFiltersResult>
        ListFiltersAsync(
            string? datasetGroupArn = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListFiltersRequest();
        if (datasetGroupArn != null) request.DatasetGroupArn = datasetGroupArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListFiltersAsync(request);
            return new PersonalizeListFiltersResult(
                Filters: resp.Filters,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list filters");
        }
    }

    // ── Recommender ──────────────────────────────────────────────────

    /// <summary>Create a Personalize recommender.</summary>
    public static async Task<PersonalizeResourceResult>
        CreateRecommenderAsync(
            string name,
            string datasetGroupArn,
            string recipeArn,
            RecommenderConfig? recommenderConfig = null,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateRecommenderRequest
        {
            Name = name,
            DatasetGroupArn = datasetGroupArn,
            RecipeArn = recipeArn
        };
        if (recommenderConfig != null)
            request.RecommenderConfig = recommenderConfig;
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateRecommenderAsync(request);
            return new PersonalizeResourceResult(
                Arn: resp.RecommenderArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create recommender '{name}'");
        }
    }

    /// <summary>Delete a Personalize recommender.</summary>
    public static async Task<PersonalizeDeleteResult>
        DeleteRecommenderAsync(
            string recommenderArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteRecommenderAsync(
                new DeleteRecommenderRequest
                {
                    RecommenderArn = recommenderArn
                });
            return new PersonalizeDeleteResult();
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete recommender '{recommenderArn}'");
        }
    }

    /// <summary>Describe a Personalize recommender.</summary>
    public static async Task<PersonalizeDescribeRecommenderResult>
        DescribeRecommenderAsync(
            string recommenderArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeRecommenderAsync(
                new DescribeRecommenderRequest
                {
                    RecommenderArn = recommenderArn
                });
            return new PersonalizeDescribeRecommenderResult(
                Recommender: resp.Recommender);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe recommender '{recommenderArn}'");
        }
    }

    /// <summary>List Personalize recommenders.</summary>
    public static async Task<PersonalizeListRecommendersResult>
        ListRecommendersAsync(
            string? datasetGroupArn = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListRecommendersRequest();
        if (datasetGroupArn != null) request.DatasetGroupArn = datasetGroupArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListRecommendersAsync(request);
            return new PersonalizeListRecommendersResult(
                Recommenders: resp.Recommenders,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list recommenders");
        }
    }

    /// <summary>Update a Personalize recommender.</summary>
    public static async Task<PersonalizeResourceResult>
        UpdateRecommenderAsync(
            string recommenderArn,
            RecommenderConfig recommenderConfig,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateRecommenderAsync(
                new UpdateRecommenderRequest
                {
                    RecommenderArn = recommenderArn,
                    RecommenderConfig = recommenderConfig
                });
            return new PersonalizeResourceResult(
                Arn: resp.RecommenderArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update recommender '{recommenderArn}'");
        }
    }

    // ── Event Tracker ────────────────────────────────────────────────

    /// <summary>Create a Personalize event tracker.</summary>
    public static async Task<PersonalizeResourceResult>
        CreateEventTrackerAsync(
            string name,
            string datasetGroupArn,
            List<Tag>? tags = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateEventTrackerRequest
        {
            Name = name,
            DatasetGroupArn = datasetGroupArn
        };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateEventTrackerAsync(request);
            return new PersonalizeResourceResult(
                Arn: resp.EventTrackerArn);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create event tracker '{name}'");
        }
    }

    /// <summary>Delete a Personalize event tracker.</summary>
    public static async Task<PersonalizeDeleteResult>
        DeleteEventTrackerAsync(
            string eventTrackerArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteEventTrackerAsync(
                new DeleteEventTrackerRequest
                {
                    EventTrackerArn = eventTrackerArn
                });
            return new PersonalizeDeleteResult();
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete event tracker '{eventTrackerArn}'");
        }
    }

    /// <summary>Describe a Personalize event tracker.</summary>
    public static async Task<PersonalizeDescribeEventTrackerResult>
        DescribeEventTrackerAsync(
            string eventTrackerArn,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeEventTrackerAsync(
                new DescribeEventTrackerRequest
                {
                    EventTrackerArn = eventTrackerArn
                });
            return new PersonalizeDescribeEventTrackerResult(
                EventTracker: resp.EventTracker);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe event tracker '{eventTrackerArn}'");
        }
    }

    /// <summary>List Personalize event trackers.</summary>
    public static async Task<PersonalizeListEventTrackersResult>
        ListEventTrackersAsync(
            string? datasetGroupArn = null,
            string? nextToken = null,
            int? maxResults = null,
            RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListEventTrackersRequest();
        if (datasetGroupArn != null) request.DatasetGroupArn = datasetGroupArn;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.ListEventTrackersAsync(request);
            return new PersonalizeListEventTrackersResult(
                EventTrackers: resp.EventTrackers,
                NextToken: resp.NextToken);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list event trackers");
        }
    }

    // ── Tags ─────────────────────────────────────────────────────────

    /// <summary>Tag a Personalize resource.</summary>
    public static async Task<PersonalizeTagResult> TagResourceAsync(
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
            return new PersonalizeTagResult();
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>Remove tags from a Personalize resource.</summary>
    public static async Task<PersonalizeTagResult> UntagResourceAsync(
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
            return new PersonalizeTagResult();
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>List tags for a Personalize resource.</summary>
    public static async Task<PersonalizeListTagsResult>
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
            return new PersonalizeListTagsResult(Tags: resp.Tags);
        }
        catch (AmazonPersonalizeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }
}

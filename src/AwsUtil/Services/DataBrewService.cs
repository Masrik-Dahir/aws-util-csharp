using Amazon;
using Amazon.GlueDataBrew;
using Amazon.GlueDataBrew.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for AWS Glue DataBrew operations.
/// </summary>
public sealed record CreateDataBrewDatasetResult(string? Name = null);
public sealed record DescribeDataBrewDatasetResult(string? Name = null, Input? Input = null);
public sealed record ListDataBrewDatasetsResult(List<Dataset>? Datasets = null);
public sealed record UpdateDataBrewDatasetResult(string? Name = null);
public sealed record CreateDataBrewProjectResult(string? Name = null);
public sealed record DescribeDataBrewProjectResult(string? Name = null, string? DatasetName = null, string? RecipeName = null);
public sealed record ListDataBrewProjectsResult(List<Project>? Projects = null);
public sealed record UpdateDataBrewProjectResult(string? Name = null);
public sealed record CreateDataBrewRecipeResult(string? Name = null);
public sealed record DescribeDataBrewRecipeResult(string? Name = null, string? Description = null);
public sealed record ListDataBrewRecipesResult(List<Recipe>? Recipes = null);
public sealed record ListDataBrewRecipeVersionsResult(List<Recipe>? Recipes = null);
public sealed record PublishDataBrewRecipeResult(string? Name = null);
public sealed record UpdateDataBrewRecipeResult(string? Name = null);
public sealed record DeleteDataBrewRecipeVersionResult(string? Name = null, string? RecipeVersion = null);
public sealed record CreateDataBrewRecipeJobResult(string? Name = null);
public sealed record DescribeDataBrewJobResult(string? Name = null, string? Type = null);
public sealed record ListDataBrewJobsResult(List<Amazon.GlueDataBrew.Model.Job>? Jobs = null);
public sealed record UpdateDataBrewRecipeJobResult(string? Name = null);
public sealed record CreateDataBrewProfileJobResult(string? Name = null);
public sealed record UpdateDataBrewProfileJobResult(string? Name = null);
public sealed record StartDataBrewJobRunResult(string? RunId = null);
public sealed record StopDataBrewJobRunResult(string? RunId = null);
public sealed record DescribeDataBrewJobRunResult(string? RunId = null, string? State = null);
public sealed record ListDataBrewJobRunsResult(List<JobRun>? JobRuns = null);
public sealed record CreateDataBrewRulesetResult(string? Name = null);
public sealed record DescribeDataBrewRulesetResult(string? Name = null, string? Description = null);
public sealed record ListDataBrewRulesetsResult(List<RulesetItem>? Rulesets = null);
public sealed record UpdateDataBrewRulesetResult(string? Name = null);
public sealed record CreateDataBrewScheduleResult(string? Name = null);
public sealed record DescribeDataBrewScheduleResult(string? Name = null, string? CronExpression = null);
public sealed record ListDataBrewSchedulesResult(List<Schedule>? Schedules = null);
public sealed record UpdateDataBrewScheduleResult(string? Name = null);
public sealed record SendProjectSessionActionResult(string? Name = null, int? ActionId = null);
public sealed record StartProjectSessionResult(string? Name = null, string? ClientSessionId = null);
public sealed record DataBrewTagResourceResult(bool Success = true);
public sealed record DataBrewUntagResourceResult(bool Success = true);
public sealed record ListDataBrewTagsResult(Dictionary<string, string>? Tags = null);
public sealed record BatchDeleteRecipeVersionResult(
    string? Name = null,
    List<RecipeVersionErrorDetail>? Errors = null);

/// <summary>
/// Utility helpers for AWS Glue DataBrew.
/// </summary>
public static class DataBrewService
{
    private static AmazonGlueDataBrewClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonGlueDataBrewClient>(region);

    // ──────────────────────────── Datasets ────────────────────────────

    /// <summary>
    /// Create a DataBrew dataset.
    /// </summary>
    public static async Task<CreateDataBrewDatasetResult> CreateDatasetAsync(
        CreateDatasetRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateDatasetAsync(request);
            return new CreateDataBrewDatasetResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create DataBrew dataset '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a DataBrew dataset.
    /// </summary>
    public static async Task DeleteDatasetAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDatasetAsync(
                new DeleteDatasetRequest { Name = name });
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DataBrew dataset '{name}'");
        }
    }

    /// <summary>
    /// Describe a DataBrew dataset.
    /// </summary>
    public static async Task<DescribeDataBrewDatasetResult> DescribeDatasetAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeDatasetAsync(
                new DescribeDatasetRequest { Name = name });
            return new DescribeDataBrewDatasetResult(
                Name: resp.Name,
                Input: resp.Input);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe DataBrew dataset '{name}'");
        }
    }

    /// <summary>
    /// List DataBrew datasets, automatically paginating.
    /// </summary>
    public static async Task<ListDataBrewDatasetsResult> ListDatasetsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var datasets = new List<Dataset>();
        var request = new ListDatasetsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListDatasetsAsync(request);
                if (resp.Datasets != null) datasets.AddRange(resp.Datasets);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list DataBrew datasets");
        }

        return new ListDataBrewDatasetsResult(Datasets: datasets);
    }

    /// <summary>
    /// Update a DataBrew dataset.
    /// </summary>
    public static async Task<UpdateDataBrewDatasetResult> UpdateDatasetAsync(
        UpdateDatasetRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateDatasetAsync(request);
            return new UpdateDataBrewDatasetResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update DataBrew dataset '{request.Name}'");
        }
    }

    // ──────────────────────────── Projects ────────────────────────────

    /// <summary>
    /// Create a DataBrew project.
    /// </summary>
    public static async Task<CreateDataBrewProjectResult> CreateProjectAsync(
        CreateProjectRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateProjectAsync(request);
            return new CreateDataBrewProjectResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create DataBrew project '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a DataBrew project.
    /// </summary>
    public static async Task DeleteProjectAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteProjectAsync(
                new DeleteProjectRequest { Name = name });
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DataBrew project '{name}'");
        }
    }

    /// <summary>
    /// Describe a DataBrew project.
    /// </summary>
    public static async Task<DescribeDataBrewProjectResult> DescribeProjectAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeProjectAsync(
                new DescribeProjectRequest { Name = name });
            return new DescribeDataBrewProjectResult(
                Name: resp.Name,
                DatasetName: resp.DatasetName,
                RecipeName: resp.RecipeName);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe DataBrew project '{name}'");
        }
    }

    /// <summary>
    /// List DataBrew projects, automatically paginating.
    /// </summary>
    public static async Task<ListDataBrewProjectsResult> ListProjectsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var projects = new List<Project>();
        var request = new ListProjectsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListProjectsAsync(request);
                if (resp.Projects != null) projects.AddRange(resp.Projects);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list DataBrew projects");
        }

        return new ListDataBrewProjectsResult(Projects: projects);
    }

    /// <summary>
    /// Update a DataBrew project.
    /// </summary>
    public static async Task<UpdateDataBrewProjectResult> UpdateProjectAsync(
        UpdateProjectRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateProjectAsync(request);
            return new UpdateDataBrewProjectResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update DataBrew project '{request.Name}'");
        }
    }

    // ──────────────────────────── Recipes ────────────────────────────

    /// <summary>
    /// Create a DataBrew recipe.
    /// </summary>
    public static async Task<CreateDataBrewRecipeResult> CreateRecipeAsync(
        CreateRecipeRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateRecipeAsync(request);
            return new CreateDataBrewRecipeResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create DataBrew recipe '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a specific version of a DataBrew recipe.
    /// </summary>
    public static async Task<DeleteDataBrewRecipeVersionResult> DeleteRecipeVersionAsync(
        string name,
        string recipeVersion,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteRecipeVersionAsync(
                new DeleteRecipeVersionRequest
                {
                    Name = name,
                    RecipeVersion = recipeVersion
                });
            return new DeleteDataBrewRecipeVersionResult(
                Name: resp.Name,
                RecipeVersion: resp.RecipeVersion);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DataBrew recipe version '{name}:{recipeVersion}'");
        }
    }

    /// <summary>
    /// Describe a DataBrew recipe.
    /// </summary>
    public static async Task<DescribeDataBrewRecipeResult> DescribeRecipeAsync(
        string name,
        string? recipeVersion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeRecipeRequest { Name = name };
        if (recipeVersion != null) request.RecipeVersion = recipeVersion;

        try
        {
            var resp = await client.DescribeRecipeAsync(request);
            return new DescribeDataBrewRecipeResult(
                Name: resp.Name,
                Description: resp.Description);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe DataBrew recipe '{name}'");
        }
    }

    /// <summary>
    /// List DataBrew recipes, automatically paginating.
    /// </summary>
    public static async Task<ListDataBrewRecipesResult> ListRecipesAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var recipes = new List<Recipe>();
        var request = new ListRecipesRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListRecipesAsync(request);
                if (resp.Recipes != null) recipes.AddRange(resp.Recipes);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list DataBrew recipes");
        }

        return new ListDataBrewRecipesResult(Recipes: recipes);
    }

    /// <summary>
    /// List DataBrew recipe versions, automatically paginating.
    /// </summary>
    public static async Task<ListDataBrewRecipeVersionsResult> ListRecipeVersionsAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var versions = new List<Recipe>();
        var request = new ListRecipeVersionsRequest { Name = name };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListRecipeVersionsAsync(request);
                if (resp.Recipes != null) versions.AddRange(resp.Recipes);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list DataBrew recipe versions for '{name}'");
        }

        return new ListDataBrewRecipeVersionsResult(Recipes: versions);
    }

    /// <summary>
    /// Publish a DataBrew recipe.
    /// </summary>
    public static async Task<PublishDataBrewRecipeResult> PublishRecipeAsync(
        string name,
        string? description = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PublishRecipeRequest { Name = name };
        if (description != null) request.Description = description;

        try
        {
            var resp = await client.PublishRecipeAsync(request);
            return new PublishDataBrewRecipeResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to publish DataBrew recipe '{name}'");
        }
    }

    /// <summary>
    /// Update a DataBrew recipe.
    /// </summary>
    public static async Task<UpdateDataBrewRecipeResult> UpdateRecipeAsync(
        UpdateRecipeRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateRecipeAsync(request);
            return new UpdateDataBrewRecipeResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update DataBrew recipe '{request.Name}'");
        }
    }

    // ──────────────────────────── Jobs ────────────────────────────

    /// <summary>
    /// Create a DataBrew recipe job.
    /// </summary>
    public static async Task<CreateDataBrewRecipeJobResult> CreateRecipeJobAsync(
        CreateRecipeJobRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateRecipeJobAsync(request);
            return new CreateDataBrewRecipeJobResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create DataBrew recipe job '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a DataBrew job.
    /// </summary>
    public static async Task DeleteJobAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteJobAsync(
                new DeleteJobRequest { Name = name });
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DataBrew job '{name}'");
        }
    }

    /// <summary>
    /// Describe a DataBrew job.
    /// </summary>
    public static async Task<DescribeDataBrewJobResult> DescribeJobAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeJobAsync(
                new DescribeJobRequest { Name = name });
            return new DescribeDataBrewJobResult(
                Name: resp.Name,
                Type: resp.Type?.Value);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe DataBrew job '{name}'");
        }
    }

    /// <summary>
    /// List DataBrew jobs, automatically paginating.
    /// </summary>
    public static async Task<ListDataBrewJobsResult> ListJobsAsync(
        string? datasetName = null,
        string? projectName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var jobs = new List<Amazon.GlueDataBrew.Model.Job>();
        var request = new ListJobsRequest();
        if (datasetName != null) request.DatasetName = datasetName;
        if (projectName != null) request.ProjectName = projectName;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListJobsAsync(request);
                if (resp.Jobs != null) jobs.AddRange(resp.Jobs);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list DataBrew jobs");
        }

        return new ListDataBrewJobsResult(Jobs: jobs);
    }

    /// <summary>
    /// Update a DataBrew recipe job.
    /// </summary>
    public static async Task<UpdateDataBrewRecipeJobResult> UpdateRecipeJobAsync(
        UpdateRecipeJobRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateRecipeJobAsync(request);
            return new UpdateDataBrewRecipeJobResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update DataBrew recipe job '{request.Name}'");
        }
    }

    /// <summary>
    /// Create a DataBrew profile job.
    /// </summary>
    public static async Task<CreateDataBrewProfileJobResult> CreateProfileJobAsync(
        CreateProfileJobRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateProfileJobAsync(request);
            return new CreateDataBrewProfileJobResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create DataBrew profile job '{request.Name}'");
        }
    }

    /// <summary>
    /// Update a DataBrew profile job.
    /// </summary>
    public static async Task<UpdateDataBrewProfileJobResult> UpdateProfileJobAsync(
        UpdateProfileJobRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateProfileJobAsync(request);
            return new UpdateDataBrewProfileJobResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update DataBrew profile job '{request.Name}'");
        }
    }

    // ──────────────────────────── Job Runs ────────────────────────────

    /// <summary>
    /// Start a DataBrew job run.
    /// </summary>
    public static async Task<StartDataBrewJobRunResult> StartJobRunAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StartJobRunAsync(
                new StartJobRunRequest { Name = name });
            return new StartDataBrewJobRunResult(RunId: resp.RunId);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start DataBrew job run for '{name}'");
        }
    }

    /// <summary>
    /// Stop a DataBrew job run.
    /// </summary>
    public static async Task<StopDataBrewJobRunResult> StopJobRunAsync(
        string name,
        string runId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StopJobRunAsync(
                new StopJobRunRequest { Name = name, RunId = runId });
            return new StopDataBrewJobRunResult(RunId: resp.RunId);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop DataBrew job run '{runId}' for '{name}'");
        }
    }

    /// <summary>
    /// Describe a DataBrew job run.
    /// </summary>
    public static async Task<DescribeDataBrewJobRunResult> DescribeJobRunAsync(
        string name,
        string runId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeJobRunAsync(
                new DescribeJobRunRequest { Name = name, RunId = runId });
            return new DescribeDataBrewJobRunResult(
                RunId: resp.RunId,
                State: resp.State?.Value);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe DataBrew job run '{runId}' for '{name}'");
        }
    }

    /// <summary>
    /// List DataBrew job runs, automatically paginating.
    /// </summary>
    public static async Task<ListDataBrewJobRunsResult> ListJobRunsAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var runs = new List<JobRun>();
        var request = new ListJobRunsRequest { Name = name };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListJobRunsAsync(request);
                if (resp.JobRuns != null) runs.AddRange(resp.JobRuns);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list DataBrew job runs for '{name}'");
        }

        return new ListDataBrewJobRunsResult(JobRuns: runs);
    }

    // ──────────────────────────── Rulesets ────────────────────────────

    /// <summary>
    /// Create a DataBrew ruleset.
    /// </summary>
    public static async Task<CreateDataBrewRulesetResult> CreateRulesetAsync(
        CreateRulesetRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateRulesetAsync(request);
            return new CreateDataBrewRulesetResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create DataBrew ruleset '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a DataBrew ruleset.
    /// </summary>
    public static async Task DeleteRulesetAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteRulesetAsync(
                new DeleteRulesetRequest { Name = name });
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DataBrew ruleset '{name}'");
        }
    }

    /// <summary>
    /// Describe a DataBrew ruleset.
    /// </summary>
    public static async Task<DescribeDataBrewRulesetResult> DescribeRulesetAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeRulesetAsync(
                new DescribeRulesetRequest { Name = name });
            return new DescribeDataBrewRulesetResult(
                Name: resp.Name,
                Description: resp.Description);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe DataBrew ruleset '{name}'");
        }
    }

    /// <summary>
    /// List DataBrew rulesets, automatically paginating.
    /// </summary>
    public static async Task<ListDataBrewRulesetsResult> ListRulesetsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var rulesets = new List<RulesetItem>();
        var request = new ListRulesetsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListRulesetsAsync(request);
                if (resp.Rulesets != null) rulesets.AddRange(resp.Rulesets);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list DataBrew rulesets");
        }

        return new ListDataBrewRulesetsResult(Rulesets: rulesets);
    }

    /// <summary>
    /// Update a DataBrew ruleset.
    /// </summary>
    public static async Task<UpdateDataBrewRulesetResult> UpdateRulesetAsync(
        UpdateRulesetRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateRulesetAsync(request);
            return new UpdateDataBrewRulesetResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update DataBrew ruleset '{request.Name}'");
        }
    }

    // ──────────────────────────── Schedules ────────────────────────────

    /// <summary>
    /// Create a DataBrew schedule.
    /// </summary>
    public static async Task<CreateDataBrewScheduleResult> CreateScheduleAsync(
        CreateScheduleRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateScheduleAsync(request);
            return new CreateDataBrewScheduleResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create DataBrew schedule '{request.Name}'");
        }
    }

    /// <summary>
    /// Delete a DataBrew schedule.
    /// </summary>
    public static async Task DeleteScheduleAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteScheduleAsync(
                new DeleteScheduleRequest { Name = name });
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete DataBrew schedule '{name}'");
        }
    }

    /// <summary>
    /// Describe a DataBrew schedule.
    /// </summary>
    public static async Task<DescribeDataBrewScheduleResult> DescribeScheduleAsync(
        string name,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeScheduleAsync(
                new DescribeScheduleRequest { Name = name });
            return new DescribeDataBrewScheduleResult(
                Name: resp.Name,
                CronExpression: resp.CronExpression);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe DataBrew schedule '{name}'");
        }
    }

    /// <summary>
    /// List DataBrew schedules, automatically paginating.
    /// </summary>
    public static async Task<ListDataBrewSchedulesResult> ListSchedulesAsync(
        string? jobName = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var schedules = new List<Schedule>();
        var request = new ListSchedulesRequest();
        if (jobName != null) request.JobName = jobName;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListSchedulesAsync(request);
                if (resp.Schedules != null) schedules.AddRange(resp.Schedules);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list DataBrew schedules");
        }

        return new ListDataBrewSchedulesResult(Schedules: schedules);
    }

    /// <summary>
    /// Update a DataBrew schedule.
    /// </summary>
    public static async Task<UpdateDataBrewScheduleResult> UpdateScheduleAsync(
        UpdateScheduleRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.UpdateScheduleAsync(request);
            return new UpdateDataBrewScheduleResult(Name: resp.Name);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update DataBrew schedule '{request.Name}'");
        }
    }

    // ──────────────────────────── Project Sessions ────────────────────────────

    /// <summary>
    /// Send a project session action.
    /// </summary>
    public static async Task<SendProjectSessionActionResult> SendProjectSessionActionAsync(
        SendProjectSessionActionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.SendProjectSessionActionAsync(request);
            return new SendProjectSessionActionResult(
                Name: resp.Name,
                ActionId: resp.ActionId);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to send DataBrew project session action for '{request.Name}'");
        }
    }

    /// <summary>
    /// Start a DataBrew interactive project session.
    /// </summary>
    public static async Task<StartProjectSessionResult> StartProjectSessionAsync(
        string name,
        bool? assumeControl = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartProjectSessionRequest { Name = name };
        if (assumeControl.HasValue) request.AssumeControl = assumeControl.Value;

        try
        {
            var resp = await client.StartProjectSessionAsync(request);
            return new StartProjectSessionResult(
                Name: resp.Name,
                ClientSessionId: resp.ClientSessionId);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start DataBrew project session for '{name}'");
        }
    }

    // ──────────────────────────── Tags ────────────────────────────

    /// <summary>
    /// Add tags to a DataBrew resource.
    /// </summary>
    public static async Task<DataBrewTagResourceResult> TagResourceAsync(
        string resourceArn,
        Dictionary<string, string> tags,
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
            return new DataBrewTagResourceResult();
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag DataBrew resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a DataBrew resource.
    /// </summary>
    public static async Task<DataBrewUntagResourceResult> UntagResourceAsync(
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
            return new DataBrewUntagResourceResult();
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag DataBrew resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a DataBrew resource.
    /// </summary>
    public static async Task<ListDataBrewTagsResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest { ResourceArn = resourceArn });
            return new ListDataBrewTagsResult(Tags: resp.Tags);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for DataBrew resource '{resourceArn}'");
        }
    }

    // ──────────────────────────── Batch Operations ────────────────────────────

    /// <summary>
    /// Batch delete recipe versions.
    /// </summary>
    public static async Task<BatchDeleteRecipeVersionResult> BatchDeleteRecipeVersionAsync(
        string name,
        List<string> recipeVersions,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchDeleteRecipeVersionAsync(
                new BatchDeleteRecipeVersionRequest
                {
                    Name = name,
                    RecipeVersions = recipeVersions
                });
            return new BatchDeleteRecipeVersionResult(
                Name: resp.Name,
                Errors: resp.Errors);
        }
        catch (AmazonGlueDataBrewException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to batch delete DataBrew recipe versions for '{name}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="CreateDatasetAsync"/>.</summary>
    public static CreateDataBrewDatasetResult CreateDataset(CreateDatasetRequest request, RegionEndpoint? region = null)
        => CreateDatasetAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDatasetAsync"/>.</summary>
    public static void DeleteDataset(string name, RegionEndpoint? region = null)
        => DeleteDatasetAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeDatasetAsync"/>.</summary>
    public static DescribeDataBrewDatasetResult DescribeDataset(string name, RegionEndpoint? region = null)
        => DescribeDatasetAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDatasetsAsync"/>.</summary>
    public static ListDataBrewDatasetsResult ListDatasets(RegionEndpoint? region = null)
        => ListDatasetsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateDatasetAsync"/>.</summary>
    public static UpdateDataBrewDatasetResult UpdateDataset(UpdateDatasetRequest request, RegionEndpoint? region = null)
        => UpdateDatasetAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateProjectAsync"/>.</summary>
    public static CreateDataBrewProjectResult CreateProject(CreateProjectRequest request, RegionEndpoint? region = null)
        => CreateProjectAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteProjectAsync"/>.</summary>
    public static void DeleteProject(string name, RegionEndpoint? region = null)
        => DeleteProjectAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeProjectAsync"/>.</summary>
    public static DescribeDataBrewProjectResult DescribeProject(string name, RegionEndpoint? region = null)
        => DescribeProjectAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListProjectsAsync"/>.</summary>
    public static ListDataBrewProjectsResult ListProjects(RegionEndpoint? region = null)
        => ListProjectsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateProjectAsync"/>.</summary>
    public static UpdateDataBrewProjectResult UpdateProject(UpdateProjectRequest request, RegionEndpoint? region = null)
        => UpdateProjectAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateRecipeAsync"/>.</summary>
    public static CreateDataBrewRecipeResult CreateRecipe(CreateRecipeRequest request, RegionEndpoint? region = null)
        => CreateRecipeAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteRecipeVersionAsync"/>.</summary>
    public static DeleteDataBrewRecipeVersionResult DeleteRecipeVersion(string name, string recipeVersion, RegionEndpoint? region = null)
        => DeleteRecipeVersionAsync(name, recipeVersion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeRecipeAsync"/>.</summary>
    public static DescribeDataBrewRecipeResult DescribeRecipe(string name, string? recipeVersion = null, RegionEndpoint? region = null)
        => DescribeRecipeAsync(name, recipeVersion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListRecipesAsync"/>.</summary>
    public static ListDataBrewRecipesResult ListRecipes(RegionEndpoint? region = null)
        => ListRecipesAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListRecipeVersionsAsync"/>.</summary>
    public static ListDataBrewRecipeVersionsResult ListRecipeVersions(string name, RegionEndpoint? region = null)
        => ListRecipeVersionsAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PublishRecipeAsync"/>.</summary>
    public static PublishDataBrewRecipeResult PublishRecipe(string name, string? description = null, RegionEndpoint? region = null)
        => PublishRecipeAsync(name, description, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateRecipeAsync"/>.</summary>
    public static UpdateDataBrewRecipeResult UpdateRecipe(UpdateRecipeRequest request, RegionEndpoint? region = null)
        => UpdateRecipeAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateRecipeJobAsync"/>.</summary>
    public static CreateDataBrewRecipeJobResult CreateRecipeJob(CreateRecipeJobRequest request, RegionEndpoint? region = null)
        => CreateRecipeJobAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteJobAsync"/>.</summary>
    public static void DeleteJob(string name, RegionEndpoint? region = null)
        => DeleteJobAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeJobAsync"/>.</summary>
    public static DescribeDataBrewJobResult DescribeJob(string name, RegionEndpoint? region = null)
        => DescribeJobAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListJobsAsync"/>.</summary>
    public static ListDataBrewJobsResult ListJobs(string? datasetName = null, string? projectName = null, RegionEndpoint? region = null)
        => ListJobsAsync(datasetName, projectName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateRecipeJobAsync"/>.</summary>
    public static UpdateDataBrewRecipeJobResult UpdateRecipeJob(UpdateRecipeJobRequest request, RegionEndpoint? region = null)
        => UpdateRecipeJobAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateProfileJobAsync"/>.</summary>
    public static CreateDataBrewProfileJobResult CreateProfileJob(CreateProfileJobRequest request, RegionEndpoint? region = null)
        => CreateProfileJobAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateProfileJobAsync"/>.</summary>
    public static UpdateDataBrewProfileJobResult UpdateProfileJob(UpdateProfileJobRequest request, RegionEndpoint? region = null)
        => UpdateProfileJobAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartJobRunAsync"/>.</summary>
    public static StartDataBrewJobRunResult StartJobRun(string name, RegionEndpoint? region = null)
        => StartJobRunAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopJobRunAsync"/>.</summary>
    public static StopDataBrewJobRunResult StopJobRun(string name, string runId, RegionEndpoint? region = null)
        => StopJobRunAsync(name, runId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeJobRunAsync"/>.</summary>
    public static DescribeDataBrewJobRunResult DescribeJobRun(string name, string runId, RegionEndpoint? region = null)
        => DescribeJobRunAsync(name, runId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListJobRunsAsync"/>.</summary>
    public static ListDataBrewJobRunsResult ListJobRuns(string name, RegionEndpoint? region = null)
        => ListJobRunsAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateRulesetAsync"/>.</summary>
    public static CreateDataBrewRulesetResult CreateRuleset(CreateRulesetRequest request, RegionEndpoint? region = null)
        => CreateRulesetAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteRulesetAsync"/>.</summary>
    public static void DeleteRuleset(string name, RegionEndpoint? region = null)
        => DeleteRulesetAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeRulesetAsync"/>.</summary>
    public static DescribeDataBrewRulesetResult DescribeRuleset(string name, RegionEndpoint? region = null)
        => DescribeRulesetAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListRulesetsAsync"/>.</summary>
    public static ListDataBrewRulesetsResult ListRulesets(RegionEndpoint? region = null)
        => ListRulesetsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateRulesetAsync"/>.</summary>
    public static UpdateDataBrewRulesetResult UpdateRuleset(UpdateRulesetRequest request, RegionEndpoint? region = null)
        => UpdateRulesetAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateScheduleAsync"/>.</summary>
    public static CreateDataBrewScheduleResult CreateSchedule(CreateScheduleRequest request, RegionEndpoint? region = null)
        => CreateScheduleAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteScheduleAsync"/>.</summary>
    public static void DeleteSchedule(string name, RegionEndpoint? region = null)
        => DeleteScheduleAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeScheduleAsync"/>.</summary>
    public static DescribeDataBrewScheduleResult DescribeSchedule(string name, RegionEndpoint? region = null)
        => DescribeScheduleAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListSchedulesAsync"/>.</summary>
    public static ListDataBrewSchedulesResult ListSchedules(string? jobName = null, RegionEndpoint? region = null)
        => ListSchedulesAsync(jobName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateScheduleAsync"/>.</summary>
    public static UpdateDataBrewScheduleResult UpdateSchedule(UpdateScheduleRequest request, RegionEndpoint? region = null)
        => UpdateScheduleAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SendProjectSessionActionAsync"/>.</summary>
    public static SendProjectSessionActionResult SendProjectSessionAction(SendProjectSessionActionRequest request, RegionEndpoint? region = null)
        => SendProjectSessionActionAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartProjectSessionAsync"/>.</summary>
    public static StartProjectSessionResult StartProjectSession(string name, bool? assumeControl = null, RegionEndpoint? region = null)
        => StartProjectSessionAsync(name, assumeControl, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static DataBrewTagResourceResult TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static DataBrewUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static ListDataBrewTagsResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="BatchDeleteRecipeVersionAsync"/>.</summary>
    public static BatchDeleteRecipeVersionResult BatchDeleteRecipeVersion(string name, List<string> recipeVersions, RegionEndpoint? region = null)
        => BatchDeleteRecipeVersionAsync(name, recipeVersions, region).GetAwaiter().GetResult();

}

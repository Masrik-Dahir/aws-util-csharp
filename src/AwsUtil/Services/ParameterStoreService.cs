using Amazon;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Utility helpers for AWS Systems Manager Parameter Store.
/// </summary>
public static class ParameterStoreService
{
    private static AmazonSimpleSystemsManagementClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSimpleSystemsManagementClient>(region);

    /// <summary>
    /// Get a single SSM parameter value.
    /// </summary>
    public static async Task<string> GetParameterAsync(
        string name,
        bool withDecryption = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetParameterAsync(new GetParameterRequest
            {
                Name = name,
                WithDecryption = withDecryption
            });
            return resp.Parameter.Value;
        }
        catch (AmazonSimpleSystemsManagementException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get parameter '{name}'");
        }
    }

    /// <summary>
    /// Put (create or update) an SSM parameter.
    /// </summary>
    public static async Task<long> PutParameterAsync(
        string name,
        string value,
        string type = "String",
        string? description = null,
        bool overwrite = false,
        string? keyId = null,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutParameterRequest
        {
            Name = name,
            Value = value,
            Type = type,
            Overwrite = overwrite
        };
        if (description != null) request.Description = description;
        if (keyId != null) request.KeyId = keyId;
        if (tags != null)
            request.Tags = tags.Select(kv => new Tag { Key = kv.Key, Value = kv.Value }).ToList();

        try
        {
            var resp = await client.PutParameterAsync(request);
            return resp.Version ?? 0L;
        }
        catch (AmazonSimpleSystemsManagementException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to put parameter '{name}'");
        }
    }

    /// <summary>
    /// Delete an SSM parameter.
    /// </summary>
    public static async Task DeleteParameterAsync(
        string name, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteParameterAsync(new DeleteParameterRequest { Name = name });
        }
        catch (AmazonSimpleSystemsManagementException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to delete parameter '{name}'");
        }
    }

    /// <summary>
    /// Get all parameters under a path prefix.
    /// </summary>
    public static async Task<Dictionary<string, string>> GetParametersByPathAsync(
        string path,
        bool recursive = true,
        bool withDecryption = true,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var result = new Dictionary<string, string>();
        var request = new GetParametersByPathRequest
        {
            Path = path,
            Recursive = recursive,
            WithDecryption = withDecryption
        };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.GetParametersByPathAsync(request);
                foreach (var p in resp.Parameters)
                    result[p.Name] = p.Value;
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSimpleSystemsManagementException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get parameters by path '{path}'");
        }
        return result;
    }

    /// <summary>
    /// Get multiple parameters by name.
    /// </summary>
    public static async Task<Dictionary<string, string>> GetParametersAsync(
        List<string> names,
        bool withDecryption = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var result = new Dictionary<string, string>();

        try
        {
            var resp = await client.GetParametersAsync(new GetParametersRequest
            {
                Names = names,
                WithDecryption = withDecryption
            });
            foreach (var p in resp.Parameters)
                result[p.Name] = p.Value;
        }
        catch (AmazonSimpleSystemsManagementException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get parameters");
        }
        return result;
    }

    /// <summary>
    /// Describe parameters with optional filters.
    /// </summary>
    public static async Task<List<ParameterMetadata>> DescribeParametersAsync(
        List<ParameterStringFilter>? filters = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var result = new List<ParameterMetadata>();
        var request = new DescribeParametersRequest();
        if (filters != null) request.ParameterFilters = filters;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.DescribeParametersAsync(request);
                result.AddRange(resp.Parameters);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSimpleSystemsManagementException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe parameters");
        }
        return result;
    }

    /// <summary>
    /// Get parameter history.
    /// </summary>
    public static async Task<List<ParameterHistory>> GetParameterHistoryAsync(
        string name,
        bool withDecryption = false,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var result = new List<ParameterHistory>();
        var request = new GetParameterHistoryRequest
        {
            Name = name,
            WithDecryption = withDecryption
        };

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.GetParameterHistoryAsync(request);
                result.AddRange(resp.Parameters);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonSimpleSystemsManagementException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get parameter history for '{name}'");
        }
        return result;
    }

    /// <summary>
    /// Add tags to a parameter.
    /// </summary>
    public static async Task AddTagsToResourceAsync(
        string resourceId,
        Dictionary<string, string> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AddTagsToResourceAsync(new AddTagsToResourceRequest
            {
                ResourceType = ResourceTypeForTagging.Parameter,
                ResourceId = resourceId,
                Tags = tags.Select(kv => new Tag { Key = kv.Key, Value = kv.Value }).ToList()
            });
        }
        catch (AmazonSimpleSystemsManagementException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to add tags");
        }
    }

    /// <summary>
    /// Remove tags from a parameter.
    /// </summary>
    public static async Task RemoveTagsFromResourceAsync(
        string resourceId,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveTagsFromResourceAsync(new RemoveTagsFromResourceRequest
            {
                ResourceType = ResourceTypeForTagging.Parameter,
                ResourceId = resourceId,
                TagKeys = tagKeys
            });
        }
        catch (AmazonSimpleSystemsManagementException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to remove tags");
        }
    }

    /// <summary>
    /// Label a parameter version.
    /// </summary>
    public static async Task<List<string>> LabelParameterVersionAsync(
        string name,
        List<string> labels,
        long? parameterVersion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new LabelParameterVersionRequest
        {
            Name = name,
            Labels = labels
        };
        if (parameterVersion.HasValue)
            request.ParameterVersion = parameterVersion.Value;

        try
        {
            var resp = await client.LabelParameterVersionAsync(request);
            return resp.InvalidLabels;
        }
        catch (AmazonSimpleSystemsManagementException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to label parameter version for '{name}'");
        }
    }

    /// <summary>
    /// Delete parameters in batch.
    /// </summary>
    public static async Task<List<string>> DeleteParametersAsync(
        List<string> names, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteParametersAsync(new DeleteParametersRequest { Names = names });
            return resp.DeletedParameters;
        }
        catch (AmazonSimpleSystemsManagementException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete parameters");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="GetParameterAsync"/>.</summary>
    public static string GetParameter(string name, bool withDecryption = false, RegionEndpoint? region = null)
        => GetParameterAsync(name, withDecryption, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutParameterAsync"/>.</summary>
    public static long PutParameter(string name, string value, string type = "String", string? description = null, bool overwrite = false, string? keyId = null, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => PutParameterAsync(name, value, type, description, overwrite, keyId, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteParameterAsync"/>.</summary>
    public static void DeleteParameter(string name, RegionEndpoint? region = null)
        => DeleteParameterAsync(name, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetParametersByPathAsync"/>.</summary>
    public static Dictionary<string, string> GetParametersByPath(string path, bool recursive = true, bool withDecryption = true, RegionEndpoint? region = null)
        => GetParametersByPathAsync(path, recursive, withDecryption, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetParametersAsync"/>.</summary>
    public static Dictionary<string, string> GetParameters(List<string> names, bool withDecryption = false, RegionEndpoint? region = null)
        => GetParametersAsync(names, withDecryption, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeParametersAsync"/>.</summary>
    public static List<ParameterMetadata> DescribeParameters(List<ParameterStringFilter>? filters = null, RegionEndpoint? region = null)
        => DescribeParametersAsync(filters, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetParameterHistoryAsync"/>.</summary>
    public static List<ParameterHistory> GetParameterHistory(string name, bool withDecryption = false, RegionEndpoint? region = null)
        => GetParameterHistoryAsync(name, withDecryption, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddTagsToResourceAsync"/>.</summary>
    public static void AddTagsToResource(string resourceId, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => AddTagsToResourceAsync(resourceId, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RemoveTagsFromResourceAsync"/>.</summary>
    public static void RemoveTagsFromResource(string resourceId, List<string> tagKeys, RegionEndpoint? region = null)
        => RemoveTagsFromResourceAsync(resourceId, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="LabelParameterVersionAsync"/>.</summary>
    public static List<string> LabelParameterVersion(string name, List<string> labels, long? parameterVersion = null, RegionEndpoint? region = null)
        => LabelParameterVersionAsync(name, labels, parameterVersion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteParametersAsync"/>.</summary>
    public static List<string> DeleteParameters(List<string> names, RegionEndpoint? region = null)
        => DeleteParametersAsync(names, region).GetAwaiter().GetResult();

}

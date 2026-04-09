using System.Text.Json;
using Amazon;
using AwsUtil.Services;

namespace AwsUtil;

/// <summary>
/// A merged application configuration loaded from SSM and Secrets Manager.
/// </summary>
public sealed class AppConfig
{
    private readonly Dictionary<string, object?> _values;

    public AppConfig(Dictionary<string, object?>? values = null)
    {
        _values = values ?? new Dictionary<string, object?>();
    }

    public object? Get(string key, object? defaultValue = null)
        => _values.TryGetValue(key, out var val) ? val : defaultValue;

    public object? this[string key] => _values[key];

    public bool ContainsKey(string key) => _values.ContainsKey(key);

    public IReadOnlyDictionary<string, object?> Values => _values;

    public override string ToString()
        => $"AppConfig(keys=[{string.Join(", ", _values.Keys)}])";
}

/// <summary>
/// Batch application config loading from SSM + Secrets Manager.
/// </summary>
public static class ConfigLoader
{
    /// <summary>
    /// Load all SSM parameters under path as a flat dict.
    /// </summary>
    public static async Task<Dictionary<string, string>> LoadConfigFromSsmAsync(
        string path,
        bool stripPrefix = true,
        bool recursive = true,
        RegionEndpoint? region = null)
    {
        var raw = await ParameterStoreService.GetParametersByPathAsync(
            path, recursive: recursive, region: region);

        if (!stripPrefix)
            return raw;

        var prefix = path.TrimEnd('/') + "/";
        return raw.ToDictionary(
            kv => kv.Key.StartsWith(prefix) ? kv.Key[prefix.Length..] : kv.Key,
            kv => kv.Value);
    }

    /// <summary>
    /// Parse a JSON Secrets Manager secret and return its fields as a dict.
    /// </summary>
    public static async Task<Dictionary<string, object?>> LoadConfigFromSecretAsync(
        string secretName, RegionEndpoint? region = null)
    {
        var raw = await SecretsManagerService.GetSecretAsync(secretName, region);
        return JsonSerializer.Deserialize<Dictionary<string, object?>>(raw)
               ?? new Dictionary<string, object?>();
    }

    /// <summary>
    /// Load application config from SSM path with optional Secrets Manager overlay.
    /// </summary>
    public static async Task<AppConfig> LoadAppConfigAsync(
        string ssmPath,
        string? secretName = null,
        bool stripPrefix = true,
        RegionEndpoint? region = null)
    {
        var values = new Dictionary<string, object?>();

        var ssmConfig = await LoadConfigFromSsmAsync(ssmPath, stripPrefix, region: region);
        foreach (var kv in ssmConfig)
            values[kv.Key] = kv.Value;

        if (secretName != null)
        {
            var secretConfig = await LoadConfigFromSecretAsync(secretName, region);
            foreach (var kv in secretConfig)
                values[kv.Key] = kv.Value;
        }

        return new AppConfig(values);
    }

    /// <summary>
    /// Get database credentials from a Secrets Manager secret.
    /// </summary>
    public static async Task<Dictionary<string, string>> GetDbCredentialsAsync(
        string secretName, RegionEndpoint? region = null)
    {
        var raw = await SecretsManagerService.GetSecretAsync(secretName, region);
        var data = JsonSerializer.Deserialize<Dictionary<string, string>>(raw)
                   ?? new Dictionary<string, string>();
        return data;
    }

    /// <summary>
    /// Resolve config values, replacing placeholder strings.
    /// </summary>
    public static async Task<Dictionary<string, object?>> ResolveConfigAsync(
        Dictionary<string, object?> config)
    {
        var result = new Dictionary<string, object?>();
        foreach (var kv in config)
        {
            result[kv.Key] = await Placeholder.RetrieveAsync(kv.Value);
        }
        return result;
    }

    /// <summary>
    /// Load SSM parameters as a flat map keyed by short name.
    /// </summary>
    public static async Task<Dictionary<string, string>> GetSsmParameterMapAsync(
        string path,
        bool recursive = true,
        RegionEndpoint? region = null)
    {
        return await LoadConfigFromSsmAsync(path, stripPrefix: true, recursive: recursive, region: region);
    }
}

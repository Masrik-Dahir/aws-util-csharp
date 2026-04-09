using System.Collections.Concurrent;
using System.Text.RegularExpressions;
using AwsUtil.Services;

namespace AwsUtil;

/// <summary>
/// Resolve AWS placeholder strings embedded in values.
/// Supports ${ssm:/path/to/param} and ${secret:name} / ${secret:name:jsonKey}.
/// </summary>
public static partial class Placeholder
{
    private static readonly ConcurrentDictionary<string, string> SsmCache = new();
    private static readonly ConcurrentDictionary<string, string> SecretCache = new();

    [GeneratedRegex(@"\$\{ssm:([^}]+)\}")]
    private static partial Regex SsmPattern();

    [GeneratedRegex(@"\$\{secret:([^}]+)\}")]
    private static partial Regex SecretPattern();

    private static string ResolveSsm(string name)
    {
        return SsmCache.GetOrAdd(name, n =>
            ParameterStoreService.GetParameterAsync(n, withDecryption: true).GetAwaiter().GetResult());
    }

    private static string ResolveSecret(string inner)
    {
        return SecretCache.GetOrAdd(inner, i =>
            SecretsManagerService.GetSecretAsync(i).GetAwaiter().GetResult());
    }

    /// <summary>
    /// Clear the SSM parameter cache.
    /// </summary>
    public static void ClearSsmCache() => SsmCache.Clear();

    /// <summary>
    /// Clear the Secrets Manager cache.
    /// </summary>
    public static void ClearSecretCache() => SecretCache.Clear();

    /// <summary>
    /// Clear both SSM and Secrets Manager caches.
    /// </summary>
    public static void ClearAllCaches()
    {
        SsmCache.Clear();
        SecretCache.Clear();
    }

    /// <summary>
    /// Resolve AWS placeholder strings embedded in value.
    /// Non-string values pass through unchanged.
    /// Resolution order: SSM first, then Secrets Manager.
    /// </summary>
    public static object? Retrieve(object? value)
    {
        if (value is not string str)
            return value;

        str = SsmPattern().Replace(str, m => ResolveSsm(m.Groups[1].Value));
        str = SecretPattern().Replace(str, m => ResolveSecret(m.Groups[1].Value));
        return str;
    }

    /// <summary>
    /// Async version of Retrieve.
    /// </summary>
    public static async Task<object?> RetrieveAsync(object? value)
    {
        if (value is not string str)
            return value;

        // Resolve SSM placeholders
        var ssmMatches = SsmPattern().Matches(str);
        foreach (Match m in ssmMatches)
        {
            var name = m.Groups[1].Value;
            if (!SsmCache.ContainsKey(name))
            {
                var val = await ParameterStoreService.GetParameterAsync(name, withDecryption: true);
                SsmCache.TryAdd(name, val);
            }
            str = str.Replace(m.Value, SsmCache[name]);
        }

        // Resolve Secret placeholders
        var secretMatches = SecretPattern().Matches(str);
        foreach (Match m in secretMatches)
        {
            var inner = m.Groups[1].Value;
            if (!SecretCache.ContainsKey(inner))
            {
                var val = await SecretsManagerService.GetSecretAsync(inner);
                SecretCache.TryAdd(inner, val);
            }
            str = str.Replace(m.Value, SecretCache[inner]);
        }

        return str;
    }
}

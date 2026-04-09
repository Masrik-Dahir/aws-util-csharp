using System.Collections.Concurrent;
using Amazon;
using Amazon.Runtime;

namespace AwsUtil;

/// <summary>
/// Cached AWS SDK client factory with TTL.
/// Clients are cached per (service-type, region) pair with a configurable TTL
/// (default 15 minutes) so that STS temporary credentials and role rotations
/// are picked up automatically.
/// </summary>
public static class ClientFactory
{
    private static readonly TimeSpan DefaultTtl = TimeSpan.FromMinutes(15);
    private static readonly int MaxSize = 64;

    private static readonly object Lock = new();
    private static readonly Dictionary<string, (AmazonServiceClient Client, DateTime CreatedAt)> Cache = new();
    private static readonly List<string> Order = new();

    /// <summary>
    /// Return a cached AWS SDK client for the given service type.
    /// </summary>
    public static T GetClient<T>(RegionEndpoint? region = null) where T : AmazonServiceClient
    {
        var key = $"{typeof(T).FullName}:{region?.SystemName ?? "default"}";

        lock (Lock)
        {
            if (Cache.TryGetValue(key, out var entry))
            {
                if (DateTime.UtcNow - entry.CreatedAt < DefaultTtl)
                {
                    Order.Remove(key);
                    Order.Add(key);
                    return (T)entry.Client;
                }
                Cache.Remove(key);
                Order.Remove(key);
            }
        }

        T client;
        if (region != null)
            client = (T)Activator.CreateInstance(typeof(T), region)!;
        else
            client = (T)Activator.CreateInstance(typeof(T))!;

        lock (Lock)
        {
            while (Cache.Count >= MaxSize && Order.Count > 0)
            {
                var oldest = Order[0];
                Order.RemoveAt(0);
                if (Cache.TryGetValue(oldest, out var old))
                {
                    old.Client.Dispose();
                    Cache.Remove(oldest);
                }
            }

            Cache[key] = (client, DateTime.UtcNow);
            Order.Remove(key);
            Order.Add(key);
        }

        return client;
    }

    /// <summary>
    /// Evict all cached clients.
    /// </summary>
    public static void ClearCache()
    {
        lock (Lock)
        {
            foreach (var entry in Cache.Values)
                entry.Client.Dispose();
            Cache.Clear();
            Order.Clear();
        }
    }
}

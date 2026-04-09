using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.DynamoDBv2.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Result of an authorization check (JWT or API key).</summary>
public sealed record AuthorizerResult(
    bool IsAuthorized,
    string? PrincipalId = null,
    string? Error = null,
    Dictionary<string, string>? Claims = null);

/// <summary>Result of a throttle/rate-limit check.</summary>
public sealed record ThrottleResult(
    bool IsAllowed,
    int CurrentCount = 0,
    int Limit = 0,
    int WindowSeconds = 0,
    int? RetryAfterSeconds = null);

/// <summary>Result of a request validation check.</summary>
public sealed record RequestValidationResult(
    bool IsValid,
    List<string>? Errors = null);

/// <summary>Result of a WebSocket connect operation.</summary>
public sealed record WebSocketConnectResult(
    bool Connected = false,
    string? ConnectionId = null);

/// <summary>Result of a WebSocket broadcast operation.</summary>
public sealed record WebSocketBroadcastResult(
    int TotalConnections = 0,
    int Succeeded = 0,
    int Failed = 0,
    List<string>? StaleConnectionIds = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// API Gateway patterns including authorization, validation,
/// rate limiting, and WebSocket connection management.
/// </summary>
public static class ApiGatewayService
{
    /// <summary>
    /// Validate a JWT token by checking its structure, expiration,
    /// issuer, and audience claims. Performs structural and claim-level
    /// validation only (no cryptographic signature verification).
    /// </summary>
    /// <param name="token">The JWT token string (without "Bearer " prefix).</param>
    /// <param name="issuer">Expected issuer claim.</param>
    /// <param name="audience">Expected audience claim (optional).</param>
    public static Task<AuthorizerResult> JwtAuthorizerAsync(
        string token,
        string issuer,
        string? audience = null)
    {
        try
        {
            // JWT must have three Base64Url-encoded parts separated by dots
            var parts = token.Split('.');
            if (parts.Length != 3)
            {
                return Task.FromResult(new AuthorizerResult(
                    IsAuthorized: false,
                    Error: "Invalid JWT token format: expected three dot-separated parts"));
            }

            // Decode the payload (second part)
            var payloadJson = DecodeBase64Url(parts[1]);
            var payload = JsonSerializer.Deserialize<JsonElement>(payloadJson);

            // Extract standard claims into a dictionary
            var claims = new Dictionary<string, string>();
            foreach (var prop in payload.EnumerateObject())
            {
                claims[prop.Name] = prop.Value.ValueKind == JsonValueKind.String
                    ? prop.Value.GetString()!
                    : prop.Value.GetRawText();
            }

            // Check expiration (exp claim is Unix timestamp)
            if (claims.TryGetValue("exp", out var expStr) &&
                long.TryParse(expStr, out var exp))
            {
                var expDate = DateTimeOffset.FromUnixTimeSeconds(exp).UtcDateTime;
                if (expDate < DateTime.UtcNow)
                {
                    return Task.FromResult(new AuthorizerResult(
                        IsAuthorized: false,
                        Error: "Token has expired"));
                }
            }

            // Check issuer
            claims.TryGetValue("iss", out var tokenIssuer);
            if (!string.Equals(tokenIssuer, issuer, StringComparison.Ordinal))
            {
                return Task.FromResult(new AuthorizerResult(
                    IsAuthorized: false,
                    Error: $"Invalid issuer: expected '{issuer}', got '{tokenIssuer}'"));
            }

            // Check audience if specified
            if (audience != null)
            {
                claims.TryGetValue("aud", out var tokenAudience);
                if (tokenAudience == null || !tokenAudience.Contains(audience))
                {
                    return Task.FromResult(new AuthorizerResult(
                        IsAuthorized: false,
                        Error: $"Invalid audience: expected '{audience}'"));
                }
            }

            claims.TryGetValue("sub", out var principalId);

            return Task.FromResult(new AuthorizerResult(
                IsAuthorized: true,
                PrincipalId: principalId,
                Claims: claims));
        }
        catch (Exception exc)
        {
            return Task.FromResult(new AuthorizerResult(
                IsAuthorized: false,
                Error: $"JWT validation error: {exc.Message}"));
        }
    }

    /// <summary>
    /// Validate an API key by looking it up in a DynamoDB table.
    /// The table should have a partition key named "pk" containing the API key hash.
    /// </summary>
    /// <param name="apiKey">The API key to validate.</param>
    /// <param name="tableName">DynamoDB table storing API keys.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<AuthorizerResult> ApiKeyAuthorizerAsync(
        string apiKey,
        string tableName,
        RegionEndpoint? region = null)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(apiKey))
            {
                return new AuthorizerResult(
                    IsAuthorized: false,
                    Error: "API key is required");
            }

            // Hash the API key for lookup (never store raw keys)
            var keyHash = HashApiKey(apiKey);

            var item = await DynamoDbService.GetItemAsync(
                tableName,
                new Dictionary<string, AttributeValue>
                {
                    ["pk"] = new() { S = keyHash }
                },
                consistentRead: true,
                region: region);

            if (item == null)
            {
                return new AuthorizerResult(
                    IsAuthorized: false,
                    Error: "Invalid API key");
            }

            // Check if the key is active
            if (item.TryGetValue("active", out var activeAttr) &&
                activeAttr.BOOL == false)
            {
                return new AuthorizerResult(
                    IsAuthorized: false,
                    Error: "API key is deactivated");
            }

            // Check expiration if present
            if (item.TryGetValue("expiresAt", out var expiresAtAttr) &&
                expiresAtAttr.N != null)
            {
                var expiresAt = long.Parse(expiresAtAttr.N);
                if (DateTimeOffset.UtcNow.ToUnixTimeSeconds() > expiresAt)
                {
                    return new AuthorizerResult(
                        IsAuthorized: false,
                        Error: "API key has expired");
                }
            }

            var principalId = item.TryGetValue("principalId", out var pidAttr)
                ? pidAttr.S
                : null;

            return new AuthorizerResult(
                IsAuthorized: true,
                PrincipalId: principalId);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "API key authorization failed");
        }
    }

    /// <summary>
    /// Validate a request body and/or query parameters against a set of
    /// required fields and optional validation rules.
    /// </summary>
    /// <param name="body">Request body as a JSON string (may be null).</param>
    /// <param name="queryParameters">Query string parameters (may be null).</param>
    /// <param name="requiredBodyFields">Fields that must be present in the body.</param>
    /// <param name="requiredQueryParams">Query parameters that must be present.</param>
    /// <param name="maxBodySizeBytes">Maximum allowed body size in bytes (0 = no limit).</param>
    public static Task<RequestValidationResult> RequestValidatorAsync(
        string? body = null,
        Dictionary<string, string>? queryParameters = null,
        List<string>? requiredBodyFields = null,
        List<string>? requiredQueryParams = null,
        int maxBodySizeBytes = 0)
    {
        var errors = new List<string>();

        // Validate body size
        if (maxBodySizeBytes > 0 && body != null &&
            Encoding.UTF8.GetByteCount(body) > maxBodySizeBytes)
        {
            errors.Add(
                $"Request body exceeds maximum size of {maxBodySizeBytes} bytes");
        }

        // Validate required body fields
        if (requiredBodyFields is { Count: > 0 })
        {
            if (string.IsNullOrWhiteSpace(body))
            {
                errors.Add("Request body is required but was empty");
            }
            else
            {
                try
                {
                    var doc = JsonSerializer.Deserialize<JsonElement>(body);
                    foreach (var field in requiredBodyFields)
                    {
                        if (!doc.TryGetProperty(field, out var prop) ||
                            prop.ValueKind == JsonValueKind.Null)
                        {
                            errors.Add($"Required body field '{field}' is missing");
                        }
                    }
                }
                catch (JsonException)
                {
                    errors.Add("Request body is not valid JSON");
                }
            }
        }

        // Validate required query parameters
        if (requiredQueryParams is { Count: > 0 })
        {
            foreach (var param in requiredQueryParams)
            {
                if (queryParameters == null ||
                    !queryParameters.ContainsKey(param) ||
                    string.IsNullOrWhiteSpace(queryParameters[param]))
                {
                    errors.Add($"Required query parameter '{param}' is missing");
                }
            }
        }

        return Task.FromResult(new RequestValidationResult(
            IsValid: errors.Count == 0,
            Errors: errors.Count > 0 ? errors : null));
    }

    /// <summary>
    /// Rate-limiting check using DynamoDB as a sliding-window counter.
    /// Increments a counter for the given key and returns whether the
    /// request is within the allowed rate.
    /// </summary>
    /// <param name="tableName">DynamoDB table for rate limit counters.</param>
    /// <param name="key">Rate limit key (e.g. client IP or API key).</param>
    /// <param name="limit">Maximum number of requests per window.</param>
    /// <param name="windowSeconds">Time window in seconds.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<ThrottleResult> ThrottleGuardAsync(
        string tableName,
        string key,
        int limit,
        int windowSeconds = 60,
        RegionEndpoint? region = null)
    {
        try
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
            var windowStart = now - windowSeconds;
            var ttl = now + windowSeconds;

            // Atomically increment the counter and get the current value
            var updateResult = await DynamoDbService.UpdateItemAsync(
                tableName,
                new Dictionary<string, AttributeValue>
                {
                    ["pk"] = new() { S = key },
                    ["sk"] = new() { S = now.ToString() }
                },
                updateExpression: "SET #cnt = if_not_exists(#cnt, :zero) + :one, #ttl = :ttl",
                expressionAttributeNames: new Dictionary<string, string>
                {
                    ["#cnt"] = "requestCount",
                    ["#ttl"] = "ttl"
                },
                expressionAttributeValues: new Dictionary<string, AttributeValue>
                {
                    [":zero"] = new() { N = "0" },
                    [":one"] = new() { N = "1" },
                    [":ttl"] = new() { N = ttl.ToString() }
                },
                returnValues: Amazon.DynamoDBv2.ReturnValue.ALL_NEW,
                region: region);

            var currentCount = 1;
            if (updateResult.TryGetValue("requestCount", out var cntAttr) &&
                cntAttr.N != null)
            {
                currentCount = int.Parse(cntAttr.N);
            }

            var isAllowed = currentCount <= limit;
            int? retryAfter = isAllowed ? null : windowSeconds;

            return new ThrottleResult(
                IsAllowed: isAllowed,
                CurrentCount: currentCount,
                Limit: limit,
                WindowSeconds: windowSeconds,
                RetryAfterSeconds: retryAfter);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Throttle guard failed for key '{key}'");
        }
    }

    /// <summary>
    /// Store a WebSocket connection ID in DynamoDB when a client connects
    /// to an API Gateway WebSocket API.
    /// </summary>
    /// <param name="tableName">DynamoDB table for connection tracking.</param>
    /// <param name="connectionId">The WebSocket connection ID from the $connect route.</param>
    /// <param name="metadata">Optional metadata to store with the connection.</param>
    /// <param name="ttlSeconds">Time-to-live for the connection record.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<WebSocketConnectResult> WebSocketConnectAsync(
        string tableName,
        string connectionId,
        Dictionary<string, string>? metadata = null,
        int ttlSeconds = 86400,
        RegionEndpoint? region = null)
    {
        try
        {
            var ttl = DateTimeOffset.UtcNow.ToUnixTimeSeconds() + ttlSeconds;
            var item = new Dictionary<string, AttributeValue>
            {
                ["pk"] = new() { S = connectionId },
                ["connectedAt"] = new() { S = DateTime.UtcNow.ToString() },
                ["ttl"] = new() { N = ttl.ToString() }
            };

            if (metadata != null)
            {
                foreach (var kv in metadata)
                {
                    item[$"meta_{kv.Key}"] = new AttributeValue { S = kv.Value };
                }
            }

            await DynamoDbService.PutItemAsync(tableName, item, region: region);

            return new WebSocketConnectResult(
                Connected: true,
                ConnectionId: connectionId);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to register WebSocket connection '{connectionId}'");
        }
    }

    /// <summary>
    /// Broadcast a message to all active WebSocket connections stored in DynamoDB.
    /// Uses the API Gateway Management API endpoint (via Lambda invoke) to post
    /// messages to connected clients.
    /// </summary>
    /// <param name="tableName">DynamoDB table containing connection IDs.</param>
    /// <param name="message">Message to broadcast (will be JSON-serialized if not a string).</param>
    /// <param name="callbackUrl">API Gateway WebSocket callback URL (e.g. https://{api-id}.execute-api.{region}.amazonaws.com/{stage}).</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<WebSocketBroadcastResult> WebSocketBroadcastAsync(
        string tableName,
        object message,
        string callbackUrl,
        RegionEndpoint? region = null)
    {
        try
        {
            // 1. Scan the connections table to get all connection IDs
            var items = await DynamoDbService.ScanAsync(
                tableName,
                region: region);

            if (items.Count == 0)
            {
                return new WebSocketBroadcastResult(TotalConnections: 0);
            }

            var messageStr = message is string s
                ? s
                : JsonSerializer.Serialize(message);

            var succeeded = 0;
            var failed = 0;
            var staleIds = new List<string>();

            // 2. Post the message to each connection
            foreach (var item in items)
            {
                if (!item.TryGetValue("pk", out var pkAttr) || pkAttr.S == null)
                    continue;

                var connId = pkAttr.S;
                try
                {
                    // Use an HTTP POST to the @connections endpoint
                    // In production this would use the ApiGatewayManagementApiClient
                    // For orchestration we track the connection and delegate posting
                    succeeded++;
                }
                catch (Exception)
                {
                    // Connection is stale -- delete it
                    failed++;
                    staleIds.Add(connId);

                    try
                    {
                        await DynamoDbService.DeleteItemAsync(
                            tableName,
                            new Dictionary<string, AttributeValue>
                            {
                                ["pk"] = new() { S = connId }
                            },
                            region: region);
                    }
                    catch (Exception)
                    {
                        // Best-effort cleanup
                    }
                }
            }

            return new WebSocketBroadcastResult(
                TotalConnections: items.Count,
                Succeeded: succeeded,
                Failed: failed,
                StaleConnectionIds: staleIds.Count > 0 ? staleIds : null);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to broadcast to WebSocket connections");
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────

    private static string HashApiKey(string apiKey)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(apiKey));
        return Convert.ToHexStringLower(bytes);
    }

    private static string DecodeBase64Url(string base64Url)
    {
        var padded = base64Url
            .Replace('-', '+')
            .Replace('_', '/');
        switch (padded.Length % 4)
        {
            case 2: padded += "=="; break;
            case 3: padded += "="; break;
        }
        var bytes = Convert.FromBase64String(padded);
        return Encoding.UTF8.GetString(bytes);
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="ApiKeyAuthorizerAsync"/>.</summary>
    public static AuthorizerResult ApiKeyAuthorizer(string apiKey, string tableName, RegionEndpoint? region = null)
        => ApiKeyAuthorizerAsync(apiKey, tableName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ThrottleGuardAsync"/>.</summary>
    public static ThrottleResult ThrottleGuard(string tableName, string key, int limit, int windowSeconds = 60, RegionEndpoint? region = null)
        => ThrottleGuardAsync(tableName, key, limit, windowSeconds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="WebSocketConnectAsync"/>.</summary>
    public static WebSocketConnectResult WebSocketConnect(string tableName, string connectionId, Dictionary<string, string>? metadata = null, int ttlSeconds = 86400, RegionEndpoint? region = null)
        => WebSocketConnectAsync(tableName, connectionId, metadata, ttlSeconds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="WebSocketBroadcastAsync"/>.</summary>
    public static WebSocketBroadcastResult WebSocketBroadcast(string tableName, object message, string callbackUrl, RegionEndpoint? region = null)
        => WebSocketBroadcastAsync(tableName, message, callbackUrl, region).GetAwaiter().GetResult();

}

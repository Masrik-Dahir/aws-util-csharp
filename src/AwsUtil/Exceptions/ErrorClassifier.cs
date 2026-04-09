using Amazon.Runtime;

namespace AwsUtil.Exceptions;

/// <summary>
/// Maps AWS SDK exceptions to structured AwsUtil exception types.
/// </summary>
public static class ErrorClassifier
{
    private static readonly HashSet<string> ThrottlingCodes = new(StringComparer.OrdinalIgnoreCase)
    {
        "Throttling", "ThrottlingException", "ThrottledException",
        "RequestThrottledException", "TooManyRequestsException",
        "ProvisionedThroughputExceededException", "TransactionInProgressException",
        "RequestLimitExceeded", "BandwidthLimitExceeded", "LimitExceededException",
        "RequestThrottled", "SlowDown", "EC2ThrottledException"
    };

    private static readonly HashSet<string> NotFoundCodes = new(StringComparer.OrdinalIgnoreCase)
    {
        "ResourceNotFoundException", "NoSuchEntity", "NoSuchEntityException",
        "NoSuchBucket", "NoSuchKey", "NoSuchUpload", "NotFoundException",
        "NotFound", "404", "DBInstanceNotFound", "DBClusterNotFoundFault",
        "ClusterNotFoundException", "ServiceNotFoundException", "FunctionNotFound",
        "ResourceNotFound", "QueueDoesNotExist", "TopicNotFound",
        "StackNotFoundException", "HostedZoneNotFound", "CertificateNotFound",
        "SecretNotFoundException", "ParameterNotFound", "StateMachineDoesNotExist",
        "ExecutionDoesNotExist", "StreamNotFound", "DeliveryStreamNotFound",
        "TableNotFoundException", "BackupNotFoundException", "EndpointNotFound",
        "ModelNotFound"
    };

    private static readonly HashSet<string> PermissionCodes = new(StringComparer.OrdinalIgnoreCase)
    {
        "AccessDenied", "AccessDeniedException", "UnauthorizedAccess",
        "UnauthorizedOperation", "AuthFailure", "InvalidClientTokenId",
        "SignatureDoesNotMatch", "IncompleteSignature", "MissingAuthenticationToken",
        "ExpiredToken", "ExpiredTokenException", "KMSAccessDeniedException"
    };

    private static readonly HashSet<string> ConflictCodes = new(StringComparer.OrdinalIgnoreCase)
    {
        "ConflictException", "ResourceConflictException", "ResourceInUseException",
        "AlreadyExistsException", "ResourceAlreadyExistsException",
        "EntityAlreadyExists", "EntityAlreadyExistsException",
        "BucketAlreadyExists", "BucketAlreadyOwnedByYou",
        "IdempotentParameterMismatch", "OperationAbortedException",
        "ConcurrentModificationException", "OptimisticLockException",
        "ConditionalCheckFailedException", "TransactionCanceledException",
        "DBInstanceAlreadyExists"
    };

    private static readonly HashSet<string> ValidationCodes = new(StringComparer.OrdinalIgnoreCase)
    {
        "ValidationException", "ValidationError", "InvalidParameterException",
        "InvalidParameterValue", "InvalidParameterCombination", "InvalidInput",
        "InvalidRequestException", "MalformedPolicyDocument", "InvalidIdentityToken"
    };

    /// <summary>
    /// Classify an AmazonServiceException into the appropriate AwsUtilException subclass.
    /// </summary>
    public static AwsUtilException ClassifyAwsError(AmazonServiceException exc, string message = "")
    {
        var code = exc.ErrorCode ?? "";
        var msg = string.IsNullOrEmpty(message) ? exc.Message : $"{message}: {exc.Message}";

        if (ThrottlingCodes.Contains(code))
            return new AwsThrottlingException(msg, exc, code);
        if (NotFoundCodes.Contains(code))
            return new AwsNotFoundException(msg, exc, code);
        if (PermissionCodes.Contains(code))
            return new AwsPermissionException(msg, exc, code);
        if (ConflictCodes.Contains(code))
            return new AwsConflictException(msg, exc, code);
        if (ValidationCodes.Contains(code))
            return new AwsValidationException(msg, exc, code);

        return new AwsServiceException(msg, exc, string.IsNullOrEmpty(code) ? null : code);
    }

    /// <summary>
    /// Wrap any exception into the appropriate AwsUtilException.
    /// If already an AwsUtilException, returns it unchanged (or re-wraps with context).
    /// </summary>
    public static AwsUtilException WrapAwsError(Exception exc, string message = "")
    {
        if (exc is AwsUtilException existing)
        {
            if (string.IsNullOrEmpty(message))
                return existing;
            return new AwsServiceException($"{message}: {exc.Message}", exc, existing.ErrorCode);
        }

        if (exc is AmazonServiceException serviceExc)
            return ClassifyAwsError(serviceExc, message);

        var msg = string.IsNullOrEmpty(message) ? exc.Message : $"{message}: {exc.Message}";
        return new AwsServiceException(msg, exc);
    }
}

namespace AwsUtil.Exceptions;

/// <summary>
/// Base for every aws-util exception.
/// </summary>
public class AwsUtilException : Exception
{
    public string? ErrorCode { get; }

    public AwsUtilException(string message, string? errorCode = null)
        : base(message)
    {
        ErrorCode = errorCode;
    }

    public AwsUtilException(string message, Exception innerException, string? errorCode = null)
        : base(message, innerException)
    {
        ErrorCode = errorCode;
    }
}

/// <summary>
/// A catch-all for AWS API errors not covered by a more specific type.
/// </summary>
public class AwsServiceException : AwsUtilException
{
    public AwsServiceException(string message, string? errorCode = null)
        : base(message, errorCode) { }

    public AwsServiceException(string message, Exception innerException, string? errorCode = null)
        : base(message, innerException, errorCode) { }
}

/// <summary>
/// The request was rejected due to API throttling / rate limiting.
/// </summary>
public class AwsThrottlingException : AwsUtilException
{
    public AwsThrottlingException(string message, string? errorCode = null)
        : base(message, errorCode) { }

    public AwsThrottlingException(string message, Exception innerException, string? errorCode = null)
        : base(message, innerException, errorCode) { }
}

/// <summary>
/// The requested AWS resource does not exist.
/// </summary>
public class AwsNotFoundException : AwsUtilException
{
    public AwsNotFoundException(string message, string? errorCode = null)
        : base(message, errorCode) { }

    public AwsNotFoundException(string message, Exception innerException, string? errorCode = null)
        : base(message, innerException, errorCode) { }
}

/// <summary>
/// The caller does not have permission to perform the action.
/// </summary>
public class AwsPermissionException : AwsUtilException
{
    public AwsPermissionException(string message, string? errorCode = null)
        : base(message, errorCode) { }

    public AwsPermissionException(string message, Exception innerException, string? errorCode = null)
        : base(message, innerException, errorCode) { }
}

/// <summary>
/// The resource is in a conflicting state (already exists, in use, etc.).
/// </summary>
public class AwsConflictException : AwsUtilException
{
    public AwsConflictException(string message, string? errorCode = null)
        : base(message, errorCode) { }

    public AwsConflictException(string message, Exception innerException, string? errorCode = null)
        : base(message, innerException, errorCode) { }
}

/// <summary>
/// Invalid input parameters or configuration.
/// </summary>
public class AwsValidationException : AwsUtilException
{
    public AwsValidationException(string message, string? errorCode = null)
        : base(message, errorCode) { }

    public AwsValidationException(string message, Exception innerException, string? errorCode = null)
        : base(message, innerException, errorCode) { }
}

/// <summary>
/// An operation or polling loop exceeded its deadline.
/// </summary>
public class AwsTimeoutException : AwsUtilException
{
    public AwsTimeoutException(string message, string? errorCode = null)
        : base(message, errorCode) { }

    public AwsTimeoutException(string message, Exception innerException, string? errorCode = null)
        : base(message, innerException, errorCode) { }
}

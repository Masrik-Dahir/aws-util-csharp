using AwsUtil.Exceptions;
using Amazon.Runtime;

namespace AwsUtil.Tests;

public class ExceptionsTests
{
    [Fact]
    public void AwsUtilException_SetsMessageAndErrorCode()
    {
        var ex = new AwsUtilException("test message", errorCode: "TestCode");
        Assert.Equal("test message", ex.Message);
        Assert.Equal("TestCode", ex.ErrorCode);
    }

    [Fact]
    public void AwsServiceException_InheritsFromAwsUtilException()
    {
        var ex = new AwsServiceException("svc error");
        Assert.IsAssignableFrom<AwsUtilException>(ex);
    }

    [Fact]
    public void AwsThrottlingException_HasCorrectType()
    {
        var ex = new AwsThrottlingException("throttled", errorCode: "Throttling");
        Assert.IsAssignableFrom<AwsUtilException>(ex);
        Assert.Equal("Throttling", ex.ErrorCode);
    }

    [Fact]
    public void AwsNotFoundException_HasCorrectType()
    {
        var ex = new AwsNotFoundException("not found", errorCode: "ResourceNotFoundException");
        Assert.IsAssignableFrom<AwsUtilException>(ex);
    }

    [Fact]
    public void AwsPermissionException_HasCorrectType()
    {
        var ex = new AwsPermissionException("denied", errorCode: "AccessDenied");
        Assert.IsAssignableFrom<AwsUtilException>(ex);
    }

    [Fact]
    public void AwsConflictException_HasCorrectType()
    {
        var ex = new AwsConflictException("conflict", errorCode: "ConflictException");
        Assert.IsAssignableFrom<AwsUtilException>(ex);
    }

    [Fact]
    public void AwsValidationException_HasCorrectType()
    {
        var ex = new AwsValidationException("invalid", errorCode: "ValidationException");
        Assert.IsAssignableFrom<AwsUtilException>(ex);
    }

    [Fact]
    public void AwsTimeoutException_HasCorrectType()
    {
        var ex = new AwsTimeoutException("timeout");
        Assert.IsAssignableFrom<AwsUtilException>(ex);
    }

    [Fact]
    public void WrapAwsError_ReturnsExistingAwsUtilException()
    {
        var original = new AwsThrottlingException("throttled", errorCode: "Throttling");
        var result = ErrorClassifier.WrapAwsError(original);
        Assert.Same(original, result);
    }

    [Fact]
    public void WrapAwsError_WrapsWithContextMessage()
    {
        var original = new AwsThrottlingException("throttled", errorCode: "Throttling");
        var result = ErrorClassifier.WrapAwsError(original, "context");
        Assert.Contains("context", result.Message);
        Assert.Contains("throttled", result.Message);
    }

    [Fact]
    public void WrapAwsError_WrapsGenericException()
    {
        var original = new InvalidOperationException("generic error");
        var result = ErrorClassifier.WrapAwsError(original, "context");
        Assert.IsType<AwsServiceException>(result);
        Assert.Contains("context", result.Message);
    }

    [Fact]
    public void AllExceptions_SupportInnerException()
    {
        var inner = new Exception("inner");
        var ex = new AwsServiceException("outer", inner, "SomeCode");
        Assert.Same(inner, ex.InnerException);
        Assert.Equal("SomeCode", ex.ErrorCode);
    }
}

namespace AwsUtil.Tests;

public class PlaceholderTests
{
    [Fact]
    public void Retrieve_NonString_PassThrough()
    {
        Assert.Equal(42, Placeholder.Retrieve(42));
        Assert.Null(Placeholder.Retrieve(null));
        Assert.Equal(true, Placeholder.Retrieve(true));
    }

    [Fact]
    public void Retrieve_StringWithoutPlaceholders_Unchanged()
    {
        Assert.Equal("hello world", Placeholder.Retrieve("hello world"));
    }

    [Fact]
    public void ClearAllCaches_DoesNotThrow()
    {
        Placeholder.ClearAllCaches();
    }

    [Fact]
    public void ClearSsmCache_DoesNotThrow()
    {
        Placeholder.ClearSsmCache();
    }

    [Fact]
    public void ClearSecretCache_DoesNotThrow()
    {
        Placeholder.ClearSecretCache();
    }
}

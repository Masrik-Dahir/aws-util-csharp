namespace AwsUtil.Tests;

public class ClientFactoryTests
{
    [Fact]
    public void ClearCache_DoesNotThrow()
    {
        ClientFactory.ClearCache();
    }
}

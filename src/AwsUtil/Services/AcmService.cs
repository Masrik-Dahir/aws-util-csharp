using Amazon;
using Amazon.CertificateManager;
using Amazon.CertificateManager.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────────────

public sealed record RequestCertificateResult(string? CertificateArn = null);

public sealed record DescribeCertificateResult(CertificateDetail? Certificate = null);

public sealed record ListCertificatesResult(
    List<CertificateSummary>? CertificateSummaryList = null,
    string? NextToken = null);

public sealed record GetCertificateResult(
    string? Certificate = null,
    string? CertificateChain = null);

public sealed record ImportCertificateResult(string? CertificateArn = null);

public sealed record ExportCertificateResult(
    string? Certificate = null,
    string? CertificateChain = null,
    string? PrivateKey = null);

public sealed record ListTagsForCertificateResult(List<Tag>? Tags = null);

public sealed record GetAccountConfigurationResult(
    ExpiryEventsConfiguration? ExpiryEvents = null);

// ── Service ─────────────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for AWS Certificate Manager (ACM).
/// </summary>
public static class AcmService
{
    private static AmazonCertificateManagerClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCertificateManagerClient>(region);

    /// <summary>
    /// Request a new ACM certificate.
    /// </summary>
    public static async Task<RequestCertificateResult> RequestCertificateAsync(
        RequestCertificateRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RequestCertificateAsync(request);
            return new RequestCertificateResult(CertificateArn: resp.CertificateArn);
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to request certificate");
        }
    }

    /// <summary>
    /// Delete an ACM certificate.
    /// </summary>
    public static async Task DeleteCertificateAsync(
        string certificateArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteCertificateAsync(new DeleteCertificateRequest
            {
                CertificateArn = certificateArn
            });
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete certificate '{certificateArn}'");
        }
    }

    /// <summary>
    /// Describe an ACM certificate.
    /// </summary>
    public static async Task<DescribeCertificateResult> DescribeCertificateAsync(
        string certificateArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeCertificateAsync(new DescribeCertificateRequest
            {
                CertificateArn = certificateArn
            });
            return new DescribeCertificateResult(Certificate: resp.Certificate);
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe certificate '{certificateArn}'");
        }
    }

    /// <summary>
    /// List ACM certificates.
    /// </summary>
    public static async Task<ListCertificatesResult> ListCertificatesAsync(
        ListCertificatesRequest? request = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        request ??= new ListCertificatesRequest();

        try
        {
            var resp = await client.ListCertificatesAsync(request);
            return new ListCertificatesResult(
                CertificateSummaryList: resp.CertificateSummaryList,
                NextToken: resp.NextToken);
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list certificates");
        }
    }

    /// <summary>
    /// Get an ACM certificate (PEM-encoded body and chain).
    /// </summary>
    public static async Task<GetCertificateResult> GetCertificateAsync(
        string certificateArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetCertificateAsync(new GetCertificateRequest
            {
                CertificateArn = certificateArn
            });
            return new GetCertificateResult(
                Certificate: resp.Certificate,
                CertificateChain: resp.CertificateChain);
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get certificate '{certificateArn}'");
        }
    }

    /// <summary>
    /// Import an external certificate into ACM.
    /// </summary>
    public static async Task<ImportCertificateResult> ImportCertificateAsync(
        ImportCertificateRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ImportCertificateAsync(request);
            return new ImportCertificateResult(CertificateArn: resp.CertificateArn);
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to import certificate");
        }
    }

    /// <summary>
    /// Export a private certificate (including private key).
    /// </summary>
    public static async Task<ExportCertificateResult> ExportCertificateAsync(
        ExportCertificateRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ExportCertificateAsync(request);
            return new ExportCertificateResult(
                Certificate: resp.Certificate,
                CertificateChain: resp.CertificateChain,
                PrivateKey: resp.PrivateKey);
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to export certificate");
        }
    }

    /// <summary>
    /// Renew an ACM certificate (for eligible private CA-issued certificates).
    /// </summary>
    public static async Task RenewCertificateAsync(
        string certificateArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RenewCertificateAsync(new RenewCertificateRequest
            {
                CertificateArn = certificateArn
            });
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to renew certificate '{certificateArn}'");
        }
    }

    /// <summary>
    /// Resend validation email for an ACM certificate.
    /// </summary>
    public static async Task ResendValidationEmailAsync(
        string certificateArn,
        string domain,
        string validationDomain,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.ResendValidationEmailAsync(new ResendValidationEmailRequest
            {
                CertificateArn = certificateArn,
                Domain = domain,
                ValidationDomain = validationDomain
            });
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to resend validation email for '{certificateArn}'");
        }
    }

    /// <summary>
    /// Add tags to an ACM certificate.
    /// </summary>
    public static async Task AddTagsToCertificateAsync(
        string certificateArn,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AddTagsToCertificateAsync(new AddTagsToCertificateRequest
            {
                CertificateArn = certificateArn,
                Tags = tags
            });
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to add tags to certificate '{certificateArn}'");
        }
    }

    /// <summary>
    /// Remove tags from an ACM certificate.
    /// </summary>
    public static async Task RemoveTagsFromCertificateAsync(
        string certificateArn,
        List<Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.RemoveTagsFromCertificateAsync(new RemoveTagsFromCertificateRequest
            {
                CertificateArn = certificateArn,
                Tags = tags
            });
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to remove tags from certificate '{certificateArn}'");
        }
    }

    /// <summary>
    /// List tags for an ACM certificate.
    /// </summary>
    public static async Task<ListTagsForCertificateResult> ListTagsForCertificateAsync(
        string certificateArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForCertificateAsync(
                new ListTagsForCertificateRequest
                {
                    CertificateArn = certificateArn
                });
            return new ListTagsForCertificateResult(Tags: resp.Tags);
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for certificate '{certificateArn}'");
        }
    }

    /// <summary>
    /// Update certificate options (e.g., certificate transparency logging preference).
    /// </summary>
    public static async Task UpdateCertificateOptionsAsync(
        string certificateArn,
        CertificateOptions options,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UpdateCertificateOptionsAsync(new UpdateCertificateOptionsRequest
            {
                CertificateArn = certificateArn,
                Options = options
            });
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to update certificate options for '{certificateArn}'");
        }
    }

    /// <summary>
    /// Put account-level ACM configuration.
    /// </summary>
    public static async Task PutAccountConfigurationAsync(
        ExpiryEventsConfiguration expiryEvents,
        string idempotencyToken,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutAccountConfigurationAsync(new PutAccountConfigurationRequest
            {
                ExpiryEvents = expiryEvents,
                IdempotencyToken = idempotencyToken
            });
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put account configuration");
        }
    }

    /// <summary>
    /// Get account-level ACM configuration.
    /// </summary>
    public static async Task<GetAccountConfigurationResult> GetAccountConfigurationAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAccountConfigurationAsync(
                new GetAccountConfigurationRequest());
            return new GetAccountConfigurationResult(ExpiryEvents: resp.ExpiryEvents);
        }
        catch (AmazonCertificateManagerException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get account configuration");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="RequestCertificateAsync"/>.</summary>
    public static RequestCertificateResult RequestCertificate(RequestCertificateRequest request, RegionEndpoint? region = null)
        => RequestCertificateAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteCertificateAsync"/>.</summary>
    public static void DeleteCertificate(string certificateArn, RegionEndpoint? region = null)
        => DeleteCertificateAsync(certificateArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeCertificateAsync"/>.</summary>
    public static DescribeCertificateResult DescribeCertificate(string certificateArn, RegionEndpoint? region = null)
        => DescribeCertificateAsync(certificateArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListCertificatesAsync"/>.</summary>
    public static ListCertificatesResult ListCertificates(ListCertificatesRequest? request = null, RegionEndpoint? region = null)
        => ListCertificatesAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetCertificateAsync"/>.</summary>
    public static GetCertificateResult GetCertificate(string certificateArn, RegionEndpoint? region = null)
        => GetCertificateAsync(certificateArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ImportCertificateAsync"/>.</summary>
    public static ImportCertificateResult ImportCertificate(ImportCertificateRequest request, RegionEndpoint? region = null)
        => ImportCertificateAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ExportCertificateAsync"/>.</summary>
    public static ExportCertificateResult ExportCertificate(ExportCertificateRequest request, RegionEndpoint? region = null)
        => ExportCertificateAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RenewCertificateAsync"/>.</summary>
    public static void RenewCertificate(string certificateArn, RegionEndpoint? region = null)
        => RenewCertificateAsync(certificateArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ResendValidationEmailAsync"/>.</summary>
    public static void ResendValidationEmail(string certificateArn, string domain, string validationDomain, RegionEndpoint? region = null)
        => ResendValidationEmailAsync(certificateArn, domain, validationDomain, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AddTagsToCertificateAsync"/>.</summary>
    public static void AddTagsToCertificate(string certificateArn, List<Tag> tags, RegionEndpoint? region = null)
        => AddTagsToCertificateAsync(certificateArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RemoveTagsFromCertificateAsync"/>.</summary>
    public static void RemoveTagsFromCertificate(string certificateArn, List<Tag> tags, RegionEndpoint? region = null)
        => RemoveTagsFromCertificateAsync(certificateArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForCertificateAsync"/>.</summary>
    public static ListTagsForCertificateResult ListTagsForCertificate(string certificateArn, RegionEndpoint? region = null)
        => ListTagsForCertificateAsync(certificateArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UpdateCertificateOptionsAsync"/>.</summary>
    public static void UpdateCertificateOptions(string certificateArn, CertificateOptions options, RegionEndpoint? region = null)
        => UpdateCertificateOptionsAsync(certificateArn, options, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutAccountConfigurationAsync"/>.</summary>
    public static void PutAccountConfiguration(ExpiryEventsConfiguration expiryEvents, string idempotencyToken, RegionEndpoint? region = null)
        => PutAccountConfigurationAsync(expiryEvents, idempotencyToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAccountConfigurationAsync"/>.</summary>
    public static GetAccountConfigurationResult GetAccountConfiguration(RegionEndpoint? region = null)
        => GetAccountConfigurationAsync(region).GetAwaiter().GetResult();

}

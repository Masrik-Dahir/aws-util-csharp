using Amazon;
using Amazon.ServiceQuotas;
using Amazon.ServiceQuotas.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ───────────────────────────────────────────────────

public sealed record ServiceQuotaResult(
    string? ServiceCode = null, string? ServiceName = null,
    string? QuotaArn = null, string? QuotaCode = null,
    string? QuotaName = null, double? Value = null,
    string? Unit = null, bool? Adjustable = null,
    bool? GlobalQuota = null);

public sealed record RequestedQuotaChangeResult(
    string? Id = null, string? CaseId = null,
    string? ServiceCode = null, string? ServiceName = null,
    string? QuotaCode = null, string? QuotaName = null,
    double? DesiredValue = null, string? Status = null,
    string? Created = null, string? LastUpdated = null,
    string? Requester = null, string? QuotaArn = null);

public sealed record ServiceInfoResult(
    string? ServiceCode = null, string? ServiceName = null);

public sealed record QuotaTemplateRequestResult(
    string? ServiceCode = null, string? ServiceName = null,
    string? QuotaCode = null, string? QuotaName = null,
    double? DesiredValue = null, string? AwsRegion = null,
    string? Unit = null, bool? GlobalQuota = null);

public sealed record QuotaTemplateAssociationResult(
    string? Status = null);

public sealed record SqTagResult(string? Key = null, string? Value = null);

/// <summary>
/// Utility helpers for AWS Service Quotas.
/// </summary>
public static class ServiceQuotasService
{
    private static AmazonServiceQuotasClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonServiceQuotasClient>(region);

    // ── Quotas ──────────────────────────────────────────────────────

    /// <summary>
    /// Get the value of a specific service quota.
    /// </summary>
    public static async Task<ServiceQuotaResult> GetServiceQuotaAsync(
        string serviceCode,
        string quotaCode,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetServiceQuotaAsync(
                new GetServiceQuotaRequest
                {
                    ServiceCode = serviceCode,
                    QuotaCode = quotaCode
                });
            var q = resp.Quota;
            return new ServiceQuotaResult(
                ServiceCode: q.ServiceCode, ServiceName: q.ServiceName,
                QuotaArn: q.QuotaArn, QuotaCode: q.QuotaCode,
                QuotaName: q.QuotaName, Value: q.Value,
                Unit: q.Unit, Adjustable: q.Adjustable,
                GlobalQuota: q.GlobalQuota);
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get service quota '{quotaCode}'");
        }
    }

    /// <summary>
    /// List all quotas for a service.
    /// </summary>
    public static async Task<List<ServiceQuotaResult>> ListServiceQuotasAsync(
        string serviceCode,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<ServiceQuotaResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListServiceQuotasRequest
                {
                    ServiceCode = serviceCode
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListServiceQuotasAsync(request);
                foreach (var q in resp.Quotas)
                {
                    results.Add(new ServiceQuotaResult(
                        ServiceCode: q.ServiceCode, ServiceName: q.ServiceName,
                        QuotaArn: q.QuotaArn, QuotaCode: q.QuotaCode,
                        QuotaName: q.QuotaName, Value: q.Value,
                        Unit: q.Unit, Adjustable: q.Adjustable,
                        GlobalQuota: q.GlobalQuota));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list service quotas for '{serviceCode}'");
        }
    }

    /// <summary>
    /// Get the AWS default value for a service quota.
    /// </summary>
    public static async Task<ServiceQuotaResult> GetAWSDefaultServiceQuotaAsync(
        string serviceCode,
        string quotaCode,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAWSDefaultServiceQuotaAsync(
                new GetAWSDefaultServiceQuotaRequest
                {
                    ServiceCode = serviceCode,
                    QuotaCode = quotaCode
                });
            var q = resp.Quota;
            return new ServiceQuotaResult(
                ServiceCode: q.ServiceCode, ServiceName: q.ServiceName,
                QuotaArn: q.QuotaArn, QuotaCode: q.QuotaCode,
                QuotaName: q.QuotaName, Value: q.Value,
                Unit: q.Unit, Adjustable: q.Adjustable,
                GlobalQuota: q.GlobalQuota);
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get AWS default quota '{quotaCode}'");
        }
    }

    /// <summary>
    /// List all AWS default quotas for a service.
    /// </summary>
    public static async Task<List<ServiceQuotaResult>> ListAWSDefaultServiceQuotasAsync(
        string serviceCode,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<ServiceQuotaResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListAWSDefaultServiceQuotasRequest
                {
                    ServiceCode = serviceCode
                };
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListAWSDefaultServiceQuotasAsync(request);
                foreach (var q in resp.Quotas)
                {
                    results.Add(new ServiceQuotaResult(
                        ServiceCode: q.ServiceCode, ServiceName: q.ServiceName,
                        QuotaArn: q.QuotaArn, QuotaCode: q.QuotaCode,
                        QuotaName: q.QuotaName, Value: q.Value,
                        Unit: q.Unit, Adjustable: q.Adjustable,
                        GlobalQuota: q.GlobalQuota));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list AWS default quotas for '{serviceCode}'");
        }
    }

    // ── Quota increase requests ─────────────────────────────────────

    /// <summary>
    /// Request a service quota increase.
    /// </summary>
    public static async Task<RequestedQuotaChangeResult> RequestServiceQuotaIncreaseAsync(
        string serviceCode,
        string quotaCode,
        double desiredValue,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RequestServiceQuotaIncreaseAsync(
                new RequestServiceQuotaIncreaseRequest
                {
                    ServiceCode = serviceCode,
                    QuotaCode = quotaCode,
                    DesiredValue = desiredValue
                });
            var r = resp.RequestedQuota;
            return new RequestedQuotaChangeResult(
                Id: r.Id, CaseId: r.CaseId,
                ServiceCode: r.ServiceCode, ServiceName: r.ServiceName,
                QuotaCode: r.QuotaCode, QuotaName: r.QuotaName,
                DesiredValue: r.DesiredValue, Status: r.Status?.Value,
                Created: r.Created?.ToString(),
                LastUpdated: r.LastUpdated?.ToString(),
                Requester: r.Requester, QuotaArn: r.QuotaArn);
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to request quota increase for '{quotaCode}'");
        }
    }

    /// <summary>
    /// Get details of a requested service quota change.
    /// </summary>
    public static async Task<RequestedQuotaChangeResult> GetRequestedServiceQuotaChangeAsync(
        string requestId,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetRequestedServiceQuotaChangeAsync(
                new GetRequestedServiceQuotaChangeRequest
                {
                    RequestId = requestId
                });
            var r = resp.RequestedQuota;
            return new RequestedQuotaChangeResult(
                Id: r.Id, CaseId: r.CaseId,
                ServiceCode: r.ServiceCode, ServiceName: r.ServiceName,
                QuotaCode: r.QuotaCode, QuotaName: r.QuotaName,
                DesiredValue: r.DesiredValue, Status: r.Status?.Value,
                Created: r.Created?.ToString(),
                LastUpdated: r.LastUpdated?.ToString(),
                Requester: r.Requester, QuotaArn: r.QuotaArn);
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get requested quota change '{requestId}'");
        }
    }

    /// <summary>
    /// List the history of requested service quota changes.
    /// </summary>
    public static async Task<List<RequestedQuotaChangeResult>> ListRequestedServiceQuotaChangeHistoryAsync(
        string? serviceCode = null,
        string? status = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<RequestedQuotaChangeResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListRequestedServiceQuotaChangeHistoryRequest();
                if (serviceCode != null) request.ServiceCode = serviceCode;
                if (status != null) request.Status = new RequestStatus(status);
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListRequestedServiceQuotaChangeHistoryAsync(request);
                foreach (var r in resp.RequestedQuotas)
                {
                    results.Add(new RequestedQuotaChangeResult(
                        Id: r.Id, CaseId: r.CaseId,
                        ServiceCode: r.ServiceCode, ServiceName: r.ServiceName,
                        QuotaCode: r.QuotaCode, QuotaName: r.QuotaName,
                        DesiredValue: r.DesiredValue, Status: r.Status?.Value,
                        Created: r.Created?.ToString(),
                        LastUpdated: r.LastUpdated?.ToString(),
                        Requester: r.Requester, QuotaArn: r.QuotaArn));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list requested quota change history");
        }
    }

    /// <summary>
    /// List the history of requested service quota changes for a specific quota.
    /// </summary>
    public static async Task<List<RequestedQuotaChangeResult>> ListRequestedServiceQuotaChangeHistoryByQuotaAsync(
        string serviceCode,
        string quotaCode,
        string? status = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<RequestedQuotaChangeResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListRequestedServiceQuotaChangeHistoryByQuotaRequest
                {
                    ServiceCode = serviceCode,
                    QuotaCode = quotaCode
                };
                if (status != null) request.Status = new RequestStatus(status);
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListRequestedServiceQuotaChangeHistoryByQuotaAsync(request);
                foreach (var r in resp.RequestedQuotas)
                {
                    results.Add(new RequestedQuotaChangeResult(
                        Id: r.Id, CaseId: r.CaseId,
                        ServiceCode: r.ServiceCode, ServiceName: r.ServiceName,
                        QuotaCode: r.QuotaCode, QuotaName: r.QuotaName,
                        DesiredValue: r.DesiredValue, Status: r.Status?.Value,
                        Created: r.Created?.ToString(),
                        LastUpdated: r.LastUpdated?.ToString(),
                        Requester: r.Requester, QuotaArn: r.QuotaArn));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list quota change history for '{quotaCode}'");
        }
    }

    // ── Services ────────────────────────────────────────────────────

    /// <summary>
    /// List available services.
    /// </summary>
    public static async Task<List<ServiceInfoResult>> ListServicesAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<ServiceInfoResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListServicesRequest();
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListServicesAsync(request);
                foreach (var s in resp.Services)
                {
                    results.Add(new ServiceInfoResult(
                        ServiceCode: s.ServiceCode,
                        ServiceName: s.ServiceName));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list services");
        }
    }

    // ── Quota template ──────────────────────────────────────────────

    /// <summary>
    /// List service quota increase requests in the template.
    /// </summary>
    public static async Task<List<QuotaTemplateRequestResult>> ListServiceQuotaIncreaseRequestsInTemplateAsync(
        string? serviceCode = null,
        string? awsRegion = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var results = new List<QuotaTemplateRequestResult>();
        string? nextToken = null;

        try
        {
            do
            {
                var request = new ListServiceQuotaIncreaseRequestsInTemplateRequest();
                if (serviceCode != null) request.ServiceCode = serviceCode;
                if (awsRegion != null) request.AwsRegion = awsRegion;
                if (nextToken != null) request.NextToken = nextToken;

                var resp = await client.ListServiceQuotaIncreaseRequestsInTemplateAsync(request);
                foreach (var t in resp.ServiceQuotaIncreaseRequestInTemplateList)
                {
                    results.Add(new QuotaTemplateRequestResult(
                        ServiceCode: t.ServiceCode, ServiceName: t.ServiceName,
                        QuotaCode: t.QuotaCode, QuotaName: t.QuotaName,
                        DesiredValue: t.DesiredValue, AwsRegion: t.AwsRegion,
                        Unit: t.Unit, GlobalQuota: t.GlobalQuota));
                }
                nextToken = resp.NextToken;
            } while (nextToken != null);

            return results;
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to list quota increase requests in template");
        }
    }

    /// <summary>
    /// Add a service quota increase request to the template.
    /// </summary>
    public static async Task<QuotaTemplateRequestResult> PutServiceQuotaIncreaseRequestIntoTemplateAsync(
        string serviceCode,
        string quotaCode,
        string awsRegion,
        double desiredValue,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutServiceQuotaIncreaseRequestIntoTemplateAsync(
                new PutServiceQuotaIncreaseRequestIntoTemplateRequest
                {
                    ServiceCode = serviceCode,
                    QuotaCode = quotaCode,
                    AwsRegion = awsRegion,
                    DesiredValue = desiredValue
                });
            var t = resp.ServiceQuotaIncreaseRequestInTemplate;
            return new QuotaTemplateRequestResult(
                ServiceCode: t.ServiceCode, ServiceName: t.ServiceName,
                QuotaCode: t.QuotaCode, QuotaName: t.QuotaName,
                DesiredValue: t.DesiredValue, AwsRegion: t.AwsRegion,
                Unit: t.Unit, GlobalQuota: t.GlobalQuota);
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put quota increase request into template for '{quotaCode}'");
        }
    }

    /// <summary>
    /// Delete a service quota increase request from the template.
    /// </summary>
    public static async Task DeleteServiceQuotaIncreaseRequestFromTemplateAsync(
        string serviceCode,
        string quotaCode,
        string awsRegion,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteServiceQuotaIncreaseRequestFromTemplateAsync(
                new DeleteServiceQuotaIncreaseRequestFromTemplateRequest
                {
                    ServiceCode = serviceCode,
                    QuotaCode = quotaCode,
                    AwsRegion = awsRegion
                });
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete quota increase request from template for '{quotaCode}'");
        }
    }

    /// <summary>
    /// Get a service quota increase request from the template.
    /// </summary>
    public static async Task<QuotaTemplateRequestResult> GetServiceQuotaIncreaseRequestFromTemplateAsync(
        string serviceCode,
        string quotaCode,
        string awsRegion,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetServiceQuotaIncreaseRequestFromTemplateAsync(
                new GetServiceQuotaIncreaseRequestFromTemplateRequest
                {
                    ServiceCode = serviceCode,
                    QuotaCode = quotaCode,
                    AwsRegion = awsRegion
                });
            var t = resp.ServiceQuotaIncreaseRequestInTemplate;
            return new QuotaTemplateRequestResult(
                ServiceCode: t.ServiceCode, ServiceName: t.ServiceName,
                QuotaCode: t.QuotaCode, QuotaName: t.QuotaName,
                DesiredValue: t.DesiredValue, AwsRegion: t.AwsRegion,
                Unit: t.Unit, GlobalQuota: t.GlobalQuota);
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get quota increase request from template for '{quotaCode}'");
        }
    }

    /// <summary>
    /// Associate the Service Quotas template with the organization.
    /// </summary>
    public static async Task AssociateServiceQuotaTemplateAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.AssociateServiceQuotaTemplateAsync(
                new AssociateServiceQuotaTemplateRequest());
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to associate service quota template");
        }
    }

    /// <summary>
    /// Disassociate the Service Quotas template from the organization.
    /// </summary>
    public static async Task DisassociateServiceQuotaTemplateAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisassociateServiceQuotaTemplateAsync(
                new DisassociateServiceQuotaTemplateRequest());
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to disassociate service quota template");
        }
    }

    /// <summary>
    /// Get the association status of the Service Quotas template.
    /// </summary>
    public static async Task<QuotaTemplateAssociationResult> GetAssociationForServiceQuotaTemplateAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetAssociationForServiceQuotaTemplateAsync(
                new GetAssociationForServiceQuotaTemplateRequest());
            return new QuotaTemplateAssociationResult(
                Status: resp.ServiceQuotaTemplateAssociationStatus?.Value);
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to get association for service quota template");
        }
    }

    // ── Tagging ─────────────────────────────────────────────────────

    /// <summary>
    /// Tag a resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceArn,
        List<Amazon.ServiceQuotas.Model.Tag> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new TagResourceRequest
            {
                ResourceARN = resourceArn,
                Tags = tags
            });
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(new UntagResourceRequest
            {
                ResourceARN = resourceArn,
                TagKeys = tagKeys
            });
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a resource.
    /// </summary>
    public static async Task<List<SqTagResult>> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new ListTagsForResourceRequest
                {
                    ResourceARN = resourceArn
                });
            return resp.Tags
                .Select(t => new SqTagResult(Key: t.Key, Value: t.Value))
                .ToList();
        }
        catch (AmazonServiceQuotasException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for resource '{resourceArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="GetServiceQuotaAsync"/>.</summary>
    public static ServiceQuotaResult GetServiceQuota(string serviceCode, string quotaCode, RegionEndpoint? region = null)
        => GetServiceQuotaAsync(serviceCode, quotaCode, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListServiceQuotasAsync"/>.</summary>
    public static List<ServiceQuotaResult> ListServiceQuotas(string serviceCode, RegionEndpoint? region = null)
        => ListServiceQuotasAsync(serviceCode, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAWSDefaultServiceQuotaAsync"/>.</summary>
    public static ServiceQuotaResult GetAWSDefaultServiceQuota(string serviceCode, string quotaCode, RegionEndpoint? region = null)
        => GetAWSDefaultServiceQuotaAsync(serviceCode, quotaCode, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListAWSDefaultServiceQuotasAsync"/>.</summary>
    public static List<ServiceQuotaResult> ListAWSDefaultServiceQuotas(string serviceCode, RegionEndpoint? region = null)
        => ListAWSDefaultServiceQuotasAsync(serviceCode, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RequestServiceQuotaIncreaseAsync"/>.</summary>
    public static RequestedQuotaChangeResult RequestServiceQuotaIncrease(string serviceCode, string quotaCode, double desiredValue, RegionEndpoint? region = null)
        => RequestServiceQuotaIncreaseAsync(serviceCode, quotaCode, desiredValue, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetRequestedServiceQuotaChangeAsync"/>.</summary>
    public static RequestedQuotaChangeResult GetRequestedServiceQuotaChange(string requestId, RegionEndpoint? region = null)
        => GetRequestedServiceQuotaChangeAsync(requestId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListRequestedServiceQuotaChangeHistoryAsync"/>.</summary>
    public static List<RequestedQuotaChangeResult> ListRequestedServiceQuotaChangeHistory(string? serviceCode = null, string? status = null, RegionEndpoint? region = null)
        => ListRequestedServiceQuotaChangeHistoryAsync(serviceCode, status, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListRequestedServiceQuotaChangeHistoryByQuotaAsync"/>.</summary>
    public static List<RequestedQuotaChangeResult> ListRequestedServiceQuotaChangeHistoryByQuota(string serviceCode, string quotaCode, string? status = null, RegionEndpoint? region = null)
        => ListRequestedServiceQuotaChangeHistoryByQuotaAsync(serviceCode, quotaCode, status, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListServicesAsync"/>.</summary>
    public static List<ServiceInfoResult> ListServices(RegionEndpoint? region = null)
        => ListServicesAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListServiceQuotaIncreaseRequestsInTemplateAsync"/>.</summary>
    public static List<QuotaTemplateRequestResult> ListServiceQuotaIncreaseRequestsInTemplate(string? serviceCode = null, string? awsRegion = null, RegionEndpoint? region = null)
        => ListServiceQuotaIncreaseRequestsInTemplateAsync(serviceCode, awsRegion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutServiceQuotaIncreaseRequestIntoTemplateAsync"/>.</summary>
    public static QuotaTemplateRequestResult PutServiceQuotaIncreaseRequestIntoTemplate(string serviceCode, string quotaCode, string awsRegion, double desiredValue, RegionEndpoint? region = null)
        => PutServiceQuotaIncreaseRequestIntoTemplateAsync(serviceCode, quotaCode, awsRegion, desiredValue, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteServiceQuotaIncreaseRequestFromTemplateAsync"/>.</summary>
    public static void DeleteServiceQuotaIncreaseRequestFromTemplate(string serviceCode, string quotaCode, string awsRegion, RegionEndpoint? region = null)
        => DeleteServiceQuotaIncreaseRequestFromTemplateAsync(serviceCode, quotaCode, awsRegion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetServiceQuotaIncreaseRequestFromTemplateAsync"/>.</summary>
    public static QuotaTemplateRequestResult GetServiceQuotaIncreaseRequestFromTemplate(string serviceCode, string quotaCode, string awsRegion, RegionEndpoint? region = null)
        => GetServiceQuotaIncreaseRequestFromTemplateAsync(serviceCode, quotaCode, awsRegion, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="AssociateServiceQuotaTemplateAsync"/>.</summary>
    public static void AssociateServiceQuotaTemplate(RegionEndpoint? region = null)
        => AssociateServiceQuotaTemplateAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisassociateServiceQuotaTemplateAsync"/>.</summary>
    public static void DisassociateServiceQuotaTemplate(RegionEndpoint? region = null)
        => DisassociateServiceQuotaTemplateAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetAssociationForServiceQuotaTemplateAsync"/>.</summary>
    public static QuotaTemplateAssociationResult GetAssociationForServiceQuotaTemplate(RegionEndpoint? region = null)
        => GetAssociationForServiceQuotaTemplateAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, List<Amazon.ServiceQuotas.Model.Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static List<SqTagResult> ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

}

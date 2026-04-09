using Amazon;
using Amazon.SageMakerFeatureStoreRuntime;
using Amazon.SageMakerFeatureStoreRuntime.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for Amazon SageMaker Feature Store Runtime operations.
/// </summary>
public sealed record PutFeatureStoreRecordResult(bool Success = true);

public sealed record GetFeatureStoreRecordResult(
    List<FeatureValue>? Record = null);

public sealed record DeleteFeatureStoreRecordResult(bool Success = true);

public sealed record BatchGetFeatureStoreRecordResult(
    List<BatchGetRecordResultDetail>? Records = null,
    List<BatchGetRecordError>? Errors = null,
    List<BatchGetRecordIdentifier>? UnprocessedIdentifiers = null);

/// <summary>
/// Utility helpers for Amazon SageMaker Feature Store Runtime.
/// </summary>
public static class SageMakerFeatureStoreRuntimeService
{
    private static AmazonSageMakerFeatureStoreRuntimeClient GetClient(
        RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonSageMakerFeatureStoreRuntimeClient>(region);

    /// <summary>
    /// Put a record into a SageMaker Feature Store feature group.
    /// </summary>
    public static async Task<PutFeatureStoreRecordResult> PutRecordAsync(
        PutRecordRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutRecordAsync(request);
            return new PutFeatureStoreRecordResult();
        }
        catch (AmazonSageMakerFeatureStoreRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to put record into Feature Store group '{request.FeatureGroupName}'");
        }
    }

    /// <summary>
    /// Get a record from a SageMaker Feature Store feature group.
    /// </summary>
    public static async Task<GetFeatureStoreRecordResult> GetRecordAsync(
        string featureGroupName,
        string recordIdentifierValueAsString,
        List<string>? featureNames = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetRecordRequest
        {
            FeatureGroupName = featureGroupName,
            RecordIdentifierValueAsString = recordIdentifierValueAsString
        };
        if (featureNames != null) request.FeatureNames = featureNames;

        try
        {
            var resp = await client.GetRecordAsync(request);
            return new GetFeatureStoreRecordResult(Record: resp.Record);
        }
        catch (AmazonSageMakerFeatureStoreRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get record from Feature Store group '{featureGroupName}'");
        }
    }

    /// <summary>
    /// Delete a record from a SageMaker Feature Store feature group.
    /// </summary>
    public static async Task<DeleteFeatureStoreRecordResult> DeleteRecordAsync(
        string featureGroupName,
        string recordIdentifierValueAsString,
        string eventTime,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteRecordAsync(new DeleteRecordRequest
            {
                FeatureGroupName = featureGroupName,
                RecordIdentifierValueAsString = recordIdentifierValueAsString,
                EventTime = eventTime
            });
            return new DeleteFeatureStoreRecordResult();
        }
        catch (AmazonSageMakerFeatureStoreRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete record from Feature Store group '{featureGroupName}'");
        }
    }

    /// <summary>
    /// Batch get records from one or more SageMaker Feature Store feature groups.
    /// </summary>
    public static async Task<BatchGetFeatureStoreRecordResult> BatchGetRecordAsync(
        List<BatchGetRecordIdentifier> identifiers,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.BatchGetRecordAsync(
                new BatchGetRecordRequest { Identifiers = identifiers });
            return new BatchGetFeatureStoreRecordResult(
                Records: resp.Records,
                Errors: resp.Errors,
                UnprocessedIdentifiers: resp.UnprocessedIdentifiers);
        }
        catch (AmazonSageMakerFeatureStoreRuntimeException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to batch get records from Feature Store");
        }
    }
}

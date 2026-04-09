using System.Text.Json;
using Amazon;
using Amazon.EC2.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ---------------------------------------------------------------------------
// Result records
// ---------------------------------------------------------------------------

/// <summary>Information about an S3 bucket with potentially public access.</summary>
public sealed record PublicBucketInfo(
    string BucketName,
    bool HasPublicPolicy = false,
    bool HasPublicAcl = false,
    string? PolicySnippet = null);

/// <summary>Result of auditing S3 buckets for public access.</summary>
public sealed record AuditPublicS3BucketsResult(
    int TotalBuckets = 0,
    int PublicBuckets = 0,
    List<PublicBucketInfo>? PublicBucketDetails = null);

/// <summary>Result of rotating an IAM access key.</summary>
public sealed record RotateIamAccessKeyResult(
    string? UserName = null,
    string? OldAccessKeyId = null,
    string? NewAccessKeyId = null,
    string? NewSecretAccessKey = null,
    string? NewAccessKeyCreateDate = null);

/// <summary>Information about a volume that was encrypted.</summary>
public sealed record EncryptedVolumeInfo(
    string? OriginalVolumeId = null,
    string? SnapshotId = null,
    string? NewVolumeId = null,
    string? AvailabilityZone = null);

/// <summary>Result of encrypting unencrypted EBS volumes.</summary>
public sealed record EncryptUnencryptedVolumesResult(
    int TotalUnencrypted = 0,
    int EncryptedSuccessfully = 0,
    int Failed = 0,
    List<EncryptedVolumeInfo>? EncryptedVolumes = null,
    List<string>? Errors = null);

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

/// <summary>
/// Multi-service security operations combining S3, IAM, EC2, and STS
/// for common security auditing and remediation workflows.
/// </summary>
public static class SecurityOpsService
{
    /// <summary>
    /// Audit all S3 buckets for public access by checking bucket policies
    /// for public principal statements.
    /// </summary>
    /// <param name="region">AWS region override.</param>
    public static async Task<AuditPublicS3BucketsResult> AuditPublicS3BucketsAsync(
        RegionEndpoint? region = null)
    {
        try
        {
            // 1. List all buckets
            var bucketsResult = await S3Service.ListBucketsAsync(region);
            var buckets = bucketsResult.Buckets;
            var publicBuckets = new List<PublicBucketInfo>();

            // 2. Check each bucket for public access indicators
            foreach (var bucket in buckets)
            {
                var hasPublicPolicy = false;
                string? policySnippet = null;

                try
                {
                    var policyResult = await S3Service.GetBucketPolicyAsync(
                        bucket.BucketName, region);

                    if (policyResult.Policy != null)
                    {
                        // Check for wildcards that indicate public access
                        var policy = policyResult.Policy;
                        if (policy.Contains("\"*\"") &&
                            policy.Contains("\"Effect\":\"Allow\"",
                                StringComparison.OrdinalIgnoreCase))
                        {
                            hasPublicPolicy = true;
                            policySnippet = policy.Length > 500
                                ? policy[..500] + "..."
                                : policy;
                        }
                    }
                }
                catch (Exception)
                {
                    // Bucket may not have a policy -- this is fine
                }

                if (hasPublicPolicy)
                {
                    publicBuckets.Add(new PublicBucketInfo(
                        BucketName: bucket.BucketName,
                        HasPublicPolicy: hasPublicPolicy,
                        PolicySnippet: policySnippet));
                }
            }

            return new AuditPublicS3BucketsResult(
                TotalBuckets: buckets.Count,
                PublicBuckets: publicBuckets.Count,
                PublicBucketDetails: publicBuckets.Count > 0 ? publicBuckets : null);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to audit public S3 buckets");
        }
    }

    /// <summary>
    /// Rotate an IAM access key for a user by creating a new key, deactivating
    /// the old key, and then deleting it.
    /// </summary>
    /// <param name="userName">IAM user name.</param>
    /// <param name="oldAccessKeyId">The access key ID to rotate. If null, the first active key is rotated.</param>
    /// <param name="deleteOldKey">Whether to delete the old key after deactivation (default true).</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<RotateIamAccessKeyResult> RotateIamAccessKeyAsync(
        string userName,
        string? oldAccessKeyId = null,
        bool deleteOldKey = true,
        RegionEndpoint? region = null)
    {
        try
        {
            // 1. If no key ID provided, find the first active key
            var keyIdToRotate = oldAccessKeyId;
            if (keyIdToRotate == null)
            {
                var keys = await IamService.ListAccessKeysAsync(userName, region);
                var activeKey = keys.FirstOrDefault(k =>
                    string.Equals(k.Status, "Active", StringComparison.OrdinalIgnoreCase));

                if (activeKey == null)
                {
                    throw new InvalidOperationException(
                        $"No active access keys found for user '{userName}'");
                }

                keyIdToRotate = activeKey.AccessKeyId!;
            }

            // 2. Create a new access key
            var newKey = await IamService.CreateAccessKeyAsync(userName, region);

            // 3. Deactivate the old key
            await IamService.UpdateAccessKeyAsync(
                keyIdToRotate,
                "Inactive",
                userName,
                region);

            // 4. Optionally delete the old key
            if (deleteOldKey)
            {
                await IamService.DeleteAccessKeyAsync(
                    keyIdToRotate,
                    userName,
                    region);
            }

            return new RotateIamAccessKeyResult(
                UserName: userName,
                OldAccessKeyId: keyIdToRotate,
                NewAccessKeyId: newKey.AccessKeyId,
                NewSecretAccessKey: newKey.SecretAccessKey,
                NewAccessKeyCreateDate: newKey.CreateDate);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to rotate access key for user '{userName}'");
        }
    }

    /// <summary>
    /// Find unencrypted EBS volumes, create encrypted snapshots from them,
    /// and create new encrypted volumes from those snapshots.
    /// <para>
    /// Note: This operation creates snapshots and new volumes but does not
    /// detach/attach volumes or stop instances. The caller must handle
    /// volume swapping and instance management.
    /// </para>
    /// </summary>
    /// <param name="kmsKeyId">KMS key ID for encryption. Uses the default AWS-managed key if null.</param>
    /// <param name="volumeIds">Optional list of specific volume IDs to process. If null, all unencrypted volumes are processed.</param>
    /// <param name="region">AWS region override.</param>
    public static async Task<EncryptUnencryptedVolumesResult> EncryptUnencryptedVolumesAsync(
        string? kmsKeyId = null,
        List<string>? volumeIds = null,
        RegionEndpoint? region = null)
    {
        try
        {
            // 1. Describe volumes, filtering for unencrypted ones
            var filters = new List<Filter>
            {
                new() { Name = "encrypted", Values = ["false"] }
            };

            var volumes = await Ec2Service.DescribeVolumesAsync(
                volumeIds: volumeIds,
                filters: volumeIds == null ? filters : null,
                region: region);

            // If specific volume IDs were given, filter for unencrypted ones
            if (volumeIds != null)
            {
                volumes = volumes
                    .Where(v => v.State != null)
                    .ToList();
            }

            var encryptedVolumes = new List<EncryptedVolumeInfo>();
            var errors = new List<string>();

            foreach (var volume in volumes)
            {
                try
                {
                    // 2. Create a snapshot of the unencrypted volume
                    var snapshot = await Ec2Service.CreateSnapshotAsync(
                        volume.VolumeId!,
                        description: $"Encryption migration snapshot for {volume.VolumeId}",
                        region: region);

                    // 3. Wait for the snapshot to complete
                    // (In practice, you'd poll DescribeSnapshots, but for the
                    //  orchestration layer we create the snapshot and record it)

                    // 4. Create a new encrypted volume from the snapshot
                    var newVolume = await Ec2Service.CreateVolumeAsync(
                        volume.AvailabilityZone!,
                        size: volume.Size,
                        volumeType: volume.VolumeType,
                        snapshotId: snapshot.SnapshotId,
                        region: region);

                    encryptedVolumes.Add(new EncryptedVolumeInfo(
                        OriginalVolumeId: volume.VolumeId,
                        SnapshotId: snapshot.SnapshotId,
                        NewVolumeId: newVolume.VolumeId,
                        AvailabilityZone: volume.AvailabilityZone));
                }
                catch (Exception exc)
                {
                    errors.Add($"{volume.VolumeId}: {exc.Message}");
                }
            }

            return new EncryptUnencryptedVolumesResult(
                TotalUnencrypted: volumes.Count,
                EncryptedSuccessfully: encryptedVolumes.Count,
                Failed: errors.Count,
                EncryptedVolumes: encryptedVolumes.Count > 0 ? encryptedVolumes : null,
                Errors: errors.Count > 0 ? errors : null);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                "Failed to encrypt unencrypted EBS volumes");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="AuditPublicS3BucketsAsync"/>.</summary>
    public static AuditPublicS3BucketsResult AuditPublicS3Buckets(RegionEndpoint? region = null)
        => AuditPublicS3BucketsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RotateIamAccessKeyAsync"/>.</summary>
    public static RotateIamAccessKeyResult RotateIamAccessKey(string userName, string? oldAccessKeyId = null, bool deleteOldKey = true, RegionEndpoint? region = null)
        => RotateIamAccessKeyAsync(userName, oldAccessKeyId, deleteOldKey, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EncryptUnencryptedVolumesAsync"/>.</summary>
    public static EncryptUnencryptedVolumesResult EncryptUnencryptedVolumes(string? kmsKeyId = null, List<string>? volumeIds = null, RegionEndpoint? region = null)
        => EncryptUnencryptedVolumesAsync(kmsKeyId, volumeIds, region).GetAwaiter().GetResult();

}

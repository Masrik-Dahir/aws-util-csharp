using Amazon;
using Amazon.Rekognition;
using Amazon.Rekognition.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

/// <summary>
/// Result models for Amazon Rekognition operations.
/// </summary>
public sealed record DetectLabelsResult(
    List<Label>? Labels = null,
    string? LabelModelVersion = null);

public sealed record DetectFacesResult(List<FaceDetail>? FaceDetails = null);

public sealed record DetectTextResult(List<TextDetection>? TextDetections = null);

public sealed record DetectModerationLabelsResult(
    List<Amazon.Rekognition.Model.ModerationLabel>? ModerationLabels = null,
    string? ModerationModelVersion = null);

public sealed record RecognizeCelebritiesResult(
    List<Celebrity>? CelebrityFaces = null,
    List<ComparedFace>? UnrecognizedFaces = null);

public sealed record CompareFacesResult(
    List<CompareFacesMatch>? FaceMatches = null,
    List<ComparedFace>? UnmatchedFaces = null,
    float? SourceImageFaceConfidence = null);

public sealed record SearchFacesByImageResult(
    List<FaceMatch>? FaceMatches = null,
    BoundingBox? SearchedFaceBoundingBox = null,
    float? SearchedFaceConfidence = null);

public sealed record IndexFacesResult(
    List<FaceRecord>? FaceRecords = null,
    List<UnindexedFace>? UnindexedFaces = null);

public sealed record DeleteFacesResult(
    List<string>? DeletedFaces = null,
    List<UnsuccessfulFaceDeletion>? UnsuccessfulFaceDeletions = null);

public sealed record SearchFacesResult(List<FaceMatch>? FaceMatches = null);

public sealed record CreateCollectionResult(
    string? CollectionArn = null,
    int? StatusCode = null);

public sealed record DescribeCollectionResult(
    string? CollectionArn = null,
    long? FaceCount = null,
    string? FaceModelVersion = null,
    string? CreationTimestamp = null);

public sealed record ListCollectionsResult(
    List<string>? CollectionIds = null);

public sealed record ListFacesResult(
    List<Face>? Faces = null,
    string? FaceModelVersion = null);

public sealed record DetectProtectiveEquipmentResult(
    List<ProtectiveEquipmentPerson>? Persons = null,
    ProtectiveEquipmentSummary? Summary = null);

public sealed record StartLabelDetectionResult(string? JobId = null);
public sealed record GetLabelDetectionResult(
    string? JobStatus = null,
    List<LabelDetection>? Labels = null,
    string? LabelModelVersion = null);

public sealed record StartFaceDetectionResult(string? JobId = null);
public sealed record GetFaceDetectionResult(
    string? JobStatus = null,
    List<FaceDetection>? Faces = null);

public sealed record StartTextDetectionResult(string? JobId = null);
public sealed record GetTextDetectionResult(
    string? JobStatus = null,
    List<TextDetectionResult>? TextDetections = null);

public sealed record StartCelebrityRecognitionResult(string? JobId = null);
public sealed record GetCelebrityRecognitionResult(
    string? JobStatus = null,
    List<CelebrityRecognition>? Celebrities = null);

public sealed record StartContentModerationResult(string? JobId = null);
public sealed record GetContentModerationResult(
    string? JobStatus = null,
    List<ContentModerationDetection>? ModerationLabels = null);

public sealed record StartPersonTrackingResult(string? JobId = null);
public sealed record GetPersonTrackingResult(
    string? JobStatus = null,
    List<PersonDetection>? Persons = null);

public sealed record ListRekognitionTagsResult(
    Dictionary<string, string>? Tags = null);

public sealed record CreateRekognitionProjectResult(string? ProjectArn = null);

public sealed record DescribeProjectsResult(
    List<ProjectDescription>? ProjectDescriptions = null);

public sealed record CreateProjectVersionResult(string? ProjectVersionArn = null);

public sealed record DescribeProjectVersionsResult(
    List<ProjectVersionDescription>? ProjectVersionDescriptions = null);

public sealed record StartProjectVersionResult(string? Status = null);

public sealed record StopProjectVersionResult(string? Status = null);

/// <summary>
/// Utility helpers for Amazon Rekognition.
/// </summary>
public static class RekognitionService
{
    private static AmazonRekognitionClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonRekognitionClient>(region);

    // ──────────────────────────── Image Analysis ────────────────────────────

    /// <summary>
    /// Detect labels in an image.
    /// </summary>
    public static async Task<DetectLabelsResult> DetectLabelsAsync(
        Image image,
        int? maxLabels = null,
        float? minConfidence = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectLabelsRequest { Image = image };
        if (maxLabels.HasValue) request.MaxLabels = maxLabels.Value;
        if (minConfidence.HasValue) request.MinConfidence = minConfidence.Value;

        try
        {
            var resp = await client.DetectLabelsAsync(request);
            return new DetectLabelsResult(
                Labels: resp.Labels,
                LabelModelVersion: resp.LabelModelVersion);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to detect labels");
        }
    }

    /// <summary>
    /// Detect faces in an image.
    /// </summary>
    public static async Task<DetectFacesResult> DetectFacesAsync(
        Image image,
        List<string>? attributes = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectFacesRequest { Image = image };
        if (attributes != null)
            request.Attributes = attributes;

        try
        {
            var resp = await client.DetectFacesAsync(request);
            return new DetectFacesResult(FaceDetails: resp.FaceDetails);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to detect faces");
        }
    }

    /// <summary>
    /// Detect text in an image.
    /// </summary>
    public static async Task<DetectTextResult> DetectTextAsync(
        Image image, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DetectTextAsync(new DetectTextRequest { Image = image });
            return new DetectTextResult(TextDetections: resp.TextDetections);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to detect text");
        }
    }

    /// <summary>
    /// Detect moderation labels in an image.
    /// </summary>
    public static async Task<DetectModerationLabelsResult> DetectModerationLabelsAsync(
        Image image,
        float? minConfidence = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectModerationLabelsRequest { Image = image };
        if (minConfidence.HasValue) request.MinConfidence = minConfidence.Value;

        try
        {
            var resp = await client.DetectModerationLabelsAsync(request);
            return new DetectModerationLabelsResult(
                ModerationLabels: resp.ModerationLabels,
                ModerationModelVersion: resp.ModerationModelVersion);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to detect moderation labels");
        }
    }

    /// <summary>
    /// Recognize celebrities in an image.
    /// </summary>
    public static async Task<RecognizeCelebritiesResult> RecognizeCelebritiesAsync(
        Image image, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.RecognizeCelebritiesAsync(
                new RecognizeCelebritiesRequest { Image = image });
            return new RecognizeCelebritiesResult(
                CelebrityFaces: resp.CelebrityFaces,
                UnrecognizedFaces: resp.UnrecognizedFaces);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to recognize celebrities");
        }
    }

    /// <summary>
    /// Compare faces between a source and target image.
    /// </summary>
    public static async Task<CompareFacesResult> CompareFacesAsync(
        Image sourceImage,
        Image targetImage,
        float? similarityThreshold = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CompareFacesRequest
        {
            SourceImage = sourceImage,
            TargetImage = targetImage
        };
        if (similarityThreshold.HasValue)
            request.SimilarityThreshold = similarityThreshold.Value;

        try
        {
            var resp = await client.CompareFacesAsync(request);
            return new CompareFacesResult(
                FaceMatches: resp.FaceMatches,
                UnmatchedFaces: resp.UnmatchedFaces,
                SourceImageFaceConfidence: resp.SourceImageFace?.Confidence);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to compare faces");
        }
    }

    // ──────────────────────────── Face Collections ────────────────────────────

    /// <summary>
    /// Search for faces in a collection by image.
    /// </summary>
    public static async Task<SearchFacesByImageResult> SearchFacesByImageAsync(
        string collectionId,
        Image image,
        int? maxFaces = null,
        float? faceMatchThreshold = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SearchFacesByImageRequest
        {
            CollectionId = collectionId,
            Image = image
        };
        if (maxFaces.HasValue) request.MaxFaces = maxFaces.Value;
        if (faceMatchThreshold.HasValue)
            request.FaceMatchThreshold = faceMatchThreshold.Value;

        try
        {
            var resp = await client.SearchFacesByImageAsync(request);
            return new SearchFacesByImageResult(
                FaceMatches: resp.FaceMatches,
                SearchedFaceBoundingBox: resp.SearchedFaceBoundingBox,
                SearchedFaceConfidence: resp.SearchedFaceConfidence);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to search faces by image in collection '{collectionId}'");
        }
    }

    /// <summary>
    /// Index faces from an image into a collection.
    /// </summary>
    public static async Task<IndexFacesResult> IndexFacesAsync(
        string collectionId,
        Image image,
        string? externalImageId = null,
        int? maxFaces = null,
        QualityFilter? qualityFilter = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new IndexFacesRequest
        {
            CollectionId = collectionId,
            Image = image
        };
        if (externalImageId != null) request.ExternalImageId = externalImageId;
        if (maxFaces.HasValue) request.MaxFaces = maxFaces.Value;
        if (qualityFilter != null) request.QualityFilter = qualityFilter;

        try
        {
            var resp = await client.IndexFacesAsync(request);
            return new IndexFacesResult(
                FaceRecords: resp.FaceRecords,
                UnindexedFaces: resp.UnindexedFaces);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to index faces in collection '{collectionId}'");
        }
    }

    /// <summary>
    /// Delete faces from a collection.
    /// </summary>
    public static async Task<DeleteFacesResult> DeleteFacesAsync(
        string collectionId,
        List<string> faceIds,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DeleteFacesAsync(new DeleteFacesRequest
            {
                CollectionId = collectionId,
                FaceIds = faceIds
            });
            return new DeleteFacesResult(
                DeletedFaces: resp.DeletedFaces,
                UnsuccessfulFaceDeletions: resp.UnsuccessfulFaceDeletions);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete faces in collection '{collectionId}'");
        }
    }

    /// <summary>
    /// Search for faces in a collection by face ID.
    /// </summary>
    public static async Task<SearchFacesResult> SearchFacesAsync(
        string collectionId,
        string faceId,
        int? maxFaces = null,
        float? faceMatchThreshold = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SearchFacesRequest
        {
            CollectionId = collectionId,
            FaceId = faceId
        };
        if (maxFaces.HasValue) request.MaxFaces = maxFaces.Value;
        if (faceMatchThreshold.HasValue)
            request.FaceMatchThreshold = faceMatchThreshold.Value;

        try
        {
            var resp = await client.SearchFacesAsync(request);
            return new SearchFacesResult(FaceMatches: resp.FaceMatches);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to search faces in collection '{collectionId}'");
        }
    }

    /// <summary>
    /// Create a face collection.
    /// </summary>
    public static async Task<CreateCollectionResult> CreateCollectionAsync(
        string collectionId,
        Dictionary<string, string>? tags = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new CreateCollectionRequest { CollectionId = collectionId };
        if (tags != null) request.Tags = tags;

        try
        {
            var resp = await client.CreateCollectionAsync(request);
            return new CreateCollectionResult(
                CollectionArn: resp.CollectionArn,
                StatusCode: resp.StatusCode);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create collection '{collectionId}'");
        }
    }

    /// <summary>
    /// Delete a face collection.
    /// </summary>
    public static async Task DeleteCollectionAsync(
        string collectionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteCollectionAsync(
                new DeleteCollectionRequest { CollectionId = collectionId });
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete collection '{collectionId}'");
        }
    }

    /// <summary>
    /// Describe a face collection.
    /// </summary>
    public static async Task<DescribeCollectionResult> DescribeCollectionAsync(
        string collectionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.DescribeCollectionAsync(
                new DescribeCollectionRequest { CollectionId = collectionId });
            return new DescribeCollectionResult(
                CollectionArn: resp.CollectionARN,
                FaceCount: resp.FaceCount,
                FaceModelVersion: resp.FaceModelVersion,
                CreationTimestamp: resp.CreationTimestamp?.ToString());
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe collection '{collectionId}'");
        }
    }

    /// <summary>
    /// List all face collections, automatically paginating.
    /// </summary>
    public static async Task<ListCollectionsResult> ListCollectionsAsync(
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var collectionIds = new List<string>();
        var request = new ListCollectionsRequest();

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListCollectionsAsync(request);
                collectionIds.AddRange(resp.CollectionIds);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list collections");
        }

        return new ListCollectionsResult(CollectionIds: collectionIds);
    }

    /// <summary>
    /// List faces in a collection, automatically paginating.
    /// </summary>
    public static async Task<ListFacesResult> ListFacesAsync(
        string collectionId, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var faces = new List<Face>();
        var request = new ListFacesRequest { CollectionId = collectionId };
        string? faceModelVersion = null;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.ListFacesAsync(request);
                faces.AddRange(resp.Faces);
                faceModelVersion = resp.FaceModelVersion;
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list faces in collection '{collectionId}'");
        }

        return new ListFacesResult(Faces: faces, FaceModelVersion: faceModelVersion);
    }

    // ──────────────────────────── Protective Equipment ────────────────────────────

    /// <summary>
    /// Detect protective equipment in an image.
    /// </summary>
    public static async Task<DetectProtectiveEquipmentResult> DetectProtectiveEquipmentAsync(
        Image image,
        ProtectiveEquipmentSummarizationAttributes? summarizationAttributes = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DetectProtectiveEquipmentRequest { Image = image };
        if (summarizationAttributes != null)
            request.SummarizationAttributes = summarizationAttributes;

        try
        {
            var resp = await client.DetectProtectiveEquipmentAsync(request);
            return new DetectProtectiveEquipmentResult(
                Persons: resp.Persons,
                Summary: resp.Summary);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to detect protective equipment");
        }
    }

    // ──────────────────────────── Video Analysis ────────────────────────────

    /// <summary>
    /// Start label detection on a stored video.
    /// </summary>
    public static async Task<StartLabelDetectionResult> StartLabelDetectionAsync(
        Video video,
        float? minConfidence = null,
        NotificationChannel? notificationChannel = null,
        string? jobTag = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartLabelDetectionRequest { Video = video };
        if (minConfidence.HasValue) request.MinConfidence = minConfidence.Value;
        if (notificationChannel != null) request.NotificationChannel = notificationChannel;
        if (jobTag != null) request.JobTag = jobTag;

        try
        {
            var resp = await client.StartLabelDetectionAsync(request);
            return new StartLabelDetectionResult(JobId: resp.JobId);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start label detection");
        }
    }

    /// <summary>
    /// Get label detection results for a video analysis job.
    /// </summary>
    public static async Task<GetLabelDetectionResult> GetLabelDetectionAsync(
        string jobId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetLabelDetectionRequest { JobId = jobId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetLabelDetectionAsync(request);
            return new GetLabelDetectionResult(
                JobStatus: resp.JobStatus?.Value,
                Labels: resp.Labels,
                LabelModelVersion: resp.LabelModelVersion);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get label detection results for job '{jobId}'");
        }
    }

    /// <summary>
    /// Start face detection on a stored video.
    /// </summary>
    public static async Task<StartFaceDetectionResult> StartFaceDetectionAsync(
        Video video,
        string? faceAttributes = null,
        NotificationChannel? notificationChannel = null,
        string? jobTag = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartFaceDetectionRequest { Video = video };
        if (faceAttributes != null) request.FaceAttributes = faceAttributes;
        if (notificationChannel != null) request.NotificationChannel = notificationChannel;
        if (jobTag != null) request.JobTag = jobTag;

        try
        {
            var resp = await client.StartFaceDetectionAsync(request);
            return new StartFaceDetectionResult(JobId: resp.JobId);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start face detection");
        }
    }

    /// <summary>
    /// Get face detection results for a video analysis job.
    /// </summary>
    public static async Task<GetFaceDetectionResult> GetFaceDetectionAsync(
        string jobId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetFaceDetectionRequest { JobId = jobId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetFaceDetectionAsync(request);
            return new GetFaceDetectionResult(
                JobStatus: resp.JobStatus?.Value,
                Faces: resp.Faces);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get face detection results for job '{jobId}'");
        }
    }

    /// <summary>
    /// Start text detection on a stored video.
    /// </summary>
    public static async Task<StartTextDetectionResult> StartTextDetectionAsync(
        Video video,
        NotificationChannel? notificationChannel = null,
        string? jobTag = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartTextDetectionRequest { Video = video };
        if (notificationChannel != null) request.NotificationChannel = notificationChannel;
        if (jobTag != null) request.JobTag = jobTag;

        try
        {
            var resp = await client.StartTextDetectionAsync(request);
            return new StartTextDetectionResult(JobId: resp.JobId);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start text detection");
        }
    }

    /// <summary>
    /// Get text detection results for a video analysis job.
    /// </summary>
    public static async Task<GetTextDetectionResult> GetTextDetectionAsync(
        string jobId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetTextDetectionRequest { JobId = jobId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetTextDetectionAsync(request);
            return new GetTextDetectionResult(
                JobStatus: resp.JobStatus?.Value,
                TextDetections: resp.TextDetections);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get text detection results for job '{jobId}'");
        }
    }

    /// <summary>
    /// Start celebrity recognition on a stored video.
    /// </summary>
    public static async Task<StartCelebrityRecognitionResult> StartCelebrityRecognitionAsync(
        Video video,
        NotificationChannel? notificationChannel = null,
        string? jobTag = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartCelebrityRecognitionRequest { Video = video };
        if (notificationChannel != null) request.NotificationChannel = notificationChannel;
        if (jobTag != null) request.JobTag = jobTag;

        try
        {
            var resp = await client.StartCelebrityRecognitionAsync(request);
            return new StartCelebrityRecognitionResult(JobId: resp.JobId);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start celebrity recognition");
        }
    }

    /// <summary>
    /// Get celebrity recognition results for a video analysis job.
    /// </summary>
    public static async Task<GetCelebrityRecognitionResult> GetCelebrityRecognitionAsync(
        string jobId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetCelebrityRecognitionRequest { JobId = jobId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetCelebrityRecognitionAsync(request);
            return new GetCelebrityRecognitionResult(
                JobStatus: resp.JobStatus?.Value,
                Celebrities: resp.Celebrities);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get celebrity recognition results for job '{jobId}'");
        }
    }

    /// <summary>
    /// Start content moderation on a stored video.
    /// </summary>
    public static async Task<StartContentModerationResult> StartContentModerationAsync(
        Video video,
        float? minConfidence = null,
        NotificationChannel? notificationChannel = null,
        string? jobTag = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartContentModerationRequest { Video = video };
        if (minConfidence.HasValue) request.MinConfidence = minConfidence.Value;
        if (notificationChannel != null) request.NotificationChannel = notificationChannel;
        if (jobTag != null) request.JobTag = jobTag;

        try
        {
            var resp = await client.StartContentModerationAsync(request);
            return new StartContentModerationResult(JobId: resp.JobId);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start content moderation");
        }
    }

    /// <summary>
    /// Get content moderation results for a video analysis job.
    /// </summary>
    public static async Task<GetContentModerationResult> GetContentModerationAsync(
        string jobId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetContentModerationRequest { JobId = jobId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetContentModerationAsync(request);
            return new GetContentModerationResult(
                JobStatus: resp.JobStatus?.Value,
                ModerationLabels: resp.ModerationLabels);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get content moderation results for job '{jobId}'");
        }
    }

    /// <summary>
    /// Start person tracking on a stored video.
    /// </summary>
    public static async Task<StartPersonTrackingResult> StartPersonTrackingAsync(
        Video video,
        NotificationChannel? notificationChannel = null,
        string? jobTag = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartPersonTrackingRequest { Video = video };
        if (notificationChannel != null) request.NotificationChannel = notificationChannel;
        if (jobTag != null) request.JobTag = jobTag;

        try
        {
            var resp = await client.StartPersonTrackingAsync(request);
            return new StartPersonTrackingResult(JobId: resp.JobId);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to start person tracking");
        }
    }

    /// <summary>
    /// Get person tracking results for a video analysis job.
    /// </summary>
    public static async Task<GetPersonTrackingResult> GetPersonTrackingAsync(
        string jobId,
        int? maxResults = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetPersonTrackingRequest { JobId = jobId };
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.GetPersonTrackingAsync(request);
            return new GetPersonTrackingResult(
                JobStatus: resp.JobStatus?.Value,
                Persons: resp.Persons);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to get person tracking results for job '{jobId}'");
        }
    }

    // ──────────────────────────── Tags ────────────────────────────

    /// <summary>
    /// Add tags to a Rekognition resource.
    /// </summary>
    public static async Task TagResourceAsync(
        string resourceArn,
        Dictionary<string, string> tags,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.TagResourceAsync(new Amazon.Rekognition.Model.TagResourceRequest
            {
                ResourceArn = resourceArn,
                Tags = tags
            });
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to tag Rekognition resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// Remove tags from a Rekognition resource.
    /// </summary>
    public static async Task UntagResourceAsync(
        string resourceArn,
        List<string> tagKeys,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.UntagResourceAsync(
                new Amazon.Rekognition.Model.UntagResourceRequest
                {
                    ResourceArn = resourceArn,
                    TagKeys = tagKeys
                });
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to untag Rekognition resource '{resourceArn}'");
        }
    }

    /// <summary>
    /// List tags for a Rekognition resource.
    /// </summary>
    public static async Task<ListRekognitionTagsResult> ListTagsForResourceAsync(
        string resourceArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(
                new Amazon.Rekognition.Model.ListTagsForResourceRequest
                {
                    ResourceArn = resourceArn
                });
            return new ListRekognitionTagsResult(Tags: resp.Tags);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to list tags for Rekognition resource '{resourceArn}'");
        }
    }

    // ──────────────────────────── Custom Labels (Projects) ────────────────────────────

    /// <summary>
    /// Create a Rekognition Custom Labels project.
    /// </summary>
    public static async Task<CreateRekognitionProjectResult> CreateProjectAsync(
        string projectName, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateProjectAsync(
                new CreateProjectRequest { ProjectName = projectName });
            return new CreateRekognitionProjectResult(ProjectArn: resp.ProjectArn);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Rekognition project '{projectName}'");
        }
    }

    /// <summary>
    /// Delete a Rekognition Custom Labels project.
    /// </summary>
    public static async Task DeleteProjectAsync(
        string projectArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteProjectAsync(
                new DeleteProjectRequest { ProjectArn = projectArn });
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Rekognition project '{projectArn}'");
        }
    }

    /// <summary>
    /// Describe Rekognition Custom Labels projects, automatically paginating.
    /// </summary>
    public static async Task<DescribeProjectsResult> DescribeProjectsAsync(
        List<string>? projectNames = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var descriptions = new List<ProjectDescription>();
        var request = new DescribeProjectsRequest();
        if (projectNames != null) request.ProjectNames = projectNames;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.DescribeProjectsAsync(request);
                descriptions.AddRange(resp.ProjectDescriptions);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe Rekognition projects");
        }

        return new DescribeProjectsResult(ProjectDescriptions: descriptions);
    }

    /// <summary>
    /// Create a version of a Rekognition Custom Labels project.
    /// </summary>
    public static async Task<CreateProjectVersionResult> CreateProjectVersionAsync(
        CreateProjectVersionRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.CreateProjectVersionAsync(request);
            return new CreateProjectVersionResult(
                ProjectVersionArn: resp.ProjectVersionArn);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to create Rekognition project version for '{request.ProjectArn}'");
        }
    }

    /// <summary>
    /// Delete a version of a Rekognition Custom Labels project.
    /// </summary>
    public static async Task DeleteProjectVersionAsync(
        string projectVersionArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteProjectVersionAsync(
                new DeleteProjectVersionRequest
                {
                    ProjectVersionArn = projectVersionArn
                });
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to delete Rekognition project version '{projectVersionArn}'");
        }
    }

    /// <summary>
    /// Describe versions of a Rekognition Custom Labels project, automatically paginating.
    /// </summary>
    public static async Task<DescribeProjectVersionsResult> DescribeProjectVersionsAsync(
        string projectArn,
        List<string>? versionNames = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var descriptions = new List<ProjectVersionDescription>();
        var request = new DescribeProjectVersionsRequest { ProjectArn = projectArn };
        if (versionNames != null) request.VersionNames = versionNames;

        try
        {
            string? nextToken = null;
            do
            {
                request.NextToken = nextToken;
                var resp = await client.DescribeProjectVersionsAsync(request);
                descriptions.AddRange(resp.ProjectVersionDescriptions);
                nextToken = resp.NextToken;
            } while (nextToken != null);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to describe Rekognition project versions for '{projectArn}'");
        }

        return new DescribeProjectVersionsResult(
            ProjectVersionDescriptions: descriptions);
    }

    /// <summary>
    /// Start a Rekognition Custom Labels project version (model).
    /// </summary>
    public static async Task<StartProjectVersionResult> StartProjectVersionAsync(
        string projectVersionArn,
        int minInferenceUnits,
        int? maxInferenceUnits = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new StartProjectVersionRequest
        {
            ProjectVersionArn = projectVersionArn,
            MinInferenceUnits = minInferenceUnits
        };
        if (maxInferenceUnits.HasValue)
            request.MaxInferenceUnits = maxInferenceUnits.Value;

        try
        {
            var resp = await client.StartProjectVersionAsync(request);
            return new StartProjectVersionResult(Status: resp.Status?.Value);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to start Rekognition project version '{projectVersionArn}'");
        }
    }

    /// <summary>
    /// Stop a Rekognition Custom Labels project version (model).
    /// </summary>
    public static async Task<StopProjectVersionResult> StopProjectVersionAsync(
        string projectVersionArn, RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.StopProjectVersionAsync(
                new StopProjectVersionRequest
                {
                    ProjectVersionArn = projectVersionArn
                });
            return new StopProjectVersionResult(Status: resp.Status?.Value);
        }
        catch (AmazonRekognitionException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc,
                $"Failed to stop Rekognition project version '{projectVersionArn}'");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="DetectLabelsAsync"/>.</summary>
    public static DetectLabelsResult DetectLabels(Image image, int? maxLabels = null, float? minConfidence = null, RegionEndpoint? region = null)
        => DetectLabelsAsync(image, maxLabels, minConfidence, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DetectFacesAsync"/>.</summary>
    public static DetectFacesResult DetectFaces(Image image, List<string>? attributes = null, RegionEndpoint? region = null)
        => DetectFacesAsync(image, attributes, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DetectTextAsync"/>.</summary>
    public static DetectTextResult DetectText(Image image, RegionEndpoint? region = null)
        => DetectTextAsync(image, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DetectModerationLabelsAsync"/>.</summary>
    public static DetectModerationLabelsResult DetectModerationLabels(Image image, float? minConfidence = null, RegionEndpoint? region = null)
        => DetectModerationLabelsAsync(image, minConfidence, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="RecognizeCelebritiesAsync"/>.</summary>
    public static RecognizeCelebritiesResult RecognizeCelebrities(Image image, RegionEndpoint? region = null)
        => RecognizeCelebritiesAsync(image, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CompareFacesAsync"/>.</summary>
    public static CompareFacesResult CompareFaces(Image sourceImage, Image targetImage, float? similarityThreshold = null, RegionEndpoint? region = null)
        => CompareFacesAsync(sourceImage, targetImage, similarityThreshold, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SearchFacesByImageAsync"/>.</summary>
    public static SearchFacesByImageResult SearchFacesByImage(string collectionId, Image image, int? maxFaces = null, float? faceMatchThreshold = null, RegionEndpoint? region = null)
        => SearchFacesByImageAsync(collectionId, image, maxFaces, faceMatchThreshold, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="IndexFacesAsync"/>.</summary>
    public static IndexFacesResult IndexFaces(string collectionId, Image image, string? externalImageId = null, int? maxFaces = null, QualityFilter? qualityFilter = null, RegionEndpoint? region = null)
        => IndexFacesAsync(collectionId, image, externalImageId, maxFaces, qualityFilter, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteFacesAsync"/>.</summary>
    public static DeleteFacesResult DeleteFaces(string collectionId, List<string> faceIds, RegionEndpoint? region = null)
        => DeleteFacesAsync(collectionId, faceIds, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SearchFacesAsync"/>.</summary>
    public static SearchFacesResult SearchFaces(string collectionId, string faceId, int? maxFaces = null, float? faceMatchThreshold = null, RegionEndpoint? region = null)
        => SearchFacesAsync(collectionId, faceId, maxFaces, faceMatchThreshold, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateCollectionAsync"/>.</summary>
    public static CreateCollectionResult CreateCollection(string collectionId, Dictionary<string, string>? tags = null, RegionEndpoint? region = null)
        => CreateCollectionAsync(collectionId, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteCollectionAsync"/>.</summary>
    public static void DeleteCollection(string collectionId, RegionEndpoint? region = null)
        => DeleteCollectionAsync(collectionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeCollectionAsync"/>.</summary>
    public static DescribeCollectionResult DescribeCollection(string collectionId, RegionEndpoint? region = null)
        => DescribeCollectionAsync(collectionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListCollectionsAsync"/>.</summary>
    public static ListCollectionsResult ListCollections(RegionEndpoint? region = null)
        => ListCollectionsAsync(region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListFacesAsync"/>.</summary>
    public static ListFacesResult ListFaces(string collectionId, RegionEndpoint? region = null)
        => ListFacesAsync(collectionId, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DetectProtectiveEquipmentAsync"/>.</summary>
    public static DetectProtectiveEquipmentResult DetectProtectiveEquipment(Image image, ProtectiveEquipmentSummarizationAttributes? summarizationAttributes = null, RegionEndpoint? region = null)
        => DetectProtectiveEquipmentAsync(image, summarizationAttributes, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartLabelDetectionAsync"/>.</summary>
    public static StartLabelDetectionResult StartLabelDetection(Video video, float? minConfidence = null, NotificationChannel? notificationChannel = null, string? jobTag = null, RegionEndpoint? region = null)
        => StartLabelDetectionAsync(video, minConfidence, notificationChannel, jobTag, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetLabelDetectionAsync"/>.</summary>
    public static GetLabelDetectionResult GetLabelDetection(string jobId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetLabelDetectionAsync(jobId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartFaceDetectionAsync"/>.</summary>
    public static StartFaceDetectionResult StartFaceDetection(Video video, string? faceAttributes = null, NotificationChannel? notificationChannel = null, string? jobTag = null, RegionEndpoint? region = null)
        => StartFaceDetectionAsync(video, faceAttributes, notificationChannel, jobTag, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetFaceDetectionAsync"/>.</summary>
    public static GetFaceDetectionResult GetFaceDetection(string jobId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetFaceDetectionAsync(jobId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartTextDetectionAsync"/>.</summary>
    public static StartTextDetectionResult StartTextDetection(Video video, NotificationChannel? notificationChannel = null, string? jobTag = null, RegionEndpoint? region = null)
        => StartTextDetectionAsync(video, notificationChannel, jobTag, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetTextDetectionAsync"/>.</summary>
    public static GetTextDetectionResult GetTextDetection(string jobId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetTextDetectionAsync(jobId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartCelebrityRecognitionAsync"/>.</summary>
    public static StartCelebrityRecognitionResult StartCelebrityRecognition(Video video, NotificationChannel? notificationChannel = null, string? jobTag = null, RegionEndpoint? region = null)
        => StartCelebrityRecognitionAsync(video, notificationChannel, jobTag, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetCelebrityRecognitionAsync"/>.</summary>
    public static GetCelebrityRecognitionResult GetCelebrityRecognition(string jobId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetCelebrityRecognitionAsync(jobId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartContentModerationAsync"/>.</summary>
    public static StartContentModerationResult StartContentModeration(Video video, float? minConfidence = null, NotificationChannel? notificationChannel = null, string? jobTag = null, RegionEndpoint? region = null)
        => StartContentModerationAsync(video, minConfidence, notificationChannel, jobTag, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetContentModerationAsync"/>.</summary>
    public static GetContentModerationResult GetContentModeration(string jobId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetContentModerationAsync(jobId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartPersonTrackingAsync"/>.</summary>
    public static StartPersonTrackingResult StartPersonTracking(Video video, NotificationChannel? notificationChannel = null, string? jobTag = null, RegionEndpoint? region = null)
        => StartPersonTrackingAsync(video, notificationChannel, jobTag, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetPersonTrackingAsync"/>.</summary>
    public static GetPersonTrackingResult GetPersonTracking(string jobId, int? maxResults = null, string? nextToken = null, RegionEndpoint? region = null)
        => GetPersonTrackingAsync(jobId, maxResults, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static void TagResource(string resourceArn, Dictionary<string, string> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static void UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static ListRekognitionTagsResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateProjectAsync"/>.</summary>
    public static CreateRekognitionProjectResult CreateProject(string projectName, RegionEndpoint? region = null)
        => CreateProjectAsync(projectName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteProjectAsync"/>.</summary>
    public static void DeleteProject(string projectArn, RegionEndpoint? region = null)
        => DeleteProjectAsync(projectArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeProjectsAsync"/>.</summary>
    public static DescribeProjectsResult DescribeProjects(List<string>? projectNames = null, RegionEndpoint? region = null)
        => DescribeProjectsAsync(projectNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="CreateProjectVersionAsync"/>.</summary>
    public static CreateProjectVersionResult CreateProjectVersion(CreateProjectVersionRequest request, RegionEndpoint? region = null)
        => CreateProjectVersionAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteProjectVersionAsync"/>.</summary>
    public static void DeleteProjectVersion(string projectVersionArn, RegionEndpoint? region = null)
        => DeleteProjectVersionAsync(projectVersionArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeProjectVersionsAsync"/>.</summary>
    public static DescribeProjectVersionsResult DescribeProjectVersions(string projectArn, List<string>? versionNames = null, RegionEndpoint? region = null)
        => DescribeProjectVersionsAsync(projectArn, versionNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StartProjectVersionAsync"/>.</summary>
    public static StartProjectVersionResult StartProjectVersion(string projectVersionArn, int minInferenceUnits, int? maxInferenceUnits = null, RegionEndpoint? region = null)
        => StartProjectVersionAsync(projectVersionArn, minInferenceUnits, maxInferenceUnits, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="StopProjectVersionAsync"/>.</summary>
    public static StopProjectVersionResult StopProjectVersion(string projectVersionArn, RegionEndpoint? region = null)
        => StopProjectVersionAsync(projectVersionArn, region).GetAwaiter().GetResult();

}

using Amazon;
using Amazon.CloudWatch;
using Amazon.CloudWatch.Model;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

public sealed record PutMetricDataResult(bool Success = true);

public sealed record GetMetricDataResultRecord(
    List<MetricDataResult>? MetricDataResults = null,
    string? NextToken = null);

public sealed record GetMetricStatisticsResult(
    string? Label = null,
    List<Datapoint>? Datapoints = null);

public sealed record ListMetricsResult(
    List<Metric>? Metrics = null,
    string? NextToken = null);

public sealed record PutMetricAlarmResult(bool Success = true);

public sealed record DeleteAlarmsResult(bool Success = true);

public sealed record DescribeAlarmsResult(
    List<MetricAlarm>? MetricAlarms = null,
    List<CompositeAlarm>? CompositeAlarms = null,
    string? NextToken = null);

public sealed record DescribeAlarmHistoryResult(
    List<AlarmHistoryItem>? AlarmHistoryItems = null,
    string? NextToken = null);

public sealed record EnableAlarmActionsResult(bool Success = true);

public sealed record DisableAlarmActionsResult(bool Success = true);

public sealed record SetAlarmStateResult(bool Success = true);

public sealed record PutDashboardResult(
    List<DashboardValidationMessage>? DashboardValidationMessages = null);

public sealed record GetDashboardResult(
    string? DashboardArn = null,
    string? DashboardBody = null,
    string? DashboardName = null);

public sealed record DeleteDashboardsResult(bool Success = true);

public sealed record ListDashboardsResult(
    List<DashboardEntry>? DashboardEntries = null,
    string? NextToken = null);

public sealed record PutAnomalyDetectorResult(bool Success = true);

public sealed record DeleteAnomalyDetectorResult(bool Success = true);

public sealed record DescribeAnomalyDetectorsResult(
    List<AnomalyDetector>? AnomalyDetectors = null,
    string? NextToken = null);

public sealed record CwTagResourceResult(bool Success = true);

public sealed record CwUntagResourceResult(bool Success = true);

public sealed record CwListTagsForResourceResult(List<Tag>? Tags = null);

public sealed record PutCompositeAlarmResult(bool Success = true);

public sealed record DescribeAlarmsForMetricResult(
    List<MetricAlarm>? MetricAlarms = null);

// ── Service ─────────────────────────────────────────────────────────

/// <summary>
/// Utility helpers for Amazon CloudWatch.
/// </summary>
public static class CloudWatchService
{
    private static AmazonCloudWatchClient GetClient(RegionEndpoint? region = null)
        => ClientFactory.GetClient<AmazonCloudWatchClient>(region);

    /// <summary>
    /// Publish metric data points to CloudWatch.
    /// </summary>
    public static async Task<PutMetricDataResult> PutMetricDataAsync(
        string metricNamespace,
        List<MetricDatum> metricData,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutMetricDataAsync(new PutMetricDataRequest
            {
                Namespace = metricNamespace,
                MetricData = metricData
            });
            return new PutMetricDataResult();
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put metric data");
        }
    }

    /// <summary>
    /// Retrieve metric data using metric queries.
    /// </summary>
    public static async Task<GetMetricDataResultRecord> GetMetricDataAsync(
        List<MetricDataQuery> metricDataQueries,
        DateTime startTimeUtc,
        DateTime endTimeUtc,
        string? nextToken = null,
        ScanBy? scanOrder = null,
        int? maxDatapoints = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetMetricDataRequest
        {
            MetricDataQueries = metricDataQueries,
            StartTime = startTimeUtc,
            EndTime = endTimeUtc
        };
        if (nextToken != null) request.NextToken = nextToken;
        if (scanOrder != null) request.ScanBy = scanOrder;
        if (maxDatapoints.HasValue) request.MaxDatapoints = maxDatapoints.Value;

        try
        {
            var resp = await client.GetMetricDataAsync(request);
            return new GetMetricDataResultRecord(
                MetricDataResults: resp.MetricDataResults,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get metric data");
        }
    }

    /// <summary>
    /// Retrieve statistics for a specific metric.
    /// </summary>
    public static async Task<GetMetricStatisticsResult> GetMetricStatisticsAsync(
        string metricNamespace,
        string metricName,
        DateTime startTimeUtc,
        DateTime endTimeUtc,
        int period,
        List<string>? statistics = null,
        List<string>? extendedStatistics = null,
        List<Dimension>? dimensions = null,
        string? unit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new GetMetricStatisticsRequest
        {
            Namespace = metricNamespace,
            MetricName = metricName,
            StartTime = startTimeUtc,
            EndTime = endTimeUtc,
            Period = period
        };
        if (statistics != null)
            request.Statistics = statistics;
        if (extendedStatistics != null) request.ExtendedStatistics = extendedStatistics;
        if (dimensions != null) request.Dimensions = dimensions;
        if (unit != null) request.Unit = new StandardUnit(unit);

        try
        {
            var resp = await client.GetMetricStatisticsAsync(request);
            return new GetMetricStatisticsResult(
                Label: resp.Label,
                Datapoints: resp.Datapoints);
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to get metric statistics");
        }
    }

    /// <summary>
    /// List available CloudWatch metrics.
    /// </summary>
    public static async Task<ListMetricsResult> ListMetricsAsync(
        string? metricNamespace = null,
        string? metricName = null,
        List<DimensionFilter>? dimensions = null,
        string? nextToken = null,
        string? recentlyActive = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListMetricsRequest();
        if (metricNamespace != null) request.Namespace = metricNamespace;
        if (metricName != null) request.MetricName = metricName;
        if (dimensions != null) request.Dimensions = dimensions;
        if (nextToken != null) request.NextToken = nextToken;
        if (recentlyActive != null) request.RecentlyActive = new RecentlyActive(recentlyActive);

        try
        {
            var resp = await client.ListMetricsAsync(request);
            return new ListMetricsResult(
                Metrics: resp.Metrics,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list metrics");
        }
    }

    /// <summary>
    /// Create or update a CloudWatch metric alarm.
    /// </summary>
    public static async Task<PutMetricAlarmResult> PutMetricAlarmAsync(
        PutMetricAlarmRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutMetricAlarmAsync(request);
            return new PutMetricAlarmResult();
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put metric alarm");
        }
    }

    /// <summary>
    /// Delete one or more CloudWatch alarms.
    /// </summary>
    public static async Task<DeleteAlarmsResult> DeleteAlarmsAsync(
        List<string> alarmNames,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteAlarmsAsync(new DeleteAlarmsRequest { AlarmNames = alarmNames });
            return new DeleteAlarmsResult();
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete alarms");
        }
    }

    /// <summary>
    /// Describe CloudWatch alarms.
    /// </summary>
    public static async Task<DescribeAlarmsResult> DescribeAlarmsAsync(
        List<string>? alarmNames = null,
        string? alarmNamePrefix = null,
        List<string>? alarmTypes = null,
        string? stateValue = null,
        string? actionPrefix = null,
        string? nextToken = null,
        int? maxRecords = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeAlarmsRequest();
        if (alarmNames != null) request.AlarmNames = alarmNames;
        if (alarmNamePrefix != null) request.AlarmNamePrefix = alarmNamePrefix;
        if (alarmTypes != null) request.AlarmTypes = alarmTypes;
        if (stateValue != null) request.StateValue = new StateValue(stateValue);
        if (actionPrefix != null) request.ActionPrefix = actionPrefix;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;

        try
        {
            var resp = await client.DescribeAlarmsAsync(request);
            return new DescribeAlarmsResult(
                MetricAlarms: resp.MetricAlarms,
                CompositeAlarms: resp.CompositeAlarms,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe alarms");
        }
    }

    /// <summary>
    /// Describe alarm history.
    /// </summary>
    public static async Task<DescribeAlarmHistoryResult> DescribeAlarmHistoryAsync(
        string? alarmName = null,
        List<string>? alarmTypes = null,
        string? historyItemType = null,
        DateTime? startDateUtc = null,
        DateTime? endDateUtc = null,
        string? nextToken = null,
        int? maxRecords = null,
        ScanBy? scanBy = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeAlarmHistoryRequest();
        if (alarmName != null) request.AlarmName = alarmName;
        if (alarmTypes != null) request.AlarmTypes = alarmTypes;
        if (historyItemType != null) request.HistoryItemType = new HistoryItemType(historyItemType);
        if (startDateUtc.HasValue) request.StartDate = startDateUtc.Value;
        if (endDateUtc.HasValue) request.EndDate = endDateUtc.Value;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxRecords.HasValue) request.MaxRecords = maxRecords.Value;
        if (scanBy != null) request.ScanBy = scanBy;

        try
        {
            var resp = await client.DescribeAlarmHistoryAsync(request);
            return new DescribeAlarmHistoryResult(
                AlarmHistoryItems: resp.AlarmHistoryItems,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe alarm history");
        }
    }

    /// <summary>
    /// Enable actions for the specified alarms.
    /// </summary>
    public static async Task<EnableAlarmActionsResult> EnableAlarmActionsAsync(
        List<string> alarmNames,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.EnableAlarmActionsAsync(new EnableAlarmActionsRequest
            {
                AlarmNames = alarmNames
            });
            return new EnableAlarmActionsResult();
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to enable alarm actions");
        }
    }

    /// <summary>
    /// Disable actions for the specified alarms.
    /// </summary>
    public static async Task<DisableAlarmActionsResult> DisableAlarmActionsAsync(
        List<string> alarmNames,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DisableAlarmActionsAsync(new DisableAlarmActionsRequest
            {
                AlarmNames = alarmNames
            });
            return new DisableAlarmActionsResult();
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to disable alarm actions");
        }
    }

    /// <summary>
    /// Manually set the state of a CloudWatch alarm.
    /// </summary>
    public static async Task<SetAlarmStateResult> SetAlarmStateAsync(
        string alarmName,
        string stateValue,
        string stateReason,
        string? stateReasonData = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new SetAlarmStateRequest
        {
            AlarmName = alarmName,
            StateValue = new StateValue(stateValue),
            StateReason = stateReason
        };
        if (stateReasonData != null) request.StateReasonData = stateReasonData;

        try
        {
            await client.SetAlarmStateAsync(request);
            return new SetAlarmStateResult();
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to set alarm state for '{alarmName}'");
        }
    }

    /// <summary>
    /// Create or update a CloudWatch dashboard.
    /// </summary>
    public static async Task<PutDashboardResult> PutDashboardAsync(
        string dashboardName,
        string dashboardBody,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.PutDashboardAsync(new PutDashboardRequest
            {
                DashboardName = dashboardName,
                DashboardBody = dashboardBody
            });
            return new PutDashboardResult(
                DashboardValidationMessages: resp.DashboardValidationMessages);
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to put dashboard '{dashboardName}'");
        }
    }

    /// <summary>
    /// Retrieve a CloudWatch dashboard.
    /// </summary>
    public static async Task<GetDashboardResult> GetDashboardAsync(
        string dashboardName,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.GetDashboardAsync(new GetDashboardRequest
            {
                DashboardName = dashboardName
            });
            return new GetDashboardResult(
                DashboardArn: resp.DashboardArn,
                DashboardBody: resp.DashboardBody,
                DashboardName: resp.DashboardName);
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, $"Failed to get dashboard '{dashboardName}'");
        }
    }

    /// <summary>
    /// Delete one or more CloudWatch dashboards.
    /// </summary>
    public static async Task<DeleteDashboardsResult> DeleteDashboardsAsync(
        List<string> dashboardNames,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.DeleteDashboardsAsync(new DeleteDashboardsRequest
            {
                DashboardNames = dashboardNames
            });
            return new DeleteDashboardsResult();
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete dashboards");
        }
    }

    /// <summary>
    /// List CloudWatch dashboards.
    /// </summary>
    public static async Task<ListDashboardsResult> ListDashboardsAsync(
        string? dashboardNamePrefix = null,
        string? nextToken = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new ListDashboardsRequest();
        if (dashboardNamePrefix != null) request.DashboardNamePrefix = dashboardNamePrefix;
        if (nextToken != null) request.NextToken = nextToken;

        try
        {
            var resp = await client.ListDashboardsAsync(request);
            return new ListDashboardsResult(
                DashboardEntries: resp.DashboardEntries,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list dashboards");
        }
    }

    /// <summary>
    /// Create or update an anomaly detector.
    /// </summary>
    public static async Task<PutAnomalyDetectorResult> PutAnomalyDetectorAsync(
        SingleMetricAnomalyDetector? singleMetricAnomalyDetector = null,
        MetricMathAnomalyDetector? metricMathAnomalyDetector = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new PutAnomalyDetectorRequest();
        if (singleMetricAnomalyDetector != null)
            request.SingleMetricAnomalyDetector = singleMetricAnomalyDetector;
        if (metricMathAnomalyDetector != null)
            request.MetricMathAnomalyDetector = metricMathAnomalyDetector;

        try
        {
            await client.PutAnomalyDetectorAsync(request);
            return new PutAnomalyDetectorResult();
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put anomaly detector");
        }
    }

    /// <summary>
    /// Delete an anomaly detector.
    /// </summary>
    public static async Task<DeleteAnomalyDetectorResult> DeleteAnomalyDetectorAsync(
        SingleMetricAnomalyDetector? singleMetricAnomalyDetector = null,
        MetricMathAnomalyDetector? metricMathAnomalyDetector = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DeleteAnomalyDetectorRequest();
        if (singleMetricAnomalyDetector != null)
            request.SingleMetricAnomalyDetector = singleMetricAnomalyDetector;
        if (metricMathAnomalyDetector != null)
            request.MetricMathAnomalyDetector = metricMathAnomalyDetector;

        try
        {
            await client.DeleteAnomalyDetectorAsync(request);
            return new DeleteAnomalyDetectorResult();
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to delete anomaly detector");
        }
    }

    /// <summary>
    /// Describe anomaly detectors.
    /// </summary>
    public static async Task<DescribeAnomalyDetectorsResult> DescribeAnomalyDetectorsAsync(
        string? metricNamespace = null,
        string? metricName = null,
        List<Dimension>? dimensions = null,
        List<string>? anomalyDetectorTypes = null,
        string? nextToken = null,
        int? maxResults = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeAnomalyDetectorsRequest();
        if (metricNamespace != null) request.Namespace = metricNamespace;
        if (metricName != null) request.MetricName = metricName;
        if (dimensions != null) request.Dimensions = dimensions;
        if (anomalyDetectorTypes != null)
            request.AnomalyDetectorTypes = anomalyDetectorTypes;
        if (nextToken != null) request.NextToken = nextToken;
        if (maxResults.HasValue) request.MaxResults = maxResults.Value;

        try
        {
            var resp = await client.DescribeAnomalyDetectorsAsync(request);
            return new DescribeAnomalyDetectorsResult(
                AnomalyDetectors: resp.AnomalyDetectors,
                NextToken: resp.NextToken);
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe anomaly detectors");
        }
    }

    /// <summary>
    /// Add tags to a CloudWatch resource.
    /// </summary>
    public static async Task<CwTagResourceResult> TagResourceAsync(
        string resourceArn,
        List<Tag> tags,
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
            return new CwTagResourceResult();
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to tag resource");
        }
    }

    /// <summary>
    /// Remove tags from a CloudWatch resource.
    /// </summary>
    public static async Task<CwUntagResourceResult> UntagResourceAsync(
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
            return new CwUntagResourceResult();
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to untag resource");
        }
    }

    /// <summary>
    /// List tags for a CloudWatch resource.
    /// </summary>
    public static async Task<CwListTagsForResourceResult> ListTagsForResourceAsync(
        string resourceArn,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            var resp = await client.ListTagsForResourceAsync(new ListTagsForResourceRequest
            {
                ResourceARN = resourceArn
            });
            return new CwListTagsForResourceResult(Tags: resp.Tags);
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to list tags for resource");
        }
    }

    /// <summary>
    /// Create or update a composite alarm.
    /// </summary>
    public static async Task<PutCompositeAlarmResult> PutCompositeAlarmAsync(
        PutCompositeAlarmRequest request,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        try
        {
            await client.PutCompositeAlarmAsync(request);
            return new PutCompositeAlarmResult();
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to put composite alarm");
        }
    }

    /// <summary>
    /// Describe alarms for a specific metric.
    /// </summary>
    public static async Task<DescribeAlarmsForMetricResult> DescribeAlarmsForMetricAsync(
        string metricNamespace,
        string metricName,
        List<Dimension>? dimensions = null,
        string? statistic = null,
        string? extendedStatistic = null,
        int? period = null,
        string? unit = null,
        RegionEndpoint? region = null)
    {
        var client = GetClient(region);
        var request = new DescribeAlarmsForMetricRequest
        {
            Namespace = metricNamespace,
            MetricName = metricName
        };
        if (dimensions != null) request.Dimensions = dimensions;
        if (statistic != null) request.Statistic = new Statistic(statistic);
        if (extendedStatistic != null) request.ExtendedStatistic = extendedStatistic;
        if (period.HasValue) request.Period = period.Value;
        if (unit != null) request.Unit = new StandardUnit(unit);

        try
        {
            var resp = await client.DescribeAlarmsForMetricAsync(request);
            return new DescribeAlarmsForMetricResult(MetricAlarms: resp.MetricAlarms);
        }
        catch (AmazonCloudWatchException exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Failed to describe alarms for metric");
        }
    }


    // -----------------------------------------------------------------------
    // Synchronous wrappers
    // -----------------------------------------------------------------------

    /// <summary>Synchronous wrapper for <see cref="PutMetricDataAsync"/>.</summary>
    public static PutMetricDataResult PutMetricData(string metricNamespace, List<MetricDatum> metricData, RegionEndpoint? region = null)
        => PutMetricDataAsync(metricNamespace, metricData, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetMetricDataAsync"/>.</summary>
    public static GetMetricDataResultRecord GetMetricData(List<MetricDataQuery> metricDataQueries, DateTime startTimeUtc, DateTime endTimeUtc, string? nextToken = null, ScanBy? scanOrder = null, int? maxDatapoints = null, RegionEndpoint? region = null)
        => GetMetricDataAsync(metricDataQueries, startTimeUtc, endTimeUtc, nextToken, scanOrder, maxDatapoints, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetMetricStatisticsAsync"/>.</summary>
    public static GetMetricStatisticsResult GetMetricStatistics(string metricNamespace, string metricName, DateTime startTimeUtc, DateTime endTimeUtc, int period, List<string>? statistics = null, List<string>? extendedStatistics = null, List<Dimension>? dimensions = null, string? unit = null, RegionEndpoint? region = null)
        => GetMetricStatisticsAsync(metricNamespace, metricName, startTimeUtc, endTimeUtc, period, statistics, extendedStatistics, dimensions, unit, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListMetricsAsync"/>.</summary>
    public static ListMetricsResult ListMetrics(string? metricNamespace = null, string? metricName = null, List<DimensionFilter>? dimensions = null, string? nextToken = null, string? recentlyActive = null, RegionEndpoint? region = null)
        => ListMetricsAsync(metricNamespace, metricName, dimensions, nextToken, recentlyActive, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutMetricAlarmAsync"/>.</summary>
    public static PutMetricAlarmResult PutMetricAlarm(PutMetricAlarmRequest request, RegionEndpoint? region = null)
        => PutMetricAlarmAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAlarmsAsync"/>.</summary>
    public static DeleteAlarmsResult DeleteAlarms(List<string> alarmNames, RegionEndpoint? region = null)
        => DeleteAlarmsAsync(alarmNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAlarmsAsync"/>.</summary>
    public static DescribeAlarmsResult DescribeAlarms(List<string>? alarmNames = null, string? alarmNamePrefix = null, List<string>? alarmTypes = null, string? stateValue = null, string? actionPrefix = null, string? nextToken = null, int? maxRecords = null, RegionEndpoint? region = null)
        => DescribeAlarmsAsync(alarmNames, alarmNamePrefix, alarmTypes, stateValue, actionPrefix, nextToken, maxRecords, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAlarmHistoryAsync"/>.</summary>
    public static DescribeAlarmHistoryResult DescribeAlarmHistory(string? alarmName = null, List<string>? alarmTypes = null, string? historyItemType = null, DateTime? startDateUtc = null, DateTime? endDateUtc = null, string? nextToken = null, int? maxRecords = null, ScanBy? scanBy = null, RegionEndpoint? region = null)
        => DescribeAlarmHistoryAsync(alarmName, alarmTypes, historyItemType, startDateUtc, endDateUtc, nextToken, maxRecords, scanBy, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="EnableAlarmActionsAsync"/>.</summary>
    public static EnableAlarmActionsResult EnableAlarmActions(List<string> alarmNames, RegionEndpoint? region = null)
        => EnableAlarmActionsAsync(alarmNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DisableAlarmActionsAsync"/>.</summary>
    public static DisableAlarmActionsResult DisableAlarmActions(List<string> alarmNames, RegionEndpoint? region = null)
        => DisableAlarmActionsAsync(alarmNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="SetAlarmStateAsync"/>.</summary>
    public static SetAlarmStateResult SetAlarmState(string alarmName, string stateValue, string stateReason, string? stateReasonData = null, RegionEndpoint? region = null)
        => SetAlarmStateAsync(alarmName, stateValue, stateReason, stateReasonData, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutDashboardAsync"/>.</summary>
    public static PutDashboardResult PutDashboard(string dashboardName, string dashboardBody, RegionEndpoint? region = null)
        => PutDashboardAsync(dashboardName, dashboardBody, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="GetDashboardAsync"/>.</summary>
    public static GetDashboardResult GetDashboard(string dashboardName, RegionEndpoint? region = null)
        => GetDashboardAsync(dashboardName, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteDashboardsAsync"/>.</summary>
    public static DeleteDashboardsResult DeleteDashboards(List<string> dashboardNames, RegionEndpoint? region = null)
        => DeleteDashboardsAsync(dashboardNames, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListDashboardsAsync"/>.</summary>
    public static ListDashboardsResult ListDashboards(string? dashboardNamePrefix = null, string? nextToken = null, RegionEndpoint? region = null)
        => ListDashboardsAsync(dashboardNamePrefix, nextToken, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutAnomalyDetectorAsync"/>.</summary>
    public static PutAnomalyDetectorResult PutAnomalyDetector(SingleMetricAnomalyDetector? singleMetricAnomalyDetector = null, MetricMathAnomalyDetector? metricMathAnomalyDetector = null, RegionEndpoint? region = null)
        => PutAnomalyDetectorAsync(singleMetricAnomalyDetector, metricMathAnomalyDetector, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DeleteAnomalyDetectorAsync"/>.</summary>
    public static DeleteAnomalyDetectorResult DeleteAnomalyDetector(SingleMetricAnomalyDetector? singleMetricAnomalyDetector = null, MetricMathAnomalyDetector? metricMathAnomalyDetector = null, RegionEndpoint? region = null)
        => DeleteAnomalyDetectorAsync(singleMetricAnomalyDetector, metricMathAnomalyDetector, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAnomalyDetectorsAsync"/>.</summary>
    public static DescribeAnomalyDetectorsResult DescribeAnomalyDetectors(string? metricNamespace = null, string? metricName = null, List<Dimension>? dimensions = null, List<string>? anomalyDetectorTypes = null, string? nextToken = null, int? maxResults = null, RegionEndpoint? region = null)
        => DescribeAnomalyDetectorsAsync(metricNamespace, metricName, dimensions, anomalyDetectorTypes, nextToken, maxResults, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="TagResourceAsync"/>.</summary>
    public static CwTagResourceResult TagResource(string resourceArn, List<Tag> tags, RegionEndpoint? region = null)
        => TagResourceAsync(resourceArn, tags, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="UntagResourceAsync"/>.</summary>
    public static CwUntagResourceResult UntagResource(string resourceArn, List<string> tagKeys, RegionEndpoint? region = null)
        => UntagResourceAsync(resourceArn, tagKeys, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="ListTagsForResourceAsync"/>.</summary>
    public static CwListTagsForResourceResult ListTagsForResource(string resourceArn, RegionEndpoint? region = null)
        => ListTagsForResourceAsync(resourceArn, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="PutCompositeAlarmAsync"/>.</summary>
    public static PutCompositeAlarmResult PutCompositeAlarm(PutCompositeAlarmRequest request, RegionEndpoint? region = null)
        => PutCompositeAlarmAsync(request, region).GetAwaiter().GetResult();

    /// <summary>Synchronous wrapper for <see cref="DescribeAlarmsForMetricAsync"/>.</summary>
    public static DescribeAlarmsForMetricResult DescribeAlarmsForMetric(string metricNamespace, string metricName, List<Dimension>? dimensions = null, string? statistic = null, string? extendedStatistic = null, int? period = null, string? unit = null, RegionEndpoint? region = null)
        => DescribeAlarmsForMetricAsync(metricNamespace, metricName, dimensions, statistic, extendedStatistic, period, unit, region).GetAwaiter().GetResult();

}

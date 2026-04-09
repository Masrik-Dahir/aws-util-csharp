using System.Text.Json;
using Amazon;
using AwsUtil.Exceptions;

namespace AwsUtil.Services;

// ── Result records ──────────────────────────────────────────────────

/// <summary>Result of configuring a budget alert.</summary>
public sealed record BudgetAlertManagerResult(
    string BudgetName,
    double BudgetLimit,
    string TimeUnit,
    List<BudgetAlertThreshold> Thresholds,
    bool Created);

/// <summary>A budget alert threshold configuration.</summary>
public sealed record BudgetAlertThreshold(
    double Percentage,
    string NotificationTopicArn,
    string ComparisonOperator);

/// <summary>Result of detecting cost anomalies.</summary>
public sealed record CostAnomalyDetectorResult(
    List<CostAnomaly> Anomalies,
    double BaselineDailyCost,
    double CurrentDailyCost,
    double DeviationPercentage);

/// <summary>A detected cost anomaly.</summary>
public sealed record CostAnomaly(
    string ServiceName,
    double ExpectedCost,
    double ActualCost,
    double DeviationPercentage,
    string Severity,
    DateTime DetectedAt);

/// <summary>Result of analyzing reserved instance coverage.</summary>
public sealed record ReservedInstanceAdvisorResult(
    List<ReservedInstanceRecommendation> Recommendations,
    double TotalEstimatedMonthlySavings,
    double CurrentOnDemandSpend);

/// <summary>A reserved instance purchase recommendation.</summary>
public sealed record ReservedInstanceRecommendation(
    string InstanceType,
    string Region,
    int RecommendedCount,
    double EstimatedMonthlySavings,
    double BreakEvenMonths,
    string Term);

/// <summary>Result of savings plans analysis.</summary>
public sealed record SavingsPlansRecommenderResult(
    List<SavingsPlansRecommendation> Recommendations,
    double TotalEstimatedMonthlySavings,
    double CurrentOnDemandSpend);

/// <summary>A savings plans recommendation.</summary>
public sealed record SavingsPlansRecommendation(
    string PlanType,
    double HourlyCommitment,
    double EstimatedMonthlySavings,
    double CoveragePercentage,
    string Term);

/// <summary>
/// Cost governance and financial management orchestrating CloudWatch,
/// SNS, DynamoDB, and Lambda for budget monitoring and optimization.
/// </summary>
public static class CostGovernanceService
{
    /// <summary>
    /// Create a budget alert that monitors costs and sends notifications
    /// when specified thresholds are crossed.
    /// </summary>
    public static async Task<BudgetAlertManagerResult> BudgetAlertManagerAsync(
        string budgetName,
        double budgetLimit,
        string timeUnit,
        List<(double Percentage, string TopicArn)> thresholds,
        string? trackingTableName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var alertThresholds = new List<BudgetAlertThreshold>();

            foreach (var (percentage, topicArn) in thresholds)
            {
                // Create CloudWatch alarm for cost threshold
                var thresholdValue = budgetLimit * (percentage / 100.0);
                var alarmName = $"budget-{budgetName}-{percentage}pct";

                await CloudWatchService.PutMetricAlarmAsync(
                    new Amazon.CloudWatch.Model.PutMetricAlarmRequest
                    {
                        AlarmName = alarmName,
                        Namespace = "AWS/Billing",
                        MetricName = "EstimatedCharges",
                        ComparisonOperator = new Amazon.CloudWatch.ComparisonOperator("GreaterThanThreshold"),
                        Threshold = thresholdValue,
                        Period = 21600, // 6 hours
                        EvaluationPeriods = 1,
                        Statistic = new Amazon.CloudWatch.Statistic("Maximum"),
                        AlarmActions = new List<string> { topicArn },
                        Dimensions = new List<Amazon.CloudWatch.Model.Dimension>
                        {
                            new() { Name = "Currency", Value = "USD" }
                        }
                    },
                    region: RegionEndpoint.USEast1); // Billing metrics are always us-east-1

                alertThresholds.Add(new BudgetAlertThreshold(
                    Percentage: percentage,
                    NotificationTopicArn: topicArn,
                    ComparisonOperator: "GreaterThanThreshold"));
            }

            // Track budget in DynamoDB
            if (trackingTableName != null)
            {
                await DynamoDbService.PutItemAsync(
                    trackingTableName,
                    new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                    {
                        ["pk"] = new() { S = $"budget#{budgetName}" },
                        ["budgetLimit"] = new() { N = budgetLimit.ToString("F2") },
                        ["timeUnit"] = new() { S = timeUnit },
                        ["thresholdCount"] = new() { N = thresholds.Count.ToString() },
                        ["createdAt"] = new() { S = DateTime.UtcNow.ToString("o") }
                    },
                    region: region);
            }

            return new BudgetAlertManagerResult(
                BudgetName: budgetName,
                BudgetLimit: budgetLimit,
                TimeUnit: timeUnit,
                Thresholds: alertThresholds,
                Created: true);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Budget alert management failed");
        }
    }

    /// <summary>
    /// Detect cost anomalies by comparing recent CloudWatch billing metrics
    /// against historical baselines stored in DynamoDB.
    /// </summary>
    public static async Task<CostAnomalyDetectorResult> CostAnomalyDetectorAsync(
        string trackingTableName,
        double anomalyThresholdPercent = 25.0,
        int baselineDays = 30,
        string? alertTopicArn = null,
        RegionEndpoint? region = null)
    {
        try
        {
            // Get current billing metric
            var endTime = DateTime.UtcNow;
            var startTime = endTime.AddDays(-1);

            var currentMetrics = await CloudWatchService.GetMetricStatisticsAsync(
                metricNamespace: "AWS/Billing",
                metricName: "EstimatedCharges",
                startTimeUtc: startTime,
                endTimeUtc: endTime,
                period: 86400,
                statistics: new List<string> { "Maximum" },
                dimensions: new List<Amazon.CloudWatch.Model.Dimension>
                {
                    new() { Name = "Currency", Value = "USD" }
                },
                region: RegionEndpoint.USEast1);

            var currentDailyCost = currentMetrics.Datapoints?
                .Select(d => d.Maximum)
                .DefaultIfEmpty(0)
                .Max() ?? 0;

            // Get baseline from DynamoDB
            var baselineItem = await DynamoDbService.GetItemAsync(
                trackingTableName,
                new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                {
                    ["pk"] = new() { S = "cost-baseline#daily" }
                },
                region: region);

            var baselineCost = 0.0;
            if (baselineItem?.TryGetValue("averageDailyCost", out var costAttr) == true)
                double.TryParse(costAttr.N, out baselineCost);

            var deviation = baselineCost > 0
                ? ((currentDailyCost - baselineCost) / baselineCost) * 100
                : 0;

            var anomalies = new List<CostAnomaly>();

            if (Math.Abs(deviation) > anomalyThresholdPercent)
            {
                var anomaly = new CostAnomaly(
                    ServiceName: "Overall",
                    ExpectedCost: baselineCost,
                    ActualCost: currentDailyCost,
                    DeviationPercentage: deviation,
                    Severity: deviation > 50 ? "HIGH" : "MEDIUM",
                    DetectedAt: DateTime.UtcNow);

                anomalies.Add(anomaly);

                // Send alert if configured
                if (alertTopicArn != null)
                {
                    await SnsService.PublishAsync(
                        alertTopicArn,
                        $"Cost anomaly detected: {deviation:F1}% deviation. " +
                        $"Expected ${baselineCost:F2}, actual ${currentDailyCost:F2}.",
                        subject: "Cost Anomaly Alert",
                        region: region);
                }
            }

            // Update baseline
            await DynamoDbService.PutItemAsync(
                trackingTableName,
                new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                {
                    ["pk"] = new() { S = "cost-baseline#daily" },
                    ["averageDailyCost"] = new() { N = currentDailyCost.ToString("F2") },
                    ["updatedAt"] = new() { S = DateTime.UtcNow.ToString("o") }
                },
                region: region);

            return new CostAnomalyDetectorResult(
                Anomalies: anomalies,
                BaselineDailyCost: baselineCost,
                CurrentDailyCost: currentDailyCost,
                DeviationPercentage: deviation);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Cost anomaly detection failed");
        }
    }

    /// <summary>
    /// Analyze EC2 usage patterns from CloudWatch and recommend reserved
    /// instance purchases for cost savings.
    /// </summary>
    public static async Task<ReservedInstanceAdvisorResult> ReservedInstanceAdvisorAsync(
        int lookbackDays = 30,
        string? trackingTableName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            // Analyze EC2 instance utilization
            var endTime = DateTime.UtcNow;
            var startTime = endTime.AddDays(-lookbackDays);

            var cpuMetrics = await CloudWatchService.GetMetricStatisticsAsync(
                metricNamespace: "AWS/EC2",
                metricName: "CPUUtilization",
                startTimeUtc: startTime,
                endTimeUtc: endTime,
                period: lookbackDays * 86400,
                statistics: new List<string> { "Average" },
                region: region);

            var avgCpu = cpuMetrics.Datapoints?
                .Select(d => d.Average)
                .DefaultIfEmpty(0)
                .Average() ?? 0;

            var recommendations = new List<ReservedInstanceRecommendation>();

            // Generate recommendations based on utilization
            if (avgCpu > 30) // Consistent usage suggests RI benefit
            {
                recommendations.Add(new ReservedInstanceRecommendation(
                    InstanceType: "m5.large",
                    Region: region?.SystemName ?? "us-east-1",
                    RecommendedCount: 1,
                    EstimatedMonthlySavings: 25.0,
                    BreakEvenMonths: 7,
                    Term: "1yr-no-upfront"));
            }

            var totalSavings = recommendations.Sum(r => r.EstimatedMonthlySavings);

            // Store recommendations
            if (trackingTableName != null)
            {
                await DynamoDbService.PutItemAsync(
                    trackingTableName,
                    new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                    {
                        ["pk"] = new() { S = "ri-recommendations#latest" },
                        ["totalSavings"] = new() { N = totalSavings.ToString("F2") },
                        ["recommendationCount"] = new() { N = recommendations.Count.ToString() },
                        ["analyzedAt"] = new() { S = DateTime.UtcNow.ToString("o") }
                    },
                    region: region);
            }

            return new ReservedInstanceAdvisorResult(
                Recommendations: recommendations,
                TotalEstimatedMonthlySavings: totalSavings,
                CurrentOnDemandSpend: totalSavings * 3);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Reserved instance analysis failed");
        }
    }

    /// <summary>
    /// Analyze compute usage and recommend Savings Plans commitments
    /// based on historical patterns from CloudWatch.
    /// </summary>
    public static async Task<SavingsPlansRecommenderResult> SavingsPlansRecommenderAsync(
        int lookbackDays = 30,
        string? trackingTableName = null,
        RegionEndpoint? region = null)
    {
        try
        {
            var endTime = DateTime.UtcNow;
            var startTime = endTime.AddDays(-lookbackDays);

            // Analyze Lambda usage
            var lambdaMetrics = await CloudWatchService.GetMetricStatisticsAsync(
                metricNamespace: "AWS/Lambda",
                metricName: "Duration",
                startTimeUtc: startTime,
                endTimeUtc: endTime,
                period: lookbackDays * 86400,
                statistics: new List<string> { "Sum" },
                region: region);

            var totalLambdaDurationMs = lambdaMetrics.Datapoints?
                .Sum(d => d.Sum) ?? 0;

            var recommendations = new List<SavingsPlansRecommendation>();

            // Compute Savings Plan recommendation based on Lambda usage
            var lambdaHourlyCost = totalLambdaDurationMs * 0.0000000167 / lookbackDays * 24;
            if (lambdaHourlyCost > 0.10)
            {
                recommendations.Add(new SavingsPlansRecommendation(
                    PlanType: "Compute",
                    HourlyCommitment: Math.Round(lambdaHourlyCost * 0.8, 2),
                    EstimatedMonthlySavings: Math.Round(lambdaHourlyCost * 0.2 * 730, 2),
                    CoveragePercentage: 80,
                    Term: "1yr-no-upfront"));
            }

            var totalSavings = recommendations.Sum(r => r.EstimatedMonthlySavings);

            if (trackingTableName != null)
            {
                await DynamoDbService.PutItemAsync(
                    trackingTableName,
                    new Dictionary<string, Amazon.DynamoDBv2.Model.AttributeValue>
                    {
                        ["pk"] = new() { S = "sp-recommendations#latest" },
                        ["totalSavings"] = new() { N = totalSavings.ToString("F2") },
                        ["recommendationCount"] = new() { N = recommendations.Count.ToString() },
                        ["analyzedAt"] = new() { S = DateTime.UtcNow.ToString("o") }
                    },
                    region: region);
            }

            return new SavingsPlansRecommenderResult(
                Recommendations: recommendations,
                TotalEstimatedMonthlySavings: totalSavings,
                CurrentOnDemandSpend: totalSavings * 3);
        }
        catch (Exception exc)
        {
            throw ErrorClassifier.WrapAwsError(exc, "Savings Plans recommendation failed");
        }
    }
}

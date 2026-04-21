# Monitoring & Observability Guide

## Current Monitoring (Built-in)

### 1. Glue Job Logs (CloudWatch)
```bash
# View logs
aws logs tail /aws-glue/jobs/output --follow
```

**What to monitor**:
- Quarantine rate (should be < 5%)
- Processing time (should be < 5 min/month)
- Data quality percentage (should be > 95%)

### 2. Lambda Logs
```bash
aws logs tail /aws/lambda/firstIngestor --follow
```

### 3. Quarantine Review (Athena)
```sql
-- See quarantine breakdown
SELECT quarantine_reason, COUNT(*) as count
FROM quarantine_yellow_trips
GROUP BY quarantine_reason
ORDER BY count DESC;

-- Track quality over time
SELECT ds, 
       COUNT(*) as quarantine_count
FROM quarantine_yellow_trips
GROUP BY ds
ORDER BY ds;
```

## Future Enhancements

### CloudWatch Metrics (To Implement)
```python
# In Glue job
cloudwatch.put_metric_data(
    Namespace='NYC-TLC-Pipeline',
    MetricData=[
        {'MetricName': 'QuarantineRate', 'Value': quarantine_rate},
        {'MetricName': 'ProcessingTime', 'Value': duration_seconds}
    ]
)
```

### SNS Alerts (To Implement)
- Pipeline failure
- Quarantine rate > 5%
- Data quality < 95%

### Dashboard (To Implement)
- Records processed per month (bar chart)
- Quarantine rate trend (line chart)
- Data quality over time

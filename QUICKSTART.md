# Quick Start Guide

## Prerequisites
- AWS Account with credentials configured
- Terraform >= 1.0
- Python 3.11
- Apache Airflow 2.x

## 5-Minute Deployment

### 1. Deploy Infrastructure
```bash
cd terraform/
terraform init
terraform apply
```

### 2. Deploy Airflow DAG
```bash
cp airflow/dags/nyc_tlc_pipeline.py $AIRFLOW_HOME/dags/
```

### 3. Trigger for One Month (Testing)
```bash
airflow dags trigger nyc_tlc_pipeline --conf '{"month": "2024-01"}'
```

### 4. Backfill 2024 Data
```bash
airflow dags backfill nyc_tlc_pipeline \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

### 5. Run Crawler & Query
```bash
aws glue start-crawler --name structured-yellow-trips-crawler

# Wait ~2 minutes, then in Athena:
SELECT COUNT(*) FROM yellow_trips;
```

## Review Quarantine

```sql
SELECT quarantine_reason, COUNT(*) as count
FROM quarantine_yellow_trips
GROUP BY quarantine_reason
ORDER BY count DESC;
```

## Troubleshooting

**Lambda returns 404**: Data not yet available (normal - pipeline skips)  
**Glue job fails**: Check CloudWatch logs for specific error  
**Athena table not found**: Run `MSCK REPAIR TABLE yellow_trips;`

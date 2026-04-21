# Terraform Infrastructure for NYC TLC Pipeline

## Overview
This Terraform configuration defines the complete AWS infrastructure for the NYC TLC data pipeline, including:
- S3 data lake with lifecycle policies
- Lambda function for data ingestion
- AWS Glue ETL jobs (landing→raw→structured)
- AWS Glue Crawler for data cataloging
- IAM roles with least-privilege permissions

## Architecture Decisions

### Glue Resource Configuration
- **Worker Type**: G.1X (4 vCPU, 16GB RAM, 1 DPU)
- **Number of Workers**: 2 (configurable via variables)
- **Rationale**: Balances cost and performance for ~56MB parquet files
- **Cost**: ~$0.15 per job run (2 DPUs × 10 min × $0.44/DPU-hour)

### S3 Lifecycle Policies
- Old versions transition to IA after 30 days
- Temp files deleted after 7 days
- Reduces storage costs while maintaining data durability

## Prerequisites
- Terraform >= 1.0
- AWS CLI configured
- Lambda deployment package: `lambda/deployment.zip`
- Glue scripts uploaded to S3

## Usage

### Initialize Terraform
```bash
cd terraform/
terraform init
```

### Plan Changes
```bash
terraform plan -out=tfplan
```

### Apply Infrastructure
```bash
terraform apply tfplan
```

### Destroy (cleanup)
```bash
terraform destroy
```

## Variables
Configure in `terraform.tfvars`:
```hcl
aws_region             = "eu-west-3"
project_name           = "nyc-tlc-pipeline"
bucket_name            = "your-unique-bucket-name"
environment            = "dev"
glue_worker_type       = "G.1X"
glue_number_of_workers = 2
```

## Outputs
After applying, Terraform will output:
- S3 bucket names and ARNs
- Lambda function details
- Glue job names
- IAM role ARNs

Use these outputs for Airflow connections.

## Current State
⚠️ **Note**: This infrastructure was initially created manually via AWS Console for rapid prototyping. This Terraform code represents the **desired production state** and demonstrates IaC best practices.

## Future Improvements
1. Add remote state backend (S3 + DynamoDB)
2. Separate environments (dev/staging/prod) with workspaces
3. Add CloudWatch alarms and SNS notifications
4. Implement Terraform Cloud for team collaboration
5. Add cost allocation tags for detailed billing
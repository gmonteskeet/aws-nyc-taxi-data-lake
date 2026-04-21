terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Data sources for dynamic values
data "aws_caller_identity" "current" {}
data "aws_region" "current" {}

# S3 Bucket for Data Lake
resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name
  
  tags = {
    Name        = "NYC TLC Data Lake"
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id
  
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "data_lake" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    id     = "transition-old-versions"
    status = "Enabled"

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
  
  rule {
    id     = "delete-temp-files"
    status = "Enabled"
    
    filter {
      prefix = "temp/"
    }
    
    expiration {
      days = 7
    }
  }
}

# Lambda Function
resource "aws_lambda_function" "first_ingestor" {
  filename         = "${path.module}/../lambda/deployment.zip"
  function_name    = "${var.project_name}-first-ingestor"
  role            = aws_iam_role.lambda_role.arn
  handler         = "lambda_function.lambda_handler"
  source_code_hash = filebase64sha256("${path.module}/../lambda/deployment.zip")
  runtime         = "python3.11"
  timeout         = 300
  memory_size     = 512

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.data_lake.bucket
    }
  }

  tags = {
    Name    = "First Ingestor Lambda"
    Project = var.project_name
  }
}

# Glue Catalog Database
resource "aws_glue_catalog_database" "nyc_tlc" {
  name = "${replace(var.project_name, "-", "_")}_db"
  
  description = "NYC TLC Taxi Trip Data"
}

# Glue Job: Landing to Raw
resource "aws_glue_job" "landing_to_raw" {
  name     = "${var.project_name}-landing-to-raw"
  role_arn = aws_iam_role.glue_role.arn

  # Resource Configuration
  glue_version      = "5.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 30
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/landing_to_raw.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.bucket}/temp/"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.data_lake.bucket}/spark-logs/"
    "--enable-job-insights"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option"              = "job-bookmark-disable"
  }

  tags = {
    Name    = "Landing to Raw ETL"
    Project = var.project_name
  }
}

# Glue Job: Raw to Structured
resource "aws_glue_job" "raw_to_structured" {
  name     = "${var.project_name}-raw-to-structured"
  role_arn = aws_iam_role.glue_role.arn

  glue_version      = "5.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 30
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.bucket}/scripts/raw_to_structured.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_lake.bucket}/temp/"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }

  tags = {
    Name    = "Raw to Structured ETL"
    Project = var.project_name
  }
}

# S3 Bucket for Glue Scripts
resource "aws_s3_bucket" "glue_scripts" {
  bucket = "${var.bucket_name}-glue-assets"
  
  tags = {
    Name    = "Glue Scripts Bucket"
    Project = var.project_name
  }
}

# Glue Crawler
resource "aws_glue_crawler" "yellow_trips" {
  name          = "${var.project_name}-yellow-trips-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.nyc_tlc.name

  s3_target {
    path = "s3://${aws_s3_bucket.data_lake.bucket}/structured/yellow-trips/"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  tags = {
    Name    = "Yellow Trips Crawler"
    Project = var.project_name
  }
}
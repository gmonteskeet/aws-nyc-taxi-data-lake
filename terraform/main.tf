# NYC TLC Data Lake - Terraform Configuration
# Simplified for interview portfolio

terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# S3 Bucket
resource "aws_s3_bucket" "data_lake" {
  bucket = var.bucket_name
  tags = {
    Name    = "NYC TLC Data Lake"
    Project = "nyc-tlc-pipeline"
  }
}

# Lambda IAM Role
resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}-lambda-role"
  
assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# Glue IAM Role
resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

# Glue Catalog Database
resource "aws_glue_catalog_database" "nyc_tlc_db" {
  name = "${var.project_name}_database"
}

# See AWS docs for complete IAM policies, Glue jobs, crawlers, etc.

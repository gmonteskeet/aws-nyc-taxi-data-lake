variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "eu-west-3"
}

variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "nyc-tlc-pipeline"
}

variable "bucket_name" {
  description = "S3 bucket name for data lake"
  type        = string
  default     = "peruvianbucket"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "glue_worker_type" {
  description = "Glue worker type (G.1X, G.2X, etc)"
  type        = string
  default     = "G.1X"
  
  validation {
    condition     = contains(["G.1X", "G.2X", "G.4X", "G.8X"], var.glue_worker_type)
    error_message = "Worker type must be G.1X, G.2X, G.4X, or G.8X"
  }
}

variable "glue_number_of_workers" {
  description = "Number of Glue workers (DPUs)"
  type        = number
  default     = 2
  
  validation {
    condition     = var.glue_number_of_workers >= 2 && var.glue_number_of_workers <= 100
    error_message = "Number of workers must be between 2 and 100"
  }
}
output "s3_bucket_name" {
  description = "Data lake S3 bucket name"
  value       = aws_s3_bucket.data_lake.id
}

output "s3_bucket_arn" {
  description = "Data lake S3 bucket ARN"
  value       = aws_s3_bucket.data_lake.arn
}

output "lambda_function_name" {
  description = "Lambda function name"
  value       = aws_lambda_function.first_ingestor.function_name
}

output "lambda_function_arn" {
  description = "Lambda function ARN"
  value       = aws_lambda_function.first_ingestor.arn
}

output "glue_database_name" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.nyc_tlc.name
}

output "glue_landing_to_raw_job_name" {
  description = "Glue landing to raw job name"
  value       = aws_glue_job.landing_to_raw.name
}

output "glue_raw_to_structured_job_name" {
  description = "Glue raw to structured job name"
  value       = aws_glue_job.raw_to_structured.name
}

output "glue_crawler_name" {
  description = "Glue crawler name"
  value       = aws_glue_crawler.yellow_trips.name
}

output "glue_iam_role_arn" {
  description = "Glue IAM role ARN"
  value       = aws_iam_role.glue_role.arn
}
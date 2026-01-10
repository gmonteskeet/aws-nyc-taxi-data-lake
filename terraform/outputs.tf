output "bucket_name" {
  value = aws_s3_bucket.data_lake.id
}

output "glue_database" {
  value = aws_glue_catalog_database.nyc_tlc_db.name
}

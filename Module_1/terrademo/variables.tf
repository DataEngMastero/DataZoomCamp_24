variable "credentials" {
  description = "My Credentials"
  default     = "/Users/poojasingh/.dbt/helical-sol-409813-687472f0ec67.json"
}

variable "project_name" {
  description = "Project Name"
  default     = "helical-sol-409813"
}

variable "location" {
  description = "Project Location"
  default     = "asia-south2"
}

variable "bigquery_dataset_name" {
  description = "Name of BigQuery Dataset"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "Name of GCS Bucket Name"
  default     = "helical-sol-409813-demo"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}
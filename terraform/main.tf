# terraform config for the FNB NAV pipeline
# creates all BigQuery datasets and table shells, SQL pipeline populates them
# terraform apply to create, terraform destroy to wipe everthing clean

terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# -- variables --

variable "project_id" {
  description = "GCP project ID"
  type        = string
  default     = "fmn-sandbox"
}

variable "location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}

variable "pipeline_enabled" {
  description = "Set to false to skip all resource creation (e.g. during migration)"
  type        = bool
  default     = true
}

# -- provider --

provider "google" {
  project = var.project_id
}

# -- datasets --

resource "google_bigquery_dataset" "staging" {
  count       = var.pipeline_enabled ? 1 : 0
  dataset_id  = "staging"
  project     = var.project_id
  location    = var.location
  description = "Cleaned, joined, PII-free tables. Source of truth for all downstream."

  labels = {
    layer       = "staging"
    managed_by  = "terraform"
  }

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "analytics" {
  count       = var.pipeline_enabled ? 1 : 0
  dataset_id  = "analytics"
  project     = var.project_id
  location    = var.location
  description = "Intermediate features, scores, and spend metrics. Plus ML models."

  labels = {
    layer       = "analytics"
    managed_by  = "terraform"
  }

  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "marts" {
  count       = var.pipeline_enabled ? 1 : 0
  dataset_id  = "marts"
  project     = var.project_id
  location    = var.location
  description = "Dashboard-ready analytical tables. Final layer consumed by Streamlit/Looker."

  labels = {
    layer       = "marts"
    managed_by  = "terraform"
  }

  delete_contents_on_destroy = true
}

# -- staging tables --

resource "google_bigquery_table" "stg_transactions" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.staging[0].dataset_id
  table_id            = "stg_transactions"
  project             = var.project_id
  description         = "All transactions with lookups joined. Partitioned by month, clustered by CATEGORY_TWO and DESTINATION."
  deletion_protection = false

  time_partitioning {
    type  = "MONTH"
    field = "EFF_DATE"
  }

  clustering = ["CATEGORY_TWO", "DESTINATION"]

  labels = {
    layer      = "staging"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "stg_customers" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.staging[0].dataset_id
  table_id            = "stg_customers"
  project             = var.project_id
  description         = "One row per customer. Demographics renamed from demo_* to human-readable names."
  deletion_protection = false

  labels = {
    layer      = "staging"
    managed_by = "terraform"
  }
}

# -- analytics tables --

resource "google_bigquery_table" "int_rfm_features" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.analytics[0].dataset_id
  table_id            = "int_rfm_features"
  project             = var.project_id
  description         = "20+ behavioral features per customer for clustering."
  deletion_protection = false

  labels = {
    layer      = "analytics"
    source     = "stg_transactions"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "int_rfm_scores" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.analytics[0].dataset_id
  table_id            = "int_rfm_scores"
  project             = var.project_id
  description         = "Quintile-scored RFM features (1-5). Feeds k-means model."
  deletion_protection = false

  labels = {
    layer      = "analytics"
    source     = "int_rfm_features"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "int_customer_category_spend" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.analytics[0].dataset_id
  table_id            = "int_customer_category_spend"
  project             = var.project_id
  description         = "Per customer x category x destination spend. Powers share-of-wallet for any client."
  deletion_protection = false

  labels = {
    layer      = "analytics"
    source     = "stg_transactions"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "int_destination_metrics" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.analytics[0].dataset_id
  table_id            = "int_destination_metrics"
  project             = var.project_id
  description         = "Per destination KPIs within each category. Powers benchmark comparisons."
  deletion_protection = false

  labels = {
    layer      = "analytics"
    source     = "int_customer_category_spend"
    managed_by = "terraform"
  }
}

# -- mart tables --

resource "google_bigquery_table" "mart_cluster_output" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.marts[0].dataset_id
  table_id            = "mart_cluster_output"
  project             = var.project_id
  description         = "Every customer with cluster assignment, RFM features, and demographics."
  deletion_protection = false

  labels = {
    layer      = "marts"
    source     = "kmeans_model"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "mart_cluster_profiles" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.marts[0].dataset_id
  table_id            = "mart_cluster_profiles"
  project             = var.project_id
  description         = "One row per segment with averages, demographics, and distributions."
  deletion_protection = false

  labels = {
    layer      = "marts"
    source     = "mart_cluster_output"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "mart_cluster_summary" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.marts[0].dataset_id
  table_id            = "mart_cluster_summary"
  project             = var.project_id
  description         = "Executive summary: segment descriptions and recommended actions."
  deletion_protection = false

  labels = {
    layer      = "marts"
    source     = "mart_cluster_profiles"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "mart_behavioral_summary" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.marts[0].dataset_id
  table_id            = "mart_behavioral_summary"
  project             = var.project_id
  description         = "Time-of-day, weekend ratio, and diversity metrics per segment."
  deletion_protection = false

  labels = {
    layer      = "marts"
    source     = "stg_transactions"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "mart_geo_summary" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.marts[0].dataset_id
  table_id            = "mart_geo_summary"
  project             = var.project_id
  description         = "Geographic spend by province x municipality x category."
  deletion_protection = false

  labels = {
    layer      = "marts"
    source     = "stg_transactions"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "mart_churn_risk" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.marts[0].dataset_id
  table_id            = "mart_churn_risk"
  project             = var.project_id
  description         = "Customer-level churn risk scoring (rule-based)."
  deletion_protection = false

  labels = {
    layer      = "marts"
    source     = "stg_transactions"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "mart_monthly_trends" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.marts[0].dataset_id
  table_id            = "mart_monthly_trends"
  project             = var.project_id
  description         = "Monthly spend per category x destination. Dashboard filters by client."
  deletion_protection = false

  labels = {
    layer      = "marts"
    source     = "stg_transactions"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "mart_demographic_summary" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.marts[0].dataset_id
  table_id            = "mart_demographic_summary"
  project             = var.project_id
  description         = "Demographic breakdown per category for pitch decks."
  deletion_protection = false

  labels = {
    layer      = "marts"
    source     = "int_customer_category_spend"
    managed_by = "terraform"
  }
}

resource "google_bigquery_table" "mart_destination_benchmarks" {
  count               = var.pipeline_enabled ? 1 : 0
  dataset_id          = google_bigquery_dataset.marts[0].dataset_id
  table_id            = "mart_destination_benchmarks"
  project             = var.project_id
  description         = "All destinations within each category. Dashboard anonymizes competitors."
  deletion_protection = false

  labels = {
    layer      = "marts"
    source     = "int_destination_metrics"
    managed_by = "terraform"
  }
}

# -- outputs --

output "datasets" {
  description = "Created datasets"
  value = var.pipeline_enabled ? {
    staging   = google_bigquery_dataset.staging[0].dataset_id
    analytics = google_bigquery_dataset.analytics[0].dataset_id
    marts     = google_bigquery_dataset.marts[0].dataset_id
  } : {}
}

output "table_count" {
  description = "Number of managed tables"
  value       = var.pipeline_enabled ? 14 : 0
}

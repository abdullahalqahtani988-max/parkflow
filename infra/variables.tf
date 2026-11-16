variable "project" {
  type    = string
  default = "parkflow-514314"
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "datasets" {
  description = "Medallion layers + ops/reference/assertions datasets"
  type        = list(string)
  default = ["parkflow_bronze", "parkflow_ref", "parkflow_staging", "parkflow_silver",
  "parkflow_ops", "parkflow_gold", "parkflow_assertions"]
}

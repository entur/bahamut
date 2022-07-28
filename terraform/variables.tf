#Enviroment variables
variable "gcp_project" {
  description = "The GCP project hosting the workloads"
}

variable "gcp_region" {
  description = "The GCP region"
  default     = "europe-west1"
}

variable "gcp_storage_project" {
  description = "The GCP project hosting the Google Storage resources"
}

variable "gcp_resources_project" {
  description = "The GCP project hosting the project resources"
}

variable "kube_namespace" {
  description = "The Kubernetes namespace"
}

variable "labels" {
  description = "Labels used in all resources"
  type = map(string)
  default = {
    manager = "terraform"
    team = "ror"
    slack = "talk-ror"
    app = "bahamut"
  }
}

variable "load_config_file" {
  description = "Do not load kube config file"
  default = false
}

variable "service_account_bucket_role_kakka" {
  description = "Role of the Service Account - more about roles https://cloud.google.com/storage/docs/access-control/iam-roles"
  default = "roles/storage.objectViewer"
}

variable "service_account_bucket_role_bahamut" {
  description = "Role of the Service Account - more about roles https://cloud.google.com/storage/docs/access-control/iam-roles"
  default = "roles/storage.objectAdmin"
}

variable "bucket_bahamut_instance_name" {
  description = "Bahamut bucket name"
}

variable "bucket_kakka_instance_name" {
  description = "Kakka bucket name"
}






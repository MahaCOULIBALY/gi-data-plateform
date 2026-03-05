# terraform/main.tf
terraform {
  required_providers {
    ovh = {
      source  = "ovh/ovh"
      version = ">= 2.1.0"
    }
    openstack = {
      source  = "terraform-provider-openstack/openstack"
      version = "~> 1.53"
    }
  }
}

provider "ovh" {
  endpoint           = "ovh-eu"
  application_key    = var.ovh_application_key
  application_secret = var.ovh_application_secret
  consumer_key       = var.ovh_consumer_key
}

provider "openstack" {
  auth_url    = "https://auth.cloud.ovh.net/v3/"
  domain_name = "default"
  region      = "GRA"
}

variable "ovh_application_key"    { sensitive = true }
variable "ovh_application_secret" { sensitive = true }
variable "ovh_consumer_key"       { sensitive = true }
variable "project_id"             { description = "OVHcloud Public Cloud project ID" }

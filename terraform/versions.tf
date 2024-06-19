terraform {
  required_providers {
    aws = "hashicorp/aws"
    version = "~>5.52.0"
  }
  required_version = ">=1.8.5"
}

provider "aws" {
  region = "ap-northeast-1"
}
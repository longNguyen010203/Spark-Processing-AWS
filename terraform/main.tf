#-----------#
# Amazon S3 #
#-----------#
resource "aws_s3_bucket" "aws-s3" {
  bucket = "spark-tf-processing-s3"

  tags = {
    Name        = "S3 bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket_versioning" "aws-s3-versioning" {
  bucket = aws_s3_bucket.aws-s3.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_kms_key" "kms-key" {
  description             = "This key is used to encrypt bucket objects"
  deletion_window_in_days = 10
}

resource "aws_s3_bucket_server_side_encryption_configuration" "aws-s3-encryption" {
  bucket = aws_s3_bucket.aws-s3.id

  rule {
    apply_server_side_encryption_by_default {
      kms_master_key_id = aws_kms_key.kms-key.arn
      sse_algorithm     = "aws:kms"
    }
    bucket_key_enabled = true
  }
}


#------------#
# Amazon VPC #
#------------#
module "vpc" {
  source = "terraform-aws-modules/vpc/aws"

  name             = "spark-tf-processing-vpc"
  cidr             = "10.0.0.0/16"
  instance_tenancy = "default"

  azs             = ["ap-southeast-2a", "ap-southeast-2b"]
  private_subnets = ["10.0.128.0/20", "10.0.144.0/20"]
  public_subnets  = ["10.0.0.0/20", "10.0.16.0/20"]

  enable_nat_gateway   = false
  enable_dns_hostnames = true

  tags = {
    Terraform   = "true"
    Environment = "dev"
  }
}

resource "aws_vpc_endpoint" "s3-gateway" {
  vpc_id       = module.vpc.vpc_id
  service_name = "com.amazonaws.ap-southeast-2.s3"

  tags = {
    Environment = "test"
    Name        = "spark-tf-processing-vpce-s3"
  }
}

# Retrieve the AZ where we want to create network resources
# This must be in the region selected on the AWS provider.


#------------#
# Amazon EC2 #
#------------#
resource "aws_network_interface" "example" {
  subnet_id   = aws_subnet.example.id
  private_ips = ["172.16.10.100"]

  tags = {
    Name = "primary_network_interface"
  }
}

data "aws_ami" "amzn-linux-2023-ami" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }
}

resource "aws_instance" "example" {
  ami           = data.aws_ami.amzn-linux-2023-ami.id
  instance_type = "c6a.2xlarge"
  subnet_id     = aws_subnet.example.id

  network_interface {
    network_interface_id = aws_network_interface.example.id
    device_index         = 0
  }

  cpu_options {
    core_count       = 2
    threads_per_core = 2
  }

  tags = {
    Name = "tf-example"
  }
}


#------------#
# Amazon EMR #
#------------#
resource "aws_emr_cluster" "cluster" {
  name          = "spark-tf-processing-emr"
  release_label = "emr-7.1.0"
  applications  = ["Spark"]

  additional_info = <<EOF
{
  "instanceAwsClientConfiguration": {
    "proxyPort": 8099,
    "proxyHost": "myproxy.example.com"
  }
}
EOF

  termination_protection            = false
  keep_job_flow_alive_when_no_steps = true

  ec2_attributes {
    subnet_id                         = aws_subnet.main.id
    emr_managed_master_security_group = aws_security_group.sg.id
    emr_managed_slave_security_group  = aws_security_group.sg.id
    instance_profile                  = aws_iam_instance_profile.emr_profile.arn
  }

  master_instance_group {
    instance_type = "m4.large"
  }

  core_instance_group {
    instance_type  = "c4.large"
    instance_count = 1

    ebs_config {
      size                 = "40"
      type                 = "gp2"
      volumes_per_instance = 1
    }

    bid_price = "0.30"

    autoscaling_policy = <<EOF
{
"Constraints": {
  "MinCapacity": 1,
  "MaxCapacity": 2
},
"Rules": [
  {
    "Name": "ScaleOutMemoryPercentage",
    "Description": "Scale out if YARNMemoryAvailablePercentage is less than 15",
    "Action": {
      "SimpleScalingPolicyConfiguration": {
        "AdjustmentType": "CHANGE_IN_CAPACITY",
        "ScalingAdjustment": 1,
        "CoolDown": 300
      }
    },
    "Trigger": {
      "CloudWatchAlarmDefinition": {
        "ComparisonOperator": "LESS_THAN",
        "EvaluationPeriods": 1,
        "MetricName": "YARNMemoryAvailablePercentage",
        "Namespace": "AWS/ElasticMapReduce",
        "Period": 300,
        "Statistic": "AVERAGE",
        "Threshold": 15.0,
        "Unit": "PERCENT"
      }
    }
  }
]
}
EOF
  }

  ebs_root_volume_size = 100

  tags = {
    role = "rolename"
    env  = "env"
  }

  bootstrap_action {
    path = "s3://elasticmapreduce/bootstrap-actions/run-if"
    name = "runif"
    args = ["instance.isMaster=true", "echo running on master node"]
  }

  configurations_json = <<EOF
  [
    {
      "Classification": "hadoop-env",
      "Configurations": [
        {
          "Classification": "export",
          "Properties": {
            "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"
          }
        }
      ],
      "Properties": {}
    },
    {
      "Classification": "spark-env",
      "Configurations": [
        {
          "Classification": "export",
          "Properties": {
            "JAVA_HOME": "/usr/lib/jvm/java-1.8.0"
          }
        }
      ],
      "Properties": {}
    }
  ]
EOF

  service_role = aws_iam_role.iam_emr_service_role.arn
}


#-----------------#
# Amazon Redshift #
#-----------------#
resource "aws_redshift_cluster" "example" {
  cluster_identifier = "tf-redshift-cluster"
  database_name      = "mydb"
  master_username    = "exampleuser"
  node_type          = "dc1.large"
  cluster_type       = "single-node"

  manage_master_password = true
}
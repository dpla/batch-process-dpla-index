#!/usr/bin/env bash

this_script_name=$0

# Hard code values for spark job
parquet_out="s3a://dpla-provider-export/"
jsonl_out="s3a://dpla-provider-export/"
metadata_quality_out="s3a://dashboard-analytics/"
sitemap_out="s3a://sitemaps.dp.la/"
sitemap_root="http://sitemaps.dp.la/"
necropolis_out="s3a://dpla-necropolis/"
do_necro="true"

# sbt assembly
# echo "Copying to s3://dpla-monthly-batch/"
# aws s3 cp ./target/scala-2.11/batch-process-dpla-index-assembly-0.1.jar s3://dpla-monthly-batch/


# spin up EMR cluster and run job 
aws emr create-cluster \
--auto-terminate \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--applications Name=Hadoop Name=Hive Name=Pig Name=Hue Name=Spark \
--ebs-root-volume-size 100 \
--ec2-attributes '{
  "KeyName": "general",
  "InstanceProfile": "EMR_EC2_DefaultRole",
  "ServiceAccessSecurityGroup": "sg-07459c7a",
  "SubnetId": "subnet-055a4ce359a29aa76",
  "EmrManagedSlaveSecurityGroup": "sg-0a459c77",
  "EmrManagedMasterSecurityGroup": "sg-08459c75"
}' \
--service-role EMR_DefaultRole \
--enable-debugging \
--release-label emr-5.32.0 \
--log-uri 's3n://aws-logs-283408157088-us-east-1/elasticmapreduce/' \
--steps '[
  {
    "Args": [
      "spark-submit",
      "--deploy-mode",
      "cluster",
      "--class",
      "dpla.batch_process_dpla_index.entries.AllProcessesEntry",
      "s3://dpla-monthly-batch/batch-process-dpla-index-assembly-0.1.jar",
      "'"$parquet_out"'",
      "'"$jsonl_out"'",
      "'"$metadata_quality_out"'",
      "'"$sitemap_out"'",
      "'"$sitemap_root"'",
      "'"$necropolis_out"'",
      "'"$do_necro"'"
    ],
    "Type": "CUSTOM_JAR",
    "ActionOnFailure": "TERMINATE_CLUSTER",
    "Jar": "command-runner.jar",
    "Properties": "",
    "Name": "monthlybatch"
  }
]' \
--name 'monthlybatch' \
--instance-groups '[
  {
    "InstanceCount": 7,
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 250,
            "VolumeType": "gp2"
          },
          "VolumesPerInstance": 2
        }
      ]
    },
    "InstanceGroupType": "CORE",
    "InstanceType": "m5.2xlarge",
    "Name": "Core - 2"
  },
  {
    "InstanceCount": 8,
    "BidPrice": "OnDemandPrice",
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 250,
            "VolumeType": "gp2"
          },
          "VolumesPerInstance": 2
        }
      ]
    },
    "InstanceGroupType": "TASK",
    "InstanceType": "m5.2xlarge",
    "Name": "Task - 3"
  },
  {
    "InstanceCount": 1,
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 32,
            "VolumeType": "gp2"
          },
          "VolumesPerInstance": 2
        }
      ]
    },
    "InstanceGroupType": "MASTER",
    "InstanceType": "m5.2xlarge",
    "Name": "Master - 1"
  }
]' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region us-east-1

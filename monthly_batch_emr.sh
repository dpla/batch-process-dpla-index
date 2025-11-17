#!/usr/bin/env bash

set -euxo pipefail


# Hard code values for spark job
parquet_out="s3a://dpla-provider-export/"
jsonl_out="s3a://dpla-provider-export/"
metadata_quality_out="s3a://dashboard-analytics/"
sitemap_out="s3a://sitemaps.dp.la/sitemap/"
sitemap_root="https://dp.la/sitemap/"

sbt assembly
echo "Copying to s3://dpla-monthly-batch/"
aws s3 cp ./target/scala-2.12/batch-process-dpla-index-assembly.jar s3://dpla-monthly-batch/

# spin up EMR cluster and run job
aws emr create-cluster \
--configurations file://./cluster-config.json \
--no-auto-terminate \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--applications Name=Hadoop Name=Hive Name=Spark \
--ebs-root-volume-size 100 \
--ec2-attributes '{
  "EmrManagedMasterSecurityGroup": "sg-08459c75",
  "EmrManagedSlaveSecurityGroup": "sg-0a459c77",
  "InstanceProfile": "sparkindexer-s3",
  "KeyName": "general",
  "ServiceAccessSecurityGroup": "sg-07459c7a",
  "SubnetId": "subnet-90afd9ba"
}' \
--service-role EMR_Default_Role_v2 \
--enable-debugging \
--release-label emr-7.10.0 \
--log-uri 's3n://aws-logs-283408157088-us-east-1/elasticmapreduce/' \
--tags for-use-with-amazon-emr-managed-policies=true \
#--steps '[
#  {
#    "Args": [
#      "spark-submit",
#      "--deploy-mode",
#      "cluster",
#      "--class",
#      "dpla.batch_process_dpla_index.entries.ParquetDumpEntry",
#      "s3://dpla-monthly-batch/batch-process-dpla-index-assembly-0.1.jar",
#      "'"$parquet_out"'"
#    ],
#    "Type": "CUSTOM_JAR",
#    "ActionOnFailure": "TERMINATE_CLUSTER",
#    "Jar": "command-runner.jar",
#    "Properties": "",
#    "Name": "parquet"
#  },
#  {
#    "Args": [
#      "spark-submit",
#      "--deploy-mode",
#      "cluster",
#      "--class",
#      "dpla.batch_process_dpla_index.entries.JsonlDumpEntry",
#      "s3://dpla-monthly-batch/batch-process-dpla-index-assembly-0.1.jar",
#      "'"$jsonl_out"'"
#    ],
#    "Type": "CUSTOM_JAR",
#    "ActionOnFailure": "CANCEL_AND_WAIT",
#    "Jar": "command-runner.jar",
#    "Properties": "",
#    "Name": "jsonl"
#  },
#  {
#    "Args": [
#      "spark-submit",
#      "--deploy-mode",
#      "cluster",
#      "--class",
#      "dpla.batch_process_dpla_index.entries.MqReportsEntry",
#      "s3://dpla-monthly-batch/batch-process-dpla-index-assembly-0.1.jar",
#      "'"$parquet_out"'",
#      "'"$metadata_quality_out"'"
#    ],
#    "Type": "CUSTOM_JAR",
#    "ActionOnFailure": "CANCEL_AND_WAIT",
#    "Jar": "command-runner.jar",
#    "Properties": "",
#    "Name": "mq"
#  },
#  {
#    "Args": [
#      "spark-submit",
#      "--deploy-mode",
#      "cluster",
#      "--class",
#      "dpla.batch_process_dpla_index.entries.SitemapEntry",
#      "s3://dpla-monthly-batch/batch-process-dpla-index-assembly-0.1.jar",
#      "'"$parquet_out"'",
#      "'"$sitemap_out"'",
#      "'"$sitemap_root"'"
#    ],
#    "Type": "CUSTOM_JAR",
#    "ActionOnFailure": "CANCEL_AND_WAIT",
#    "Jar": "command-runner.jar",
#    "Properties": "",
#    "Name": "sitemap"
#  }
#]' \
--name 'monthlybatch' \
--instance-groups '[
  {
    "InstanceCount": 2
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 250,
            "VolumeType": "gp3"
          },
          "VolumesPerInstance": 2
        }
      ]
    },
    "InstanceGroupType": "CORE",
    "InstanceType": "r8g.xlarge",
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
            "VolumeType": "gp3"
          },
          "VolumesPerInstance": 2
        }
      ]
    },
    "InstanceGroupType": "TASK",
    "InstanceType": "r8g.xlarge",
    "Name": "Task - 3"
  },
  {
    "InstanceCount": 1,
    "EbsConfiguration": {
      "EbsBlockDeviceConfigs": [
        {
          "VolumeSpecification": {
            "SizeInGB": 32,
            "VolumeType": "gp3"
          },
          "VolumesPerInstance": 2
        }
      ]
    },
    "InstanceGroupType": "MASTER",
    "InstanceType": "m8g.xlarge",
    "Name": "Master - 1"
  }
]' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region us-east-1

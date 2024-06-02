aws emr create-cluster \
--release-label "emr-7.1.0" \
--name "lab-emr-drpalaciod" \
--applications Name=Spark Name=Hadoop Name=Pig Name=Hive \
--ec2-attributes KeyName=lab-06 \
--instance-type m5.xlarge \
--instance-count 3 \
--use-default-roles \
--no-auto-terminate \
--log-uri  "s3://lab-emr-bucket"



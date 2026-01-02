use warehouse cal_wh;
use database musicdata;
use schema musicdata.raw;

set aws_key_id = '{{ env_var("AWS_ACCESS_KEY_ID") }}';
set aws_secret = '{{ env_var("AWS_SECRET_ACCESS_KEY") }}';
set bucket = '{{ env_var("S3_BUCKET") }}';
  create stage if not exists healthstage
      url = concat('s3://', $bucket)
      credentials=(aws_key_id=$aws_key_id aws_secret_key=$aws_secret);
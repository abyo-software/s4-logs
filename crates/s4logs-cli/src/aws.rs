//! AWS client construction honoring `--region` / `--endpoint-url`.
//!
//! With an endpoint override (LocalStack / MinIO) the S3 client switches to
//! path-style addressing — virtual-hosted bucket DNS does not resolve
//! against a local endpoint.

use crate::cli::GlobalArgs;

pub struct AwsClients {
    config: aws_config::SdkConfig,
    force_path_style: bool,
}

/// Load the shared SDK config once; both clients derive from it so the
/// endpoint override applies to S3 and CloudWatch Logs alike.
pub async fn load(global: &GlobalArgs) -> AwsClients {
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = &global.region {
        loader = loader.region(aws_config::Region::new(region.clone()));
    }
    if let Some(url) = &global.endpoint_url {
        loader = loader.endpoint_url(url.clone());
    }
    AwsClients {
        config: loader.load().await,
        force_path_style: global.endpoint_url.is_some(),
    }
}

impl AwsClients {
    pub fn s3(&self) -> aws_sdk_s3::Client {
        let mut builder =
            aws_sdk_s3::config::Builder::from(&self.config).force_path_style(self.force_path_style);
        if self.force_path_style {
            // S3-compatible stores (LocalStack at least) return the
            // full-object CRC32C header on *ranged* GetObject, which trips
            // the SDK's default response-checksum validation on grep's
            // range reads. Real S3 omits the header there, so only relax
            // when an endpoint override is in play.
            builder = builder.response_checksum_validation(
                aws_sdk_s3::config::ResponseChecksumValidation::WhenRequired,
            );
        }
        aws_sdk_s3::Client::from_conf(builder.build())
    }

    pub fn cwl(&self) -> aws_sdk_cloudwatchlogs::Client {
        aws_sdk_cloudwatchlogs::Client::new(&self.config)
    }

    /// CloudWatch **Metrics** (not Logs) — `s4logs plan` reads the
    /// `AWS/Logs IncomingBytes` metric via `GetMetricData`.
    pub fn cw_metrics(&self) -> aws_sdk_cloudwatch::Client {
        aws_sdk_cloudwatch::Client::new(&self.config)
    }
}

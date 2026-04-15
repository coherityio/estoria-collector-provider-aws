package io.coherity.estoria.collector.provider.aws;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import io.coherity.estoria.collector.spi.ProviderContext;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.apigateway.ApiGatewayClient;
import software.amazon.awssdk.services.appmesh.AppMeshClient;
import software.amazon.awssdk.services.autoscaling.AutoScalingClient;
import software.amazon.awssdk.services.cloudfront.CloudFrontClient;
import software.amazon.awssdk.services.cloudtrail.CloudTrailClient;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient;
import software.amazon.awssdk.services.config.ConfigClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ecs.EcsClient;
import software.amazon.awssdk.services.efs.EfsClient;
import software.amazon.awssdk.services.eks.EksClient;
import software.amazon.awssdk.services.elasticloadbalancing.ElasticLoadBalancingClient;
import software.amazon.awssdk.services.elasticloadbalancingv2.ElasticLoadBalancingV2Client;
import software.amazon.awssdk.services.fsx.FSxClient;
import software.amazon.awssdk.services.globalaccelerator.GlobalAcceleratorClient;
import software.amazon.awssdk.services.guardduty.GuardDutyClient;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.identitystore.IdentitystoreClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.macie2.Macie2Client;
import software.amazon.awssdk.services.organizations.OrganizationsClient;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3control.S3ControlClient;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.securityhub.SecurityHubClient;
import software.amazon.awssdk.services.shield.ShieldClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssoadmin.SsoAdminClient;
import software.amazon.awssdk.services.storagegateway.StorageGatewayClient;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.wafv2.Wafv2Client;
import software.amazon.awssdk.services.opensearch.OpenSearchClient;
import software.amazon.awssdk.services.redshift.RedshiftClient;
import software.amazon.awssdk.services.elasticache.ElastiCacheClient;
import software.amazon.awssdk.services.neptune.NeptuneClient;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.kafka.KafkaClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.kinesisanalyticsv2.KinesisAnalyticsV2Client;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.appconfig.AppConfigClient;
import software.amazon.awssdk.services.account.AccountClient;
import software.amazon.awssdk.services.budgets.BudgetsClient;
import software.amazon.awssdk.services.costexplorer.CostExplorerClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.lakeformation.LakeFormationClient;
import software.amazon.awssdk.services.athena.AthenaClient;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.datapipeline.DataPipelineClient;
import software.amazon.awssdk.services.cloudformation.CloudFormationClient;
import software.amazon.awssdk.services.servicecatalog.ServiceCatalogClient;
import software.amazon.awssdk.services.backup.BackupClient;
import software.amazon.awssdk.services.licensemanager.LicenseManagerClient;
import software.amazon.awssdk.services.resourcegroups.ResourceGroupsClient;
import software.amazon.awssdk.services.resourcegroupstaggingapi.ResourceGroupsTaggingApiClient;
import software.amazon.awssdk.services.ses.SesClient;
import software.amazon.awssdk.services.sesv2.SesV2Client;

@Slf4j
public class AwsClientFactory
{
    // =========================================================================
    // Constants
    // =========================================================================

    private static final String PROVIDER_CONTEXT_ATTRIBUTE_PROFILE = "profile";
    private static final String PROVIDER_CONTEXT_ATTRIBUTE_REGION  = "region";

    private static final String DEFAULT_PROFILE = "default";
    private static final String DEFAULT_REGION  = "default";

    private static final String CLIENT_NAME_EC2             = "Ec2";
    private static final String CLIENT_NAME_AUTOSCALING     = "AutoScaling";
    private static final String CLIENT_NAME_ELB             = "ELB";
    private static final String CLIENT_NAME_ELB_V2          = "ELBv2";
    private static final String CLIENT_NAME_ECS             = "ECS";
    private static final String CLIENT_NAME_EKS             = "EKS";
    private static final String CLIENT_NAME_LAMBDA          = "Lambda";
    private static final String CLIENT_NAME_S3              = "S3";
    private static final String CLIENT_NAME_RDS             = "RDS";
    private static final String CLIENT_NAME_DYNAMODB        = "DynamoDB";
    private static final String CLIENT_NAME_SQS             = "SQS";
    private static final String CLIENT_NAME_SNS             = "SNS";
    private static final String CLIENT_NAME_IAM             = "IAM";
    private static final String CLIENT_NAME_KMS             = "KMS";
    private static final String CLIENT_NAME_SECRETS_MANAGER = "SecretsManager";
    private static final String CLIENT_NAME_SSM             = "SSM";
    private static final String CLIENT_NAME_CLOUDWATCH      = "CloudWatch";
    private static final String CLIENT_NAME_CLOUDWATCH_LOGS = "CloudWatchLogs";
    private static final String CLIENT_NAME_CLOUDTRAIL           = "CloudTrail";
    private static final String CLIENT_NAME_STS                  = "STS";
    private static final String CLIENT_NAME_GLOBAL_ACCELERATOR   = "GlobalAccelerator";
    private static final String CLIENT_NAME_APP_MESH             = "AppMesh";
    private static final String CLIENT_NAME_API_GATEWAY          = "ApiGateway";
    private static final String CLIENT_NAME_EFS                  = "EFS";
    private static final String CLIENT_NAME_FSX                  = "FSx";
    private static final String CLIENT_NAME_STORAGE_GATEWAY      = "StorageGateway";
    private static final String CLIENT_NAME_CLOUDFRONT           = "CloudFront";
    private static final String CLIENT_NAME_S3_CONTROL           = "S3Control";
    private static final String CLIENT_NAME_ORGANIZATIONS        = "Organizations";
    private static final String CLIENT_NAME_SSO_ADMIN            = "SsoAdmin";
    private static final String CLIENT_NAME_IDENTITY_STORE       = "IdentityStore";
    private static final String CLIENT_NAME_WAFV2                = "Wafv2";
    private static final String CLIENT_NAME_SHIELD               = "Shield";
    private static final String CLIENT_NAME_MACIE2               = "Macie2";
    private static final String CLIENT_NAME_GUARDDUTY            = "GuardDuty";
    private static final String CLIENT_NAME_SECURITYHUB          = "SecurityHub";
    private static final String CLIENT_NAME_CONFIG               = "Config";
    private static final String CLIENT_NAME_OPENSEARCH            = "OpenSearch";
    private static final String CLIENT_NAME_REDSHIFT              = "Redshift";
    private static final String CLIENT_NAME_ELASTICACHE           = "ElastiCache";
    private static final String CLIENT_NAME_NEPTUNE               = "Neptune";
    private static final String CLIENT_NAME_TIMESTREAM_WRITE      = "TimestreamWrite";
    private static final String CLIENT_NAME_MSK                   = "MSK";
    private static final String CLIENT_NAME_KINESIS                = "Kinesis";
    private static final String CLIENT_NAME_FIREHOSE               = "Firehose";
    private static final String CLIENT_NAME_KINESIS_ANALYTICS_V2  = "KinesisAnalyticsV2";
    private static final String CLIENT_NAME_EVENTBRIDGE            = "EventBridge";
    private static final String CLIENT_NAME_SFN                    = "SFN";
    private static final String CLIENT_NAME_APPCONFIG              = "AppConfig";
    private static final String CLIENT_NAME_ACCOUNT               = "Account";
    private static final String CLIENT_NAME_BUDGETS               = "Budgets";
    private static final String CLIENT_NAME_COST_EXPLORER         = "CostExplorer";
    private static final String CLIENT_NAME_GLUE                  = "Glue";
    private static final String CLIENT_NAME_LAKE_FORMATION        = "LakeFormation";
    private static final String CLIENT_NAME_ATHENA                = "Athena";
    private static final String CLIENT_NAME_EMR                   = "EMR";
    private static final String CLIENT_NAME_DATA_PIPELINE             = "DataPipeline";
    private static final String CLIENT_NAME_CLOUDFORMATION            = "CloudFormation";
    private static final String CLIENT_NAME_SERVICE_CATALOG           = "ServiceCatalog";
    private static final String CLIENT_NAME_BACKUP                    = "Backup";
    private static final String CLIENT_NAME_LICENSE_MANAGER           = "LicenseManager";
    private static final String CLIENT_NAME_RESOURCE_GROUPS           = "ResourceGroups";
    private static final String CLIENT_NAME_RESOURCE_GROUPS_TAGGING   = "ResourceGroupsTaggingApi";
    private static final String CLIENT_NAME_SES                       = "SES";
    private static final String CLIENT_NAME_SES_V2                    = "SESv2";

    private static final int    SHUTDOWN_TIMEOUT_SECONDS    = 30;
    private static final String CACHE_KEY_SEPARATOR         = ":";

    // =========================================================================
    // Singleton
    // =========================================================================

    private static final AwsClientFactory INSTANCE = new AwsClientFactory();

    public static AwsClientFactory getInstance()
    {
        return INSTANCE;
    }

    private AwsClientFactory() {}

    // =========================================================================
    // Client caches — one typed map per client type, key is "profile:region"
    // =========================================================================

    private final Map<String, Ec2Client>                    ec2Clients            = new ConcurrentHashMap<>();
    private final Map<String, AutoScalingClient>            autoScalingClients    = new ConcurrentHashMap<>();
    private final Map<String, ElasticLoadBalancingClient>   elbClients            = new ConcurrentHashMap<>();
    private final Map<String, ElasticLoadBalancingV2Client> elbV2Clients          = new ConcurrentHashMap<>();
    private final Map<String, EcsClient>                    ecsClients            = new ConcurrentHashMap<>();
    private final Map<String, EksClient>                    eksClients            = new ConcurrentHashMap<>();
    private final Map<String, LambdaClient>                 lambdaClients         = new ConcurrentHashMap<>();
    private final Map<String, S3Client>                     s3Clients             = new ConcurrentHashMap<>();
    private final Map<String, RdsClient>                    rdsClients            = new ConcurrentHashMap<>();
    private final Map<String, DynamoDbClient>               dynamoDbClients       = new ConcurrentHashMap<>();
    private final Map<String, SqsClient>                    sqsClients            = new ConcurrentHashMap<>();
    private final Map<String, SnsClient>                    snsClients            = new ConcurrentHashMap<>();
    private final Map<String, IamClient>                    iamClients            = new ConcurrentHashMap<>();
    private final Map<String, KmsClient>                    kmsClients            = new ConcurrentHashMap<>();
    private final Map<String, SecretsManagerClient>         secretsManagerClients = new ConcurrentHashMap<>();
    private final Map<String, SsmClient>                    ssmClients            = new ConcurrentHashMap<>();
    private final Map<String, CloudWatchClient>             cloudWatchClients     = new ConcurrentHashMap<>();
    private final Map<String, CloudWatchLogsClient>         cloudWatchLogsClients = new ConcurrentHashMap<>();
    private final Map<String, CloudTrailClient>             cloudTrailClients         = new ConcurrentHashMap<>();
    private final Map<String, StsClient>                    stsClients                = new ConcurrentHashMap<>();
    private final Map<String, GlobalAcceleratorClient>      globalAcceleratorClients  = new ConcurrentHashMap<>();
    private final Map<String, AppMeshClient>                appMeshClients            = new ConcurrentHashMap<>();
    private final Map<String, ApiGatewayClient>             apiGatewayClients         = new ConcurrentHashMap<>();
    private final Map<String, EfsClient>                    efsClients                = new ConcurrentHashMap<>();
    private final Map<String, FSxClient>                    fsxClients                = new ConcurrentHashMap<>();
    private final Map<String, StorageGatewayClient>         storageGatewayClients     = new ConcurrentHashMap<>();
    private final Map<String, CloudFrontClient>             cloudFrontClients         = new ConcurrentHashMap<>();
    private final Map<String, S3ControlClient>              s3ControlClients          = new ConcurrentHashMap<>();
    private final Map<String, OrganizationsClient>          organizationsClients      = new ConcurrentHashMap<>();
    private final Map<String, SsoAdminClient>               ssoAdminClients           = new ConcurrentHashMap<>();
    private final Map<String, IdentitystoreClient>          identityStoreClients      = new ConcurrentHashMap<>();
    private final Map<String, Wafv2Client>                  wafv2Clients              = new ConcurrentHashMap<>();
    private final Map<String, ShieldClient>                 shieldClients             = new ConcurrentHashMap<>();
    private final Map<String, Macie2Client>                 macie2Clients             = new ConcurrentHashMap<>();
    private final Map<String, GuardDutyClient>              guardDutyClients          = new ConcurrentHashMap<>();
    private final Map<String, SecurityHubClient>            securityHubClients        = new ConcurrentHashMap<>();
    private final Map<String, ConfigClient>                 configClients             = new ConcurrentHashMap<>();
    private final Map<String, OpenSearchClient>              openSearchClients          = new ConcurrentHashMap<>();
    private final Map<String, RedshiftClient>                redshiftClients            = new ConcurrentHashMap<>();
    private final Map<String, ElastiCacheClient>             elastiCacheClients         = new ConcurrentHashMap<>();
    private final Map<String, NeptuneClient>                 neptuneClients             = new ConcurrentHashMap<>();
    private final Map<String, TimestreamWriteClient>         timestreamWriteClients     = new ConcurrentHashMap<>();
    private final Map<String, KafkaClient>                   kafkaClients               = new ConcurrentHashMap<>();
    private final Map<String, KinesisClient>                 kinesisClients             = new ConcurrentHashMap<>();
    private final Map<String, FirehoseClient>                firehoseClients            = new ConcurrentHashMap<>();
    private final Map<String, KinesisAnalyticsV2Client>      kinesisAnalyticsV2Clients  = new ConcurrentHashMap<>();
    private final Map<String, EventBridgeClient>             eventBridgeClients         = new ConcurrentHashMap<>();
    private final Map<String, SfnClient>                     sfnClients                 = new ConcurrentHashMap<>();
    private final Map<String, AppConfigClient>               appConfigClients           = new ConcurrentHashMap<>();
    private final Map<String, AccountClient>                 accountClients             = new ConcurrentHashMap<>();
    private final Map<String, BudgetsClient>                 budgetsClients             = new ConcurrentHashMap<>();
    private final Map<String, CostExplorerClient>            costExplorerClients        = new ConcurrentHashMap<>();
    private final Map<String, GlueClient>                    glueClients                = new ConcurrentHashMap<>();
    private final Map<String, LakeFormationClient>           lakeFormationClients       = new ConcurrentHashMap<>();
    private final Map<String, AthenaClient>                  athenaClients              = new ConcurrentHashMap<>();
    private final Map<String, EmrClient>                     emrClients                 = new ConcurrentHashMap<>();
    private final Map<String, DataPipelineClient>            dataPipelineClients        = new ConcurrentHashMap<>();
    private final Map<String, CloudFormationClient>           cloudFormationClients       = new ConcurrentHashMap<>();
    private final Map<String, ServiceCatalogClient>           serviceCatalogClients       = new ConcurrentHashMap<>();
    private final Map<String, BackupClient>                   backupClients               = new ConcurrentHashMap<>();
    private final Map<String, LicenseManagerClient>           licenseManagerClients       = new ConcurrentHashMap<>();
    private final Map<String, ResourceGroupsClient>           resourceGroupsClients       = new ConcurrentHashMap<>();
    private final Map<String, ResourceGroupsTaggingApiClient> resourceGroupsTaggingClients = new ConcurrentHashMap<>();
    private final Map<String, SesClient>                      sesClients                   = new ConcurrentHashMap<>();
    private final Map<String, SesV2Client>                    sesV2Clients                 = new ConcurrentHashMap<>();

    // =========================================================================
    // Public accessors
    // =========================================================================

    public Ec2Client getEc2Client(ProviderContext providerContext)
    {
        return getClient(ec2Clients, providerContext,
            (profile, region) -> buildClient(Ec2Client.builder(), profile, region, CLIENT_NAME_EC2));
    }

    public AutoScalingClient getAutoScalingClient(ProviderContext providerContext)
    {
        return getClient(autoScalingClients, providerContext,
            (profile, region) -> buildClient(AutoScalingClient.builder(), profile, region, CLIENT_NAME_AUTOSCALING));
    }

    public ElasticLoadBalancingClient getElbClient(ProviderContext providerContext)
    {
        return getClient(elbClients, providerContext,
            (profile, region) -> buildClient(ElasticLoadBalancingClient.builder(), profile, region, CLIENT_NAME_ELB));
    }

    public ElasticLoadBalancingV2Client getElbV2Client(ProviderContext providerContext)
    {
        return getClient(elbV2Clients, providerContext,
            (profile, region) -> buildClient(ElasticLoadBalancingV2Client.builder(), profile, region, CLIENT_NAME_ELB_V2));
    }

    public EcsClient getEcsClient(ProviderContext providerContext)
    {
        return getClient(ecsClients, providerContext,
            (profile, region) -> buildClient(EcsClient.builder(), profile, region, CLIENT_NAME_ECS));
    }

    public EksClient getEksClient(ProviderContext providerContext)
    {
        return getClient(eksClients, providerContext,
            (profile, region) -> buildClient(EksClient.builder(), profile, region, CLIENT_NAME_EKS));
    }

    public LambdaClient getLambdaClient(ProviderContext providerContext)
    {
        return getClient(lambdaClients, providerContext,
            (profile, region) -> buildClient(LambdaClient.builder(), profile, region, CLIENT_NAME_LAMBDA));
    }

    public S3Client getS3Client(ProviderContext providerContext)
    {
        return getClient(s3Clients, providerContext,
            (profile, region) -> buildClient(S3Client.builder(), profile, region, CLIENT_NAME_S3));
    }

    public RdsClient getRdsClient(ProviderContext providerContext)
    {
        return getClient(rdsClients, providerContext,
            (profile, region) -> buildClient(RdsClient.builder(), profile, region, CLIENT_NAME_RDS));
    }

    public DynamoDbClient getDynamoDbClient(ProviderContext providerContext)
    {
        return getClient(dynamoDbClients, providerContext,
            (profile, region) -> buildClient(DynamoDbClient.builder(), profile, region, CLIENT_NAME_DYNAMODB));
    }

    public SqsClient getSqsClient(ProviderContext providerContext)
    {
        return getClient(sqsClients, providerContext,
            (profile, region) -> buildClient(SqsClient.builder(), profile, region, CLIENT_NAME_SQS));
    }

    public SnsClient getSnsClient(ProviderContext providerContext)
    {
        return getClient(snsClients, providerContext,
            (profile, region) -> buildClient(SnsClient.builder(), profile, region, CLIENT_NAME_SNS));
    }

    public IamClient getIamClient(ProviderContext providerContext)
    {
        // IAM is global — region is not applied
        return getClient(iamClients, providerContext,
            (profile, region) -> buildClient(IamClient.builder(), profile, region, CLIENT_NAME_IAM));
    }

    public KmsClient getKmsClient(ProviderContext providerContext)
    {
        return getClient(kmsClients, providerContext,
            (profile, region) -> buildClient(KmsClient.builder(), profile, region, CLIENT_NAME_KMS));
    }

    public SecretsManagerClient getSecretsManagerClient(ProviderContext providerContext)
    {
        return getClient(secretsManagerClients, providerContext,
            (profile, region) -> buildClient(SecretsManagerClient.builder(), profile, region, CLIENT_NAME_SECRETS_MANAGER));
    }

    public SsmClient getSsmClient(ProviderContext providerContext)
    {
        return getClient(ssmClients, providerContext,
            (profile, region) -> buildClient(SsmClient.builder(), profile, region, CLIENT_NAME_SSM));
    }

    public CloudWatchClient getCloudWatchClient(ProviderContext providerContext)
    {
        return getClient(cloudWatchClients, providerContext,
            (profile, region) -> buildClient(CloudWatchClient.builder(), profile, region, CLIENT_NAME_CLOUDWATCH));
    }

    public CloudWatchLogsClient getCloudWatchLogsClient(ProviderContext providerContext)
    {
        return getClient(cloudWatchLogsClients, providerContext,
            (profile, region) -> buildClient(CloudWatchLogsClient.builder(), profile, region, CLIENT_NAME_CLOUDWATCH_LOGS));
    }

    public CloudTrailClient getCloudTrailClient(ProviderContext providerContext)
    {
        return getClient(cloudTrailClients, providerContext,
            (profile, region) -> buildClient(CloudTrailClient.builder(), profile, region, CLIENT_NAME_CLOUDTRAIL));
    }

    public StsClient getStsClient(ProviderContext providerContext)
    {
        return getClient(stsClients, providerContext,
            (profile, region) -> buildClient(StsClient.builder(), profile, region, CLIENT_NAME_STS));
    }

    public GlobalAcceleratorClient getGlobalAcceleratorClient(ProviderContext providerContext)
    {
        // Global Accelerator is a global service fronted from us-west-2
        return getClient(globalAcceleratorClients, providerContext,
            (profile, region) -> buildClient(GlobalAcceleratorClient.builder(), profile, "us-west-2", CLIENT_NAME_GLOBAL_ACCELERATOR));
    }

    public AppMeshClient getAppMeshClient(ProviderContext providerContext)
    {
        return getClient(appMeshClients, providerContext,
            (profile, region) -> buildClient(AppMeshClient.builder(), profile, region, CLIENT_NAME_APP_MESH));
    }

    public ApiGatewayClient getApiGatewayClient(ProviderContext providerContext)
    {
        return getClient(apiGatewayClients, providerContext,
            (profile, region) -> buildClient(ApiGatewayClient.builder(), profile, region, CLIENT_NAME_API_GATEWAY));
    }

    public EfsClient getEfsClient(ProviderContext providerContext)
    {
        return getClient(efsClients, providerContext,
            (profile, region) -> buildClient(EfsClient.builder(), profile, region, CLIENT_NAME_EFS));
    }

    public FSxClient getFsxClient(ProviderContext providerContext)
    {
        return getClient(fsxClients, providerContext,
            (profile, region) -> buildClient(FSxClient.builder(), profile, region, CLIENT_NAME_FSX));
    }

    public StorageGatewayClient getStorageGatewayClient(ProviderContext providerContext)
    {
        return getClient(storageGatewayClients, providerContext,
            (profile, region) -> buildClient(StorageGatewayClient.builder(), profile, region, CLIENT_NAME_STORAGE_GATEWAY));
    }

    public CloudFrontClient getCloudFrontClient(ProviderContext providerContext)
    {
        // CloudFront is a global service — always uses us-east-1
        return getClient(cloudFrontClients, providerContext,
            (profile, region) -> buildClient(CloudFrontClient.builder(), profile, "us-east-1", CLIENT_NAME_CLOUDFRONT));
    }

    public S3ControlClient getS3ControlClient(ProviderContext providerContext)
    {
        return getClient(s3ControlClients, providerContext,
            (profile, region) -> buildClient(S3ControlClient.builder(), profile, region, CLIENT_NAME_S3_CONTROL));
    }

    public OrganizationsClient getOrganizationsClient(ProviderContext providerContext)
    {
        // Organizations is a global service — always us-east-1
        return getClient(organizationsClients, providerContext,
            (profile, region) -> buildClient(OrganizationsClient.builder(), profile, "us-east-1", CLIENT_NAME_ORGANIZATIONS));
    }

    public SsoAdminClient getSsoAdminClient(ProviderContext providerContext)
    {
        return getClient(ssoAdminClients, providerContext,
            (profile, region) -> buildClient(SsoAdminClient.builder(), profile, region, CLIENT_NAME_SSO_ADMIN));
    }

    public IdentitystoreClient getIdentityStoreClient(ProviderContext providerContext)
    {
        return getClient(identityStoreClients, providerContext,
            (profile, region) -> buildClient(IdentitystoreClient.builder(), profile, region, CLIENT_NAME_IDENTITY_STORE));
    }

    public Wafv2Client getWafv2Client(ProviderContext providerContext)
    {
        return getClient(wafv2Clients, providerContext,
            (profile, region) -> buildClient(Wafv2Client.builder(), profile, region, CLIENT_NAME_WAFV2));
    }

    public ShieldClient getShieldClient(ProviderContext providerContext)
    {
        // Shield Advanced is a global service — always us-east-1
        return getClient(shieldClients, providerContext,
            (profile, region) -> buildClient(ShieldClient.builder(), profile, "us-east-1", CLIENT_NAME_SHIELD));
    }

    public Macie2Client getMacie2Client(ProviderContext providerContext)
    {
        return getClient(macie2Clients, providerContext,
            (profile, region) -> buildClient(Macie2Client.builder(), profile, region, CLIENT_NAME_MACIE2));
    }

    public GuardDutyClient getGuardDutyClient(ProviderContext providerContext)
    {
        return getClient(guardDutyClients, providerContext,
            (profile, region) -> buildClient(GuardDutyClient.builder(), profile, region, CLIENT_NAME_GUARDDUTY));
    }

    public SecurityHubClient getSecurityHubClient(ProviderContext providerContext)
    {
        return getClient(securityHubClients, providerContext,
            (profile, region) -> buildClient(SecurityHubClient.builder(), profile, region, CLIENT_NAME_SECURITYHUB));
    }

    public ConfigClient getConfigClient(ProviderContext providerContext)
    {
        return getClient(configClients, providerContext,
            (profile, region) -> buildClient(ConfigClient.builder(), profile, region, CLIENT_NAME_CONFIG));
    }

    public OpenSearchClient getOpenSearchClient(ProviderContext providerContext)
    {
        return getClient(openSearchClients, providerContext,
            (profile, region) -> buildClient(OpenSearchClient.builder(), profile, region, CLIENT_NAME_OPENSEARCH));
    }

    public RedshiftClient getRedshiftClient(ProviderContext providerContext)
    {
        return getClient(redshiftClients, providerContext,
            (profile, region) -> buildClient(RedshiftClient.builder(), profile, region, CLIENT_NAME_REDSHIFT));
    }

    public ElastiCacheClient getElastiCacheClient(ProviderContext providerContext)
    {
        return getClient(elastiCacheClients, providerContext,
            (profile, region) -> buildClient(ElastiCacheClient.builder(), profile, region, CLIENT_NAME_ELASTICACHE));
    }

    public NeptuneClient getNeptuneClient(ProviderContext providerContext)
    {
        return getClient(neptuneClients, providerContext,
            (profile, region) -> buildClient(NeptuneClient.builder(), profile, region, CLIENT_NAME_NEPTUNE));
    }

    public TimestreamWriteClient getTimestreamWriteClient(ProviderContext providerContext)
    {
        return getClient(timestreamWriteClients, providerContext,
            (profile, region) -> buildClient(TimestreamWriteClient.builder(), profile, region, CLIENT_NAME_TIMESTREAM_WRITE));
    }

    public KafkaClient getKafkaClient(ProviderContext providerContext)
    {
        return getClient(kafkaClients, providerContext,
            (profile, region) -> buildClient(KafkaClient.builder(), profile, region, CLIENT_NAME_MSK));
    }

    public KinesisClient getKinesisClient(ProviderContext providerContext)
    {
        return getClient(kinesisClients, providerContext,
            (profile, region) -> buildClient(KinesisClient.builder(), profile, region, CLIENT_NAME_KINESIS));
    }

    public FirehoseClient getFirehoseClient(ProviderContext providerContext)
    {
        return getClient(firehoseClients, providerContext,
            (profile, region) -> buildClient(FirehoseClient.builder(), profile, region, CLIENT_NAME_FIREHOSE));
    }

    public KinesisAnalyticsV2Client getKinesisAnalyticsV2Client(ProviderContext providerContext)
    {
        return getClient(kinesisAnalyticsV2Clients, providerContext,
            (profile, region) -> buildClient(KinesisAnalyticsV2Client.builder(), profile, region, CLIENT_NAME_KINESIS_ANALYTICS_V2));
    }

    public EventBridgeClient getEventBridgeClient(ProviderContext providerContext)
    {
        return getClient(eventBridgeClients, providerContext,
            (profile, region) -> buildClient(EventBridgeClient.builder(), profile, region, CLIENT_NAME_EVENTBRIDGE));
    }

    public SfnClient getSfnClient(ProviderContext providerContext)
    {
        return getClient(sfnClients, providerContext,
            (profile, region) -> buildClient(SfnClient.builder(), profile, region, CLIENT_NAME_SFN));
    }

    public AppConfigClient getAppConfigClient(ProviderContext providerContext)
    {
        return getClient(appConfigClients, providerContext,
            (profile, region) -> buildClient(AppConfigClient.builder(), profile, region, CLIENT_NAME_APPCONFIG));
    }

    public AccountClient getAccountClient(ProviderContext providerContext)
    {
        return getClient(accountClients, providerContext,
            (profile, region) -> buildClient(AccountClient.builder(), profile, region, CLIENT_NAME_ACCOUNT));
    }

    public BudgetsClient getBudgetsClient(ProviderContext providerContext)
    {
        return getClient(budgetsClients, providerContext,
            (profile, region) -> buildClient(BudgetsClient.builder(), profile, region, CLIENT_NAME_BUDGETS));
    }

    public CostExplorerClient getCostExplorerClient(ProviderContext providerContext)
    {
        return getClient(costExplorerClients, providerContext,
            (profile, region) -> buildClient(CostExplorerClient.builder(), profile, region, CLIENT_NAME_COST_EXPLORER));
    }

    public GlueClient getGlueClient(ProviderContext providerContext)
    {
        return getClient(glueClients, providerContext,
            (profile, region) -> buildClient(GlueClient.builder(), profile, region, CLIENT_NAME_GLUE));
    }

    public LakeFormationClient getLakeFormationClient(ProviderContext providerContext)
    {
        return getClient(lakeFormationClients, providerContext,
            (profile, region) -> buildClient(LakeFormationClient.builder(), profile, region, CLIENT_NAME_LAKE_FORMATION));
    }

    public AthenaClient getAthenaClient(ProviderContext providerContext)
    {
        return getClient(athenaClients, providerContext,
            (profile, region) -> buildClient(AthenaClient.builder(), profile, region, CLIENT_NAME_ATHENA));
    }

    public EmrClient getEmrClient(ProviderContext providerContext)
    {
        return getClient(emrClients, providerContext,
            (profile, region) -> buildClient(EmrClient.builder(), profile, region, CLIENT_NAME_EMR));
    }

    public DataPipelineClient getDataPipelineClient(ProviderContext providerContext)
    {
        return getClient(dataPipelineClients, providerContext,
            (profile, region) -> buildClient(DataPipelineClient.builder(), profile, region, CLIENT_NAME_DATA_PIPELINE));
    }

    public CloudFormationClient getCloudFormationClient(ProviderContext providerContext)
    {
        return getClient(cloudFormationClients, providerContext,
            (profile, region) -> buildClient(CloudFormationClient.builder(), profile, region, CLIENT_NAME_CLOUDFORMATION));
    }

    public ServiceCatalogClient getServiceCatalogClient(ProviderContext providerContext)
    {
        return getClient(serviceCatalogClients, providerContext,
            (profile, region) -> buildClient(ServiceCatalogClient.builder(), profile, region, CLIENT_NAME_SERVICE_CATALOG));
    }

    public BackupClient getBackupClient(ProviderContext providerContext)
    {
        return getClient(backupClients, providerContext,
            (profile, region) -> buildClient(BackupClient.builder(), profile, region, CLIENT_NAME_BACKUP));
    }

    public LicenseManagerClient getLicenseManagerClient(ProviderContext providerContext)
    {
        return getClient(licenseManagerClients, providerContext,
            (profile, region) -> buildClient(LicenseManagerClient.builder(), profile, region, CLIENT_NAME_LICENSE_MANAGER));
    }

    public ResourceGroupsClient getResourceGroupsClient(ProviderContext providerContext)
    {
        return getClient(resourceGroupsClients, providerContext,
            (profile, region) -> buildClient(ResourceGroupsClient.builder(), profile, region, CLIENT_NAME_RESOURCE_GROUPS));
    }

    public ResourceGroupsTaggingApiClient getResourceGroupsTaggingApiClient(ProviderContext providerContext)
    {
        return getClient(resourceGroupsTaggingClients, providerContext,
            (profile, region) -> buildClient(ResourceGroupsTaggingApiClient.builder(), profile, region, CLIENT_NAME_RESOURCE_GROUPS_TAGGING));
    }

    public SesClient getSesClient(ProviderContext providerContext)
    {
        return getClient(sesClients, providerContext,
            (profile, region) -> buildClient(SesClient.builder(), profile, region, CLIENT_NAME_SES));
    }

    public SesV2Client getSesV2Client(ProviderContext providerContext)
    {
        return getClient(sesV2Clients, providerContext,
            (profile, region) -> buildClient(SesV2Client.builder(), profile, region, CLIENT_NAME_SES_V2));
    }

    // =========================================================================
    // Generic cache core
    // =========================================================================

    @FunctionalInterface
    private interface ClientCreator<C>
    {
        C create(String profile, String region);
    }

    private <C> C getClient(
        Map<String, C> cache,
        ProviderContext providerContext,
        ClientCreator<C> creator)
    {
        Validate.notNull(providerContext, "required: providerContext");

        String profile  = resolveProfile(providerContext);
        String region   = resolveRegion(providerContext);
        String cacheKey = buildCacheKey(profile, region);

        return cache.computeIfAbsent(cacheKey, k -> creator.create(profile, region));
    }

    private static String buildCacheKey(String profile, String region)
    {
        String resolvedProfile = StringUtils.isNotBlank(profile) ? profile : DEFAULT_PROFILE;
        String resolvedRegion  = StringUtils.isNotBlank(region)  ? region  : DEFAULT_REGION;
        return resolvedProfile + CACHE_KEY_SEPARATOR + resolvedRegion;
    }

    // =========================================================================
    // Generic builder
    // =========================================================================

    private <B extends AwsClientBuilder<B, C>, C> C buildClient(
        B builder,
        String profile,
        String region,
        String clientName)
    {
        builder.overrideConfiguration(
            ClientOverrideConfiguration.builder()
                .addExecutionInterceptor(new AwsHttpLoggingInterceptor())
                .build());

        if (StringUtils.isNotBlank(profile))
        {
            builder.credentialsProvider(ProfileCredentialsProvider.create(profile));
            log.debug("AwsClientFactory.build{} using profile: {}", clientName, profile);
        }

        if (StringUtils.isNotBlank(region))
        {
            builder.region(Region.of(region));
            log.debug("AwsClientFactory.build{} using region: {}", clientName, region);
        }
        else
        {
            log.debug("AwsClientFactory.build{} using default region for profile", clientName);
        }

        return builder.build();
    }

    // =========================================================================
    // Context helpers
    // =========================================================================

    private static String resolveProfile(ProviderContext providerContext)
    {
        if (providerContext != null && providerContext.getAttributes() != null)
        {
            Object found = providerContext.getAttributes().get(PROVIDER_CONTEXT_ATTRIBUTE_PROFILE);
            if (found != null)
            {
                return found.toString();
            }
        }
        return DEFAULT_PROFILE;
    }

    private static String resolveRegion(ProviderContext providerContext)
    {
        if (providerContext != null && providerContext.getAttributes() != null)
        {
            Object found = providerContext.getAttributes().get(PROVIDER_CONTEXT_ATTRIBUTE_REGION);
            if (found != null)
            {
                return found.toString();
            }
        }
        return null;
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    public void shutdown()
    {
        int clientTypeCount = 60;
        ExecutorService executor = Executors.newFixedThreadPool(clientTypeCount);

        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();

        futures.add(executor.submit(() -> closeAll(ec2Clients,            CLIENT_NAME_EC2)));
        futures.add(executor.submit(() -> closeAll(autoScalingClients,    CLIENT_NAME_AUTOSCALING)));
        futures.add(executor.submit(() -> closeAll(elbClients,            CLIENT_NAME_ELB)));
        futures.add(executor.submit(() -> closeAll(elbV2Clients,          CLIENT_NAME_ELB_V2)));
        futures.add(executor.submit(() -> closeAll(ecsClients,            CLIENT_NAME_ECS)));
        futures.add(executor.submit(() -> closeAll(eksClients,            CLIENT_NAME_EKS)));
        futures.add(executor.submit(() -> closeAll(lambdaClients,         CLIENT_NAME_LAMBDA)));
        futures.add(executor.submit(() -> closeAll(s3Clients,             CLIENT_NAME_S3)));
        futures.add(executor.submit(() -> closeAll(rdsClients,            CLIENT_NAME_RDS)));
        futures.add(executor.submit(() -> closeAll(dynamoDbClients,       CLIENT_NAME_DYNAMODB)));
        futures.add(executor.submit(() -> closeAll(sqsClients,            CLIENT_NAME_SQS)));
        futures.add(executor.submit(() -> closeAll(snsClients,            CLIENT_NAME_SNS)));
        futures.add(executor.submit(() -> closeAll(iamClients,            CLIENT_NAME_IAM)));
        futures.add(executor.submit(() -> closeAll(kmsClients,            CLIENT_NAME_KMS)));
        futures.add(executor.submit(() -> closeAll(secretsManagerClients, CLIENT_NAME_SECRETS_MANAGER)));
        futures.add(executor.submit(() -> closeAll(ssmClients,            CLIENT_NAME_SSM)));
        futures.add(executor.submit(() -> closeAll(cloudWatchClients,     CLIENT_NAME_CLOUDWATCH)));
        futures.add(executor.submit(() -> closeAll(cloudWatchLogsClients, CLIENT_NAME_CLOUDWATCH_LOGS)));
        futures.add(executor.submit(() -> closeAll(cloudTrailClients,     CLIENT_NAME_CLOUDTRAIL)));
        futures.add(executor.submit(() -> closeAll(stsClients,            CLIENT_NAME_STS)));
        futures.add(executor.submit(() -> closeAll(efsClients,            CLIENT_NAME_EFS)));
        futures.add(executor.submit(() -> closeAll(fsxClients,            CLIENT_NAME_FSX)));
        futures.add(executor.submit(() -> closeAll(storageGatewayClients, CLIENT_NAME_STORAGE_GATEWAY)));
        futures.add(executor.submit(() -> closeAll(cloudFrontClients,     CLIENT_NAME_CLOUDFRONT)));
        futures.add(executor.submit(() -> closeAll(s3ControlClients,      CLIENT_NAME_S3_CONTROL)));
        futures.add(executor.submit(() -> closeAll(organizationsClients,   CLIENT_NAME_ORGANIZATIONS)));
        futures.add(executor.submit(() -> closeAll(ssoAdminClients,        CLIENT_NAME_SSO_ADMIN)));
        futures.add(executor.submit(() -> closeAll(identityStoreClients,   CLIENT_NAME_IDENTITY_STORE)));
        futures.add(executor.submit(() -> closeAll(wafv2Clients,           CLIENT_NAME_WAFV2)));
        futures.add(executor.submit(() -> closeAll(shieldClients,          CLIENT_NAME_SHIELD)));
        futures.add(executor.submit(() -> closeAll(macie2Clients,          CLIENT_NAME_MACIE2)));
        futures.add(executor.submit(() -> closeAll(guardDutyClients,       CLIENT_NAME_GUARDDUTY)));
        futures.add(executor.submit(() -> closeAll(securityHubClients,     CLIENT_NAME_SECURITYHUB)));
        futures.add(executor.submit(() -> closeAll(configClients,          CLIENT_NAME_CONFIG)));
        futures.add(executor.submit(() -> closeAll(openSearchClients,      CLIENT_NAME_OPENSEARCH)));
        futures.add(executor.submit(() -> closeAll(redshiftClients,        CLIENT_NAME_REDSHIFT)));
        futures.add(executor.submit(() -> closeAll(elastiCacheClients,     CLIENT_NAME_ELASTICACHE)));
        futures.add(executor.submit(() -> closeAll(neptuneClients,         CLIENT_NAME_NEPTUNE)));
        futures.add(executor.submit(() -> closeAll(timestreamWriteClients, CLIENT_NAME_TIMESTREAM_WRITE)));
        futures.add(executor.submit(() -> closeAll(kafkaClients,           CLIENT_NAME_MSK)));
        futures.add(executor.submit(() -> closeAll(kinesisClients,          CLIENT_NAME_KINESIS)));
        futures.add(executor.submit(() -> closeAll(firehoseClients,         CLIENT_NAME_FIREHOSE)));
        futures.add(executor.submit(() -> closeAll(kinesisAnalyticsV2Clients, CLIENT_NAME_KINESIS_ANALYTICS_V2)));
        futures.add(executor.submit(() -> closeAll(eventBridgeClients,      CLIENT_NAME_EVENTBRIDGE)));
        futures.add(executor.submit(() -> closeAll(sfnClients,              CLIENT_NAME_SFN)));
        futures.add(executor.submit(() -> closeAll(appConfigClients,        CLIENT_NAME_APPCONFIG)));
        futures.add(executor.submit(() -> closeAll(accountClients,         CLIENT_NAME_ACCOUNT)));
        futures.add(executor.submit(() -> closeAll(budgetsClients,         CLIENT_NAME_BUDGETS)));
        futures.add(executor.submit(() -> closeAll(costExplorerClients,    CLIENT_NAME_COST_EXPLORER)));
        futures.add(executor.submit(() -> closeAll(glueClients,            CLIENT_NAME_GLUE)));
        futures.add(executor.submit(() -> closeAll(lakeFormationClients,   CLIENT_NAME_LAKE_FORMATION)));
        futures.add(executor.submit(() -> closeAll(athenaClients,          CLIENT_NAME_ATHENA)));
        futures.add(executor.submit(() -> closeAll(emrClients,             CLIENT_NAME_EMR)));
        futures.add(executor.submit(() -> closeAll(dataPipelineClients,         CLIENT_NAME_DATA_PIPELINE)));
        futures.add(executor.submit(() -> closeAll(cloudFormationClients,         CLIENT_NAME_CLOUDFORMATION)));
        futures.add(executor.submit(() -> closeAll(serviceCatalogClients,         CLIENT_NAME_SERVICE_CATALOG)));
        futures.add(executor.submit(() -> closeAll(backupClients,                 CLIENT_NAME_BACKUP)));
        futures.add(executor.submit(() -> closeAll(licenseManagerClients,         CLIENT_NAME_LICENSE_MANAGER)));
        futures.add(executor.submit(() -> closeAll(resourceGroupsClients,         CLIENT_NAME_RESOURCE_GROUPS)));
        futures.add(executor.submit(() -> closeAll(resourceGroupsTaggingClients,  CLIENT_NAME_RESOURCE_GROUPS_TAGGING)));
        futures.add(executor.submit(() -> closeAll(sesClients,                     CLIENT_NAME_SES)));
        futures.add(executor.submit(() -> closeAll(sesV2Clients,                   CLIENT_NAME_SES_V2)));

        executor.shutdown();

        try
        {
            boolean completed = executor.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!completed)
            {
                log.warn("AwsClientFactory.shutdown timed out after {}s — some clients may not have closed cleanly",
                    SHUTDOWN_TIMEOUT_SECONDS);
                executor.shutdownNow();
            }
            else
            {
                log.debug("AwsClientFactory.shutdown completed cleanly");
            }
        }
        catch (InterruptedException e)
        {
            log.warn("AwsClientFactory.shutdown interrupted", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    private <C extends AutoCloseable> void closeAll(Map<String, C> cache, String clientName)
    {
        cache.forEach((key, client) ->
        {
            try
            {
                client.close();
                log.debug("AwsClientFactory.shutdown closed {} client: {}", clientName, key);
            }
            catch (Exception e)
            {
                log.warn("AwsClientFactory.shutdown error closing {} client: {}", clientName, key, e);
            }
        });
        cache.clear();
    }
}
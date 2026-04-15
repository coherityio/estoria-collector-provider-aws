package io.coherity.estoria.collector.provider.aws;

import java.util.regex.Pattern;

public final class ARNHelper
{
    // ===== Core ARN Constants =====
    public static final String ARN_PREFIX = "arn";
    public static final String DEFAULT_PARTITION = "aws";

    public static final String SEPARATOR = ":";
    public static final String RESOURCE_SEPARATOR_SLASH = "/";
    public static final String RESOURCE_SEPARATOR_COLON = ":";

    // ===== Partitions =====
    public static final String PARTITION_AWS = "aws";
    public static final String PARTITION_AWS_GOV = "aws-us-gov";
    public static final String PARTITION_AWS_CN = "aws-cn";

    // ===== Services =====
    public static final String SERVICE_EC2 = "ec2";
    public static final String SERVICE_ECS = "ecs";
    public static final String SERVICE_ECR = "ecr";
    public static final String SERVICE_IAM = "iam";
    public static final String SERVICE_LAMBDA = "lambda";
    public static final String SERVICE_CLOUDTRAIL = "cloudtrail";
    public static final String SERVICE_S3 = "s3";
    public static final String SERVICE_ELB = "elasticloadbalancing";
    public static final String SERVICE_GLOBAL_ACCELERATOR = "globalaccelerator";
    public static final String SERVICE_APP_MESH = "appmesh";
    public static final String SERVICE_EXECUTE_API = "execute-api";
    public static final String SERVICE_API_GATEWAY = "apigateway";
    public static final String SERVICE_CLOUDFORMATION = "cloudformation";
    public static final String SERVICE_GLUE = "glue";
    public static final String SERVICE_BACKUP = "backup";
    public static final String SERVICE_APPCONFIG = "appconfig";
    public static final String SERVICE_NEPTUNE = "neptune";
    public static final String SERVICE_SERVICE_CATALOG = "catalog";
    public static final String SERVICE_SES             = "ses";
    public static final String SERVICE_SES_V2          = "ses";

    // ===== SES Resource Types =====
    public static final String RESOURCE_CONFIGURATION_SET    = "configuration-set";
    public static final String RESOURCE_IDENTITY             = "identity";
    public static final String RESOURCE_RECEIPT_RULE_SET     = "receipt-rule-set";
    public static final String RESOURCE_TEMPLATE             = "template";

    // ===== EC2 Resource Types =====
    public static final String RESOURCE_VPC = "vpc";
    public static final String RESOURCE_SUBNET = "subnet";
    public static final String RESOURCE_SECURITY_GROUP = "security-group";
    public static final String RESOURCE_ROUTE_TABLE = "route-table";
    public static final String RESOURCE_INTERNET_GATEWAY = "internet-gateway";
    public static final String RESOURCE_NAT_GATEWAY = "natgateway";
    public static final String RESOURCE_VPC_ENDPOINT = "vpc-endpoint";
    public static final String RESOURCE_NETWORK_ACL = "network-acl";
    public static final String RESOURCE_TRANSIT_GATEWAY = "transit-gateway";
    public static final String RESOURCE_TRANSIT_GATEWAY_ATTACHMENT = "transit-gateway-attachment";
    public static final String RESOURCE_VPC_PEERING_CONNECTION = "vpc-peering-connection";
    public static final String RESOURCE_CUSTOMER_GATEWAY = "customer-gateway";
    public static final String RESOURCE_VPN_CONNECTION = "vpn-connection";
    public static final String RESOURCE_CARRIER_GATEWAY = "carrier-gateway";
    public static final String RESOURCE_LOCAL_GATEWAY = "local-gateway";
    public static final String RESOURCE_NETWORK_INTERFACE = "network-interface";
    public static final String RESOURCE_ELASTIC_IP = "elastic-ip";
    public static final String RESOURCE_INSTANCE = "instance";
    public static final String RESOURCE_LOADBALANCER = "loadbalancer";
    public static final String RESOURCE_VPC_ENDPOINT_SERVICE = "vpc-endpoint-service";
    public static final String RESOURCE_ACCELERATOR = "accelerator";
    public static final String RESOURCE_MESH = "mesh";
    public static final String RESOURCE_VPCLINKS = "vpclinks";

    // ===== EC2 Compute Resources =====
    public static final String RESOURCE_IMAGE = "image";
    public static final String RESOURCE_DEDICATED_HOST = "dedicated-host";
    public static final String RESOURCE_VOLUME = "volume";
    public static final String RESOURCE_SNAPSHOT = "snapshot";
    public static final String RESOURCE_LAUNCH_TEMPLATE = "launch-template";
    public static final String RESOURCE_KEY_PAIR = "key-pair";
    public static final String RESOURCE_PLACEMENT_GROUP = "placement-group";
    public static final String RESOURCE_CAPACITY_RESERVATION = "capacity-reservation";
    public static final String RESOURCE_SPOT_FLEET_REQUEST = "spot-fleet-request";

    // ===== ECS / ECR =====
    public static final String RESOURCE_CLUSTER = "cluster";
    public static final String RESOURCE_SERVICE = "service";
    public static final String RESOURCE_REPOSITORY = "repository";

    // ===== IAM =====
    public static final String RESOURCE_ROLE = "role";
    public static final String RESOURCE_POLICY = "policy";

    // ===== Lambda =====
    public static final String RESOURCE_FUNCTION = "function";

    // ===== CloudTrail =====
    public static final String RESOURCE_TRAIL = "trail";

    // ===== CloudFormation =====
    public static final String RESOURCE_STACKSET = "stackset";

    // ===== Glue =====
    public static final String RESOURCE_JOB = "job";

    // ===== Backup =====
    public static final String RESOURCE_BACKUP_VAULT = "backup-vault";
    public static final String RESOURCE_RECOVERY_POINT = "recovery-point";

    // ===== AppConfig =====
    public static final String RESOURCE_APPLICATION = "application";
    public static final String RESOURCE_CONFIGURATION_PROFILE = "configurationprofile";

    // ===== Neptune =====
    public static final String RESOURCE_CLUSTER_LOWER = "cluster";

    // ===== Service Catalog =====
    public static final String RESOURCE_PROVISIONED_PRODUCT = "stack";

    // ===== Regex =====
    public static final String ARN_REGEX = "^arn:([^:]*):([^:]*):([^:]*):([^:]*):(.*)$";
    private static final Pattern ARN_PATTERN = Pattern.compile(ARN_REGEX);

    private ARNHelper()
    {
    }

    public enum ResourceSeparator
    {
        NONE(""),
        SLASH(RESOURCE_SEPARATOR_SLASH),
        COLON(RESOURCE_SEPARATOR_COLON);

        private final String value;

        ResourceSeparator(String value)
        {
            this.value = value;
        }

        public String value()
        {
            return this.value;
        }
    }

    public static String buildArn(
        String partition,
        String service,
        String region,
        String accountId,
        String resource)
    {
        requireNonBlank(service, "service");
        requireNonBlank(resource, "resource");

        return ARN_PREFIX + SEPARATOR
            + defaultIfBlank(partition, DEFAULT_PARTITION) + SEPARATOR
            + nullToEmpty(service) + SEPARATOR
            + nullToEmpty(region) + SEPARATOR
            + nullToEmpty(accountId) + SEPARATOR
            + resource;
    }

    public static String buildArn(
        String partition,
        String service,
        String region,
        String accountId,
        String resourceType,
        String resourceId,
        ResourceSeparator resourceSeparator)
    {
        requireNonBlank(service, "service");
        requireNonBlank(resourceId, "resourceId");

        String resource = buildResource(resourceType, resourceId, resourceSeparator);
        return buildArn(partition, service, region, accountId, resource);
    }

    public static String buildResource(
        String resourceType,
        String resourceId,
        ResourceSeparator resourceSeparator)
    {
        requireNonBlank(resourceId, "resourceId");

        if (resourceType == null || resourceType.isBlank())
        {
            return resourceId;
        }

        ResourceSeparator separator =
            resourceSeparator == null ? ResourceSeparator.SLASH : resourceSeparator;

        if (separator == ResourceSeparator.NONE)
        {
            return resourceType + resourceId;
        }

        return resourceType + separator.value() + resourceId;
    }

    public static boolean isArn(String value)
    {
        return value != null && ARN_PATTERN.matcher(value).matches();
    }

    public static String partitionForRegion(String region)
    {
        if (region == null || region.isBlank())
        {
            return PARTITION_AWS;
        }

        if (region.startsWith("us-gov-"))
        {
            return PARTITION_AWS_GOV;
        }

        if (region.startsWith("cn-"))
        {
            return PARTITION_AWS_CN;
        }

        return PARTITION_AWS;
    }

    // ===== EC2 =====

    public static String ec2VpcArn(String region, String accountId, String vpcId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_VPC, vpcId, ResourceSeparator.SLASH);
    }

    public static String ec2SubnetArn(String region, String accountId, String subnetId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_SUBNET, subnetId, ResourceSeparator.SLASH);
    }

    public static String ec2SecurityGroupArn(String region, String accountId, String sgId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_SECURITY_GROUP, sgId, ResourceSeparator.SLASH);
    }

    public static String ec2InstanceArn(String region, String accountId, String instanceId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_INSTANCE, instanceId, ResourceSeparator.SLASH);
    }

    public static String ec2RouteTableArn(String region, String accountId, String routeTableId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_ROUTE_TABLE, routeTableId, ResourceSeparator.SLASH);
    }

    public static String ec2InternetGatewayArn(String region, String accountId, String igwId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_INTERNET_GATEWAY, igwId, ResourceSeparator.SLASH);
    }

    public static String ec2NatGatewayArn(String region, String accountId, String natGatewayId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_NAT_GATEWAY, natGatewayId, ResourceSeparator.SLASH);
    }

    public static String ec2VpcEndpointArn(String region, String accountId, String vpcEndpointId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_VPC_ENDPOINT, vpcEndpointId, ResourceSeparator.SLASH);
    }

    public static String ec2NetworkAclArn(String region, String accountId, String networkAclId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_NETWORK_ACL, networkAclId, ResourceSeparator.SLASH);
    }

    public static String ec2TransitGatewayArn(String region, String accountId, String tgwId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_TRANSIT_GATEWAY, tgwId, ResourceSeparator.SLASH);
    }

    public static String ec2TransitGatewayAttachmentArn(String region, String accountId, String attachmentId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_TRANSIT_GATEWAY_ATTACHMENT, attachmentId, ResourceSeparator.SLASH);
    }

    public static String ec2VpcPeeringConnectionArn(String region, String accountId, String peeringId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_VPC_PEERING_CONNECTION, peeringId, ResourceSeparator.SLASH);
    }

    public static String ec2CustomerGatewayArn(String region, String accountId, String cgwId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_CUSTOMER_GATEWAY, cgwId, ResourceSeparator.SLASH);
    }

    public static String ec2VpnConnectionArn(String region, String accountId, String vpnId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_VPN_CONNECTION, vpnId, ResourceSeparator.SLASH);
    }

    public static String ec2CarrierGatewayArn(String region, String accountId, String cgwId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_CARRIER_GATEWAY, cgwId, ResourceSeparator.SLASH);
    }

    public static String ec2LocalGatewayArn(String region, String accountId, String lgwId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_LOCAL_GATEWAY, lgwId, ResourceSeparator.SLASH);
    }

    public static String ec2NetworkInterfaceArn(String region, String accountId, String eniId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_NETWORK_INTERFACE, eniId, ResourceSeparator.SLASH);
    }

    public static String ec2ElasticIpArn(String region, String accountId, String allocationId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_ELASTIC_IP, allocationId, ResourceSeparator.SLASH);
    }

    public static String ec2ImageArn(String region, String accountId, String imageId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_IMAGE, imageId, ResourceSeparator.SLASH);
    }

    public static String ec2HostArn(String region, String accountId, String hostId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_DEDICATED_HOST, hostId, ResourceSeparator.SLASH);
    }

    public static String ec2VolumeArn(String region, String accountId, String volumeId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_VOLUME, volumeId, ResourceSeparator.SLASH);
    }

    public static String ec2SnapshotArn(String region, String accountId, String snapshotId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_SNAPSHOT, snapshotId, ResourceSeparator.SLASH);
    }

    public static String ec2LaunchTemplateArn(String region, String accountId, String templateId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_LAUNCH_TEMPLATE, templateId, ResourceSeparator.SLASH);
    }

    public static String ec2KeyPairArn(String region, String accountId, String keyPairId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_KEY_PAIR, keyPairId, ResourceSeparator.SLASH);
    }

    public static String ec2PlacementGroupArn(String region, String accountId, String groupId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_PLACEMENT_GROUP, groupId, ResourceSeparator.SLASH);
    }

    public static String ec2CapacityReservationArn(String region, String accountId, String reservationId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_CAPACITY_RESERVATION, reservationId, ResourceSeparator.SLASH);
    }

    public static String ec2SpotFleetArn(String region, String accountId, String requestId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_SPOT_FLEET_REQUEST, requestId, ResourceSeparator.SLASH);
    }

    // ===== ECS / ECR =====

    public static String ecsClusterArn(String region, String accountId, String clusterName)
    {
        return buildArn(partitionForRegion(region), SERVICE_ECS, region, accountId,
            RESOURCE_CLUSTER, clusterName, ResourceSeparator.SLASH);
    }

    public static String ecrRepositoryArn(String region, String accountId, String repoName)
    {
        return buildArn(partitionForRegion(region), SERVICE_ECR, region, accountId,
            RESOURCE_REPOSITORY, repoName, ResourceSeparator.SLASH);
    }

    // ===== IAM =====

    public static String iamRoleArn(String accountId, String rolePathAndName)
    {
        return buildArn(PARTITION_AWS, SERVICE_IAM, "", accountId,
            RESOURCE_ROLE + RESOURCE_SEPARATOR_SLASH + requireNonBlank(rolePathAndName, "rolePathAndName"));
    }

    // ===== Lambda =====

    public static String lambdaFunctionArn(String region, String accountId, String functionName)
    {
        return buildArn(partitionForRegion(region), SERVICE_LAMBDA, region, accountId,
            RESOURCE_FUNCTION, functionName, ResourceSeparator.COLON);
    }

    // ===== S3 =====

    public static String s3BucketArn(String bucketName)
    {
        requireNonBlank(bucketName, "bucketName");
        return ARN_PREFIX + SEPARATOR + PARTITION_AWS + SEPARATOR + SERVICE_S3
            + SEPARATOR + SEPARATOR + SEPARATOR + bucketName;
    }

    public static String s3ObjectArn(String bucketName, String objectKey)
    {
        requireNonBlank(bucketName, "bucketName");
        requireNonBlank(objectKey, "objectKey");

        return ARN_PREFIX + SEPARATOR + PARTITION_AWS + SEPARATOR + SERVICE_S3
            + SEPARATOR + SEPARATOR + SEPARATOR
            + bucketName + RESOURCE_SEPARATOR_SLASH + objectKey;
    }

    public static String s3MultiRegionAccessPointArn(String accountId, String name)
    {
        requireNonBlank(accountId, "accountId");
        requireNonBlank(name, "name");

        return ARN_PREFIX + SEPARATOR + PARTITION_AWS + SEPARATOR + SERVICE_S3
            + SEPARATOR + SEPARATOR + accountId + SEPARATOR
            + "accesspoint" + RESOURCE_SEPARATOR_SLASH + name;
    }

    // ===== ELB Classic =====

    public static String elbClassicArn(String region, String accountId, String lbName)
    {
        return buildArn(partitionForRegion(region), SERVICE_ELB, region, accountId,
            RESOURCE_LOADBALANCER + RESOURCE_SEPARATOR_SLASH + lbName);
    }

    // ===== EC2 PrivateLink =====

    public static String ec2PrivateLinkServiceArn(String region, String accountId, String serviceId)
    {
        return buildArn(partitionForRegion(region), SERVICE_EC2, region, accountId,
            RESOURCE_VPC_ENDPOINT_SERVICE, serviceId, ResourceSeparator.SLASH);
    }

    // ===== Global Accelerator =====

    public static String globalAcceleratorArn(String accountId, String acceleratorId)
    {
        return buildArn(PARTITION_AWS, SERVICE_GLOBAL_ACCELERATOR, "", accountId,
            RESOURCE_ACCELERATOR, acceleratorId, ResourceSeparator.SLASH);
    }

    // ===== App Mesh =====

    public static String appMeshMeshArn(String region, String accountId, String meshName)
    {
        return buildArn(partitionForRegion(region), SERVICE_APP_MESH, region, accountId,
            RESOURCE_MESH, meshName, ResourceSeparator.SLASH);
    }

    // ===== API Gateway =====
    // Management ARN style: arn:partition:apigateway:region::/vpclinks/{id}

    public static String apiGatewayVpcLinkArn(String region, String vpcLinkId)
    {
        requireNonBlank(region, "region");
        requireNonBlank(vpcLinkId, "vpcLinkId");

        return buildArn(partitionForRegion(region), SERVICE_API_GATEWAY, region, "",
            RESOURCE_SEPARATOR_SLASH + RESOURCE_VPCLINKS + RESOURCE_SEPARATOR_SLASH + vpcLinkId);
    }

    // ===== CloudFormation =====

    public static String cloudFormationStackSetArn(String region, String accountId, String stackSetName)
    {
        return buildArn(partitionForRegion(region), SERVICE_CLOUDFORMATION, region, accountId,
            RESOURCE_STACKSET, stackSetName, ResourceSeparator.SLASH);
    }

    public static String cloudFormationStackSetArn(String region, String accountId, String stackSetName, String stackSetId)
    {
        requireNonBlank(stackSetName, "stackSetName");

        if (stackSetId == null || stackSetId.isBlank())
        {
            return cloudFormationStackSetArn(region, accountId, stackSetName);
        }

        return buildArn(partitionForRegion(region), SERVICE_CLOUDFORMATION, region, accountId,
            RESOURCE_STACKSET + RESOURCE_SEPARATOR_SLASH + stackSetName + RESOURCE_SEPARATOR_COLON + stackSetId);
    }

    // ===== Glue =====

    public static String glueJobArn(String region, String accountId, String jobName)
    {
        return buildArn(partitionForRegion(region), SERVICE_GLUE, region, accountId,
            RESOURCE_JOB, jobName, ResourceSeparator.SLASH);
    }

    // ===== Backup =====

    public static String backupVaultArn(String region, String accountId, String vaultName)
    {
        return buildArn(partitionForRegion(region), SERVICE_BACKUP, region, accountId,
            RESOURCE_BACKUP_VAULT, vaultName, ResourceSeparator.COLON);
    }

    public static String backupRecoveryPointArn(String region, String accountId, String backupVaultName, String recoveryPointId)
    {
        requireNonBlank(backupVaultName, "backupVaultName");
        requireNonBlank(recoveryPointId, "recoveryPointId");

        return buildArn(partitionForRegion(region), SERVICE_BACKUP, region, accountId,
            RESOURCE_BACKUP_VAULT + RESOURCE_SEPARATOR_COLON + backupVaultName
                + RESOURCE_SEPARATOR_COLON + RESOURCE_RECOVERY_POINT + RESOURCE_SEPARATOR_COLON + recoveryPointId);
    }

    // ===== AppConfig =====

    public static String appConfigApplicationArn(String region, String accountId, String applicationId)
    {
        return buildArn(partitionForRegion(region), SERVICE_APPCONFIG, region, accountId,
            RESOURCE_APPLICATION, applicationId, ResourceSeparator.SLASH);
    }

    public static String appConfigConfigurationProfileArn(String region, String accountId, String applicationId, String configurationProfileId)
    {
        requireNonBlank(applicationId, "applicationId");
        requireNonBlank(configurationProfileId, "configurationProfileId");

        return buildArn(partitionForRegion(region), SERVICE_APPCONFIG, region, accountId,
            RESOURCE_APPLICATION + RESOURCE_SEPARATOR_SLASH + applicationId
                + RESOURCE_SEPARATOR_SLASH + RESOURCE_CONFIGURATION_PROFILE
                + RESOURCE_SEPARATOR_SLASH + configurationProfileId);
    }

    // ===== Neptune =====

    public static String neptuneClusterArn(String region, String accountId, String clusterId)
    {
        return buildArn(partitionForRegion(region), SERVICE_NEPTUNE, region, accountId,
            RESOURCE_CLUSTER_LOWER, clusterId, ResourceSeparator.COLON);
    }

    // ===== Service Catalog =====

    public static String serviceCatalogProvisionedProductArn(
        String region,
        String accountId,
        String provisionedProductId)
    {
        requireNonBlank(region, "region");
        requireNonBlank(accountId, "accountId");
        requireNonBlank(provisionedProductId, "provisionedProductId");

        return buildArn(
            partitionForRegion(region),
            SERVICE_SERVICE_CATALOG,
            region,
            accountId,
            RESOURCE_PROVISIONED_PRODUCT,
            provisionedProductId,
            ResourceSeparator.SLASH);
    }

    // ===== SES =====

    public static String sesConfigurationSetArn(String region, String accountId, String configSetName)
    {
        return buildArn(partitionForRegion(region), SERVICE_SES_V2, region, accountId,
            RESOURCE_CONFIGURATION_SET, configSetName, ResourceSeparator.SLASH);
    }

    public static String sesIdentityArn(String region, String accountId, String identityName)
    {
        return buildArn(partitionForRegion(region), SERVICE_SES_V2, region, accountId,
            RESOURCE_IDENTITY, identityName, ResourceSeparator.SLASH);
    }

    public static String sesReceiptRuleSetArn(String region, String accountId, String ruleSetName)
    {
        return buildArn(partitionForRegion(region), SERVICE_SES, region, accountId,
            RESOURCE_RECEIPT_RULE_SET, ruleSetName, ResourceSeparator.SLASH);
    }

    public static String sesTemplateArn(String region, String accountId, String templateName)
    {
        return buildArn(partitionForRegion(region), SERVICE_SES_V2, region, accountId,
            RESOURCE_TEMPLATE, templateName, ResourceSeparator.SLASH);
    }

    // ===== Helpers =====

    private static String requireNonBlank(String value, String name)
    {
        if (value == null || value.isBlank())
        {
            throw new IllegalArgumentException(name + " must not be null or blank");
        }

        return value;
    }

    private static String defaultIfBlank(String value, String defaultValue)
    {
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String nullToEmpty(String value)
    {
        return value == null ? "" : value;
    }
}
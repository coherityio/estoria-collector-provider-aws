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

    // ===== EC2 Resource Types =====
    public static final String RESOURCE_VPC = "vpc";
    public static final String RESOURCE_SUBNET = "subnet";
    public static final String RESOURCE_SECURITY_GROUP = "security-group";
    public static final String RESOURCE_ROUTE_TABLE = "route-table";
    public static final String RESOURCE_INTERNET_GATEWAY = "internet-gateway";
    public static final String RESOURCE_NAT_GATEWAY = "natgateway";
    public static final String RESOURCE_INSTANCE = "instance";

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
        return ARN_PREFIX + SEPARATOR + PARTITION_AWS + SEPARATOR + SERVICE_S3 + SEPARATOR + SEPARATOR + SEPARATOR + bucketName;
    }

    public static String s3ObjectArn(String bucketName, String objectKey)
    {
        requireNonBlank(bucketName, "bucketName");
        requireNonBlank(objectKey, "objectKey");

        return ARN_PREFIX + SEPARATOR + PARTITION_AWS + SEPARATOR + SERVICE_S3
            + SEPARATOR + SEPARATOR + SEPARATOR
            + bucketName + RESOURCE_SEPARATOR_SLASH + objectKey;
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
package io.coherity.estoria.collector.provider.aws;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.coherity.estoria.collector.spi.CollectorContext;
import io.coherity.estoria.collector.spi.EndpointReference;
import io.coherity.estoria.collector.spi.EndpointResolver;
import io.coherity.estoria.collector.spi.ProviderContext;

public class AwsEndpointResolver implements EndpointResolver
{
	private static final String DEFAULT_COMMERCIAL_PARTITION = "aws";
	private static final String COMMERCIAL_DNS_SUFFIX = "amazonaws.com";
	private static final String CHINA_DNS_SUFFIX = "amazonaws.com.cn";
	private static final String REGION_ATTRIBUTE = "region";
	private static final String PARTITION_ATTRIBUTE = "partition";
	private static final String DNS_SUFFIX_ATTRIBUTE = "dnsSuffix";
	private static final String ENDPOINT_OVERRIDE_ATTRIBUTE = "endpoint";
	private static final String AWS_ENDPOINT_OVERRIDE_ATTRIBUTE = "aws.endpoint";
	private static final Map<String, ServiceDefinition> SERVICE_DEFINITIONS = createServiceDefinitions();
	private static final Map<String, String> SERVICE_ALIASES = createServiceAliases();

	@Override
	public Optional<EndpointReference> resolve(String serviceId, ProviderContext providerContext)
	{
		if (!hasText(serviceId))
		{
			return Optional.empty();
		}

		String normalizedRequest = normalize(serviceId);
		String canonicalServiceId = SERVICE_ALIASES.getOrDefault(normalizedRequest, normalizedRequest);
		ServiceDefinition serviceDefinition = SERVICE_DEFINITIONS.get(canonicalServiceId);
		if (serviceDefinition == null)
		{
			return Optional.empty();
		}

		Optional<URI> endpointOverride = resolveOverrideUri(serviceId, canonicalServiceId, providerContext);
		if (endpointOverride.isPresent())
		{
			return Optional.of(EndpointReference.builder()
				.id(canonicalServiceId)
				.uri(endpointOverride.get())
				.metadata(createMetadata(serviceDefinition, providerContext, "override", endpointOverride.get(), normalizedRequest, canonicalServiceId))
				.build());
		}

		String region = resolveRegion(providerContext, serviceDefinition);
		if (!serviceDefinition.getEndpointScope().requiresRegion() || hasText(region))
		{
			String effectiveRegion = serviceDefinition.getEndpointScope().effectiveRegion(region, serviceDefinition.getFixedRegion());
			String partition = resolvePartition(providerContext, effectiveRegion);
			String dnsSuffix = resolveDnsSuffix(providerContext, partition);
			URI resolvedUri = serviceDefinition.toUri(effectiveRegion, dnsSuffix);
			return Optional.of(EndpointReference.builder()
				.id(canonicalServiceId)
				.uri(resolvedUri)
				.metadata(createMetadata(serviceDefinition, providerContext, "derived", resolvedUri, normalizedRequest, canonicalServiceId))
				.build());
		}

		return Optional.empty();
	}

	public Collection<String> supportedServiceIds()
	{
		return Collections.unmodifiableSet(new LinkedHashSet<>(SERVICE_DEFINITIONS.keySet()));
	}

	public Collection<String> supportedKeys()
	{
		Set<String> supportedKeys = new LinkedHashSet<>();
		supportedKeys.addAll(SERVICE_DEFINITIONS.keySet());
		supportedKeys.addAll(SERVICE_ALIASES.keySet());
		return Collections.unmodifiableSet(supportedKeys);
	}

	private Optional<URI> resolveOverrideUri(String requestServiceId, String canonicalServiceId, ProviderContext providerContext)
	{
		Map<String, Object> attributes = providerContext == null ? null : providerContext.getAttributes();
		if (attributes == null || attributes.isEmpty())
		{
			return Optional.empty();
		}

		List<String> candidateKeys = new ArrayList<>();
		candidateKeys.add(serviceSpecificOverrideKey(canonicalServiceId));
		candidateKeys.add(serviceSpecificOverrideKey(normalize(requestServiceId)));
		candidateKeys.add("aws.endpoint." + canonicalServiceId);
		candidateKeys.add(ENDPOINT_OVERRIDE_ATTRIBUTE + "." + canonicalServiceId);
		candidateKeys.add(AWS_ENDPOINT_OVERRIDE_ATTRIBUTE);
		candidateKeys.add(ENDPOINT_OVERRIDE_ATTRIBUTE);

		for (String candidateKey : candidateKeys)
		{
			String value = attributes.get(candidateKey).toString();
			if (hasText(value))
			{
				return Optional.of(URI.create(value));
			}
		}

		return Optional.empty();
	}

	private String serviceSpecificOverrideKey(String serviceKey)
	{
		return "endpoint." + serviceKey;
	}

	private String resolveRegion(ProviderContext providerContext, ServiceDefinition serviceDefinition)
	{
		if (serviceDefinition.getEndpointScope() == EndpointScope.GLOBAL)
		{
			return null;
		}
		if (serviceDefinition.getEndpointScope() == EndpointScope.FIXED_REGION)
		{
			return serviceDefinition.getFixedRegion();
		}

		Map<String, Object> attributes = providerContext == null ? null : providerContext.getAttributes();
		if (attributes == null)
		{
			return null;
		}

		String region = attributes.get(REGION_ATTRIBUTE).toString();
		return hasText(region) ? region : null;
	}

	private String resolvePartition(ProviderContext providerContext, String region)
	{
		Map<String, Object> attributes = providerContext == null ? null : providerContext.getAttributes();
		if (attributes != null)
		{
			String configuredPartition = attributes.get(PARTITION_ATTRIBUTE).toString();
			if (hasText(configuredPartition))
			{
				return configuredPartition;
			}
		}

		if (hasText(region))
		{
			if (region.startsWith("cn-"))
			{
				return "aws-cn";
			}
			if (region.startsWith("us-gov-"))
			{
				return "aws-us-gov";
			}
		}
		return DEFAULT_COMMERCIAL_PARTITION;
	}

	private String resolveDnsSuffix(ProviderContext providerContext, String partition)
	{
		Map<String, Object> attributes = providerContext == null ? null : providerContext.getAttributes();
		if (attributes != null)
		{
			String configuredDnsSuffix = attributes.get(DNS_SUFFIX_ATTRIBUTE).toString();
			if (hasText(configuredDnsSuffix))
			{
				return configuredDnsSuffix;
			}
		}

		if ("aws-cn".equals(partition))
		{
			return CHINA_DNS_SUFFIX;
		}
		return COMMERCIAL_DNS_SUFFIX;
	}

	private Map<String, Object> createMetadata(
		ServiceDefinition serviceDefinition,
		ProviderContext providerContext,
		String resolutionSource,
		URI resolvedUri,
		String requestKey,
		String canonicalServiceId)
	{
		Map<String, Object> metadata = new LinkedHashMap<>();
		metadata.put("providerId", providerContext == null ? null : providerContext.getProviderId());
		metadata.put("requestKey", requestKey);
		metadata.put("serviceId", canonicalServiceId);
		metadata.put("signingName", serviceDefinition.getSigningName());
		metadata.put("endpointPrefix", serviceDefinition.getEndpointPrefix());
		metadata.put("scope", serviceDefinition.getEndpointScope().name());
		metadata.put("resolutionSource", resolutionSource);
		metadata.put("uri", resolvedUri.toString());
		if (serviceDefinition.getFixedRegion() != null)
		{
			metadata.put("fixedRegion", serviceDefinition.getFixedRegion());
		}
		if (providerContext != null && providerContext.getAttributes() != null && !providerContext.getAttributes().isEmpty())
		{
			metadata.put("providerContextAttributes", Collections.unmodifiableMap(new LinkedHashMap<>(providerContext.getAttributes())));
		}
		return Collections.unmodifiableMap(metadata);
	}

	private static boolean hasText(String value)
	{
		return value != null && !value.isBlank();
	}

	private static String normalize(String value)
	{
		return value == null
			? null
			: value.trim()
				.toLowerCase(Locale.ROOT)
				.replace("collector", "")
				.replace("_", "")
				.replace("-", "")
				.replace(" ", "");
	}

	private static Map<String, ServiceDefinition> createServiceDefinitions()
	{
		Map<String, ServiceDefinition> definitions = new LinkedHashMap<>();

		definitions.put("account", ServiceDefinition.global("account", "account"));
		definitions.put("apigateway", ServiceDefinition.regional("apigateway", "apigateway"));
		definitions.put("appconfig", ServiceDefinition.regional("appconfig", "appconfig"));
		definitions.put("appmesh", ServiceDefinition.regional("appmesh", "appmesh"));
		definitions.put("appstream", ServiceDefinition.regional("appstream2", "appstream"));
		definitions.put("athena", ServiceDefinition.regional("athena", "athena"));
		definitions.put("autoscaling", ServiceDefinition.regional("autoscaling", "autoscaling"));
		definitions.put("backup", ServiceDefinition.regional("backup", "backup"));
		definitions.put("budgets", ServiceDefinition.global("budgets", "budgets"));
		definitions.put("ce", ServiceDefinition.fixedRegion("ce", "ce", "us-east-1"));
		definitions.put("cloud9", ServiceDefinition.regional("cloud9", "cloud9"));
		definitions.put("cloudformation", ServiceDefinition.regional("cloudformation", "cloudformation"));
		definitions.put("cloudfront", ServiceDefinition.global("cloudfront", "cloudfront"));
		definitions.put("cloudtrail", ServiceDefinition.regional("cloudtrail", "cloudtrail"));
		definitions.put("codeartifact", ServiceDefinition.regional("codeartifact", "codeartifact"));
		definitions.put("codebuild", ServiceDefinition.regional("codebuild", "codebuild"));
		definitions.put("codecommit", ServiceDefinition.regional("codecommit", "codecommit"));
		definitions.put("codedeploy", ServiceDefinition.regional("codedeploy", "codedeploy"));
		definitions.put("codeguruprofiler", ServiceDefinition.regional("codeguru-profiler", "codeguru-profiler"));
		definitions.put("codegurureviewer", ServiceDefinition.regional("codeguru-reviewer", "codeguru-reviewer"));
		definitions.put("codepipeline", ServiceDefinition.regional("codepipeline", "codepipeline"));
		definitions.put("config", ServiceDefinition.regional("config", "config"));
		definitions.put("datapipeline", ServiceDefinition.regional("datapipeline", "datapipeline"));
		definitions.put("dynamodb", ServiceDefinition.regional("dynamodb", "dynamodb"));
		definitions.put("ec2", ServiceDefinition.regional("ec2", "ec2"));
		definitions.put("ecs", ServiceDefinition.regional("ecs", "ecs"));
		definitions.put("eks", ServiceDefinition.regional("eks", "eks"));
		definitions.put("elasticache", ServiceDefinition.regional("elasticache", "elasticache"));
		definitions.put("elasticfilesystem", ServiceDefinition.regional("elasticfilesystem", "elasticfilesystem"));
		definitions.put("elasticloadbalancing", ServiceDefinition.regional("elasticloadbalancing", "elasticloadbalancing"));
		definitions.put("elasticmapreduce", ServiceDefinition.regional("elasticmapreduce", "elasticmapreduce"));
		definitions.put("es", ServiceDefinition.regional("es", "es"));
		definitions.put("events", ServiceDefinition.regional("events", "events"));
		definitions.put("firehose", ServiceDefinition.regional("firehose", "firehose"));
		definitions.put("fsx", ServiceDefinition.regional("fsx", "fsx"));
		definitions.put("glue", ServiceDefinition.regional("glue", "glue"));
		definitions.put("globalaccelerator", ServiceDefinition.global("globalaccelerator", "globalaccelerator"));
		definitions.put("guardduty", ServiceDefinition.regional("guardduty", "guardduty"));
		definitions.put("iam", ServiceDefinition.global("iam", "iam"));
		definitions.put("identitystore", ServiceDefinition.regional("identitystore", "identitystore"));
		definitions.put("kafka", ServiceDefinition.regional("kafka", "kafka"));
		definitions.put("kinesis", ServiceDefinition.regional("kinesis", "kinesis"));
		definitions.put("kinesisanalytics", ServiceDefinition.regional("kinesisanalytics", "kinesisanalytics"));
		definitions.put("kms", ServiceDefinition.regional("kms", "kms"));
		definitions.put("lakeformation", ServiceDefinition.regional("lakeformation", "lakeformation"));
		definitions.put("lambda", ServiceDefinition.regional("lambda", "lambda"));
		definitions.put("licensemanager", ServiceDefinition.regional("license-manager", "license-manager"));
		definitions.put("logs", ServiceDefinition.regional("logs", "logs"));
		definitions.put("macie2", ServiceDefinition.regional("macie2", "macie2"));
		definitions.put("monitoring", ServiceDefinition.regional("monitoring", "monitoring"));
		definitions.put("neptune", ServiceDefinition.regional("neptune-db", "neptune-db"));
		definitions.put("organizations", ServiceDefinition.global("organizations", "organizations"));
		definitions.put("rds", ServiceDefinition.regional("rds", "rds"));
		definitions.put("redshift", ServiceDefinition.regional("redshift", "redshift"));
		definitions.put("resourcegroups", ServiceDefinition.regional("resource-groups", "resource-groups"));
		definitions.put("resourcegroupstaggingapi", ServiceDefinition.regional("tagging", "tagging"));
		definitions.put("s3", ServiceDefinition.regional("s3", "s3"));
		definitions.put("s3control", ServiceDefinition.regional("s3-control", "s3"));
		definitions.put("secretsmanager", ServiceDefinition.regional("secretsmanager", "secretsmanager"));
		definitions.put("securityhub", ServiceDefinition.regional("securityhub", "securityhub"));
		definitions.put("servicecatalog", ServiceDefinition.regional("servicecatalog", "servicecatalog"));
		definitions.put("servicediscovery", ServiceDefinition.regional("servicediscovery", "servicediscovery"));
		definitions.put("sesv2", ServiceDefinition.regional("email", "ses"));
		definitions.put("shield", ServiceDefinition.global("shield", "shield"));
		definitions.put("sns", ServiceDefinition.regional("sns", "sns"));
		definitions.put("sqs", ServiceDefinition.regional("sqs", "sqs"));
		definitions.put("ssoadmin", ServiceDefinition.regional("sso-admin", "sso"));
		definitions.put("ssm", ServiceDefinition.regional("ssm", "ssm"));
		definitions.put("states", ServiceDefinition.regional("states", "states"));
		definitions.put("storagegateway", ServiceDefinition.regional("storagegateway", "storagegateway"));
		definitions.put("sts", ServiceDefinition.regional("sts", "sts"));
		definitions.put("timestream", ServiceDefinition.regional("ingest.timestream", "timestream"));
		definitions.put("wafv2", ServiceDefinition.regional("wafv2", "wafv2"));
		definitions.put("workspaces", ServiceDefinition.regional("workspaces", "workspaces"));
		definitions.put("xray", ServiceDefinition.regional("xray", "xray"));

		return Collections.unmodifiableMap(definitions);
	}

	private static Map<String, String> createServiceAliases()
	{
		Map<String, String> aliases = new LinkedHashMap<>();

		alias(aliases, "ec2",
			"vpc", "subnet", "routetable", "internetgateway", "natgateway",
			"vpcendpoint", "vpcendpointservice", "networkacl", "securitygroup",
			"transitgateway", "transitgatewayattachment", "vpcpeeringconnection",
			"customergateway", "vpnconnection", "carriergateway", "localgateway",
			"networkinterface", "elasticip", "prefixlist", "dhcpoptions",
			"ipam", "ipampool", "ipamscope", "ec2instance", "ec2launchtemplate",
			"capacityreservation", "spotfleetrequest", "ec2placementgroup", "ec2keypair",
			"ec2image", "ec2host", "ec2snapshot", "ebsvolume", "ebsvolumeattachment",
			"privatelinkservice");

		alias(aliases, "elasticloadbalancing",
			"classicloadbalancer", "applicationloadbalancer", "networkloadbalancer",
			"gatewayloadbalancer", "loadbalancerlistener", "targetgroup", "targethealth");
		alias(aliases, "globalaccelerator", "globalaccelerator");
		alias(aliases, "appmesh", "appmeshmesh", "appmeshvirtualservice", "appmeshvirtualnode");
		alias(aliases, "apigateway", "vpclink");
		alias(aliases, "autoscaling", "autoscalinggroup", "autoscalingpolicy", "ec2launchconfiguration");

		alias(aliases, "ecs",
			"ecscluster", "ecsservice", "ecstaskdefinition", "ecstask", "ecscapacityprovider",
			"ecscontainerinstance");
		alias(aliases, "servicediscovery", "ecsservicediscovery");
		alias(aliases, "eks", "ekscluster", "eksnodegroup", "eksaddon", "eksfargateprofile");
		alias(aliases, "lambda",
			"lambdafunction", "lambdaalias", "lambdaversion", "lambdalayer",
			"lambdaeventsourcemapping", "lambdapermission");

		alias(aliases, "s3",
			"s3bucket", "s3bucketpolicy", "s3bucketacl", "s3bucketlifecycle",
			"s3bucketreplication", "s3bucketlogging", "s3bucketnotification",
			"s3bucketwebsite", "s3objectlockconfiguration");
		alias(aliases, "s3control", "s3multiregionaccesspoint");
		alias(aliases, "elasticfilesystem", "efsfilesystem", "efsmounttarget", "efsaccesspoint");
		alias(aliases, "fsx", "fsxfilesystem", "fsxbackup");
		alias(aliases, "storagegateway", "storagegatewayvolume");
		alias(aliases, "cloudfront",
			"cloudfrontdistribution", "cloudfrontoriginaccessidentity", "cloudfrontfunction",
			"cloudfrontcachepolicy", "cloudfrontoriginrequestpolicy", "cloudfrontresponseheaderspolicy");

		alias(aliases, "rds",
			"rdsinstance", "rdscluster", "rdssnapshot", "rdsclustersnapshot", "rdssubnetgroup",
			"rdsparametergroup", "rdsoptiongroup", "rdsproxy", "rdseventsubscription",
			"rdsglobalcluster");
		alias(aliases, "dynamodb",
			"dynamodbtable", "dynamodbglobaltable", "dynamodbbackup", "dynamodbstream",
			"dynamodbindex");
		alias(aliases, "es", "opensearchdomain", "opensearchvpcendpoint");
		alias(aliases, "redshift",
			"redshiftcluster", "redshiftsnapshot", "redshiftsubnetgroup", "redshiftparametergroup");
		alias(aliases, "elasticache",
			"elasticachecluster", "elasticachereplicationgroup", "elasticachesubnetgroup",
			"elasticacheparametergroup");
		alias(aliases, "neptune", "neptunecluster", "neptuneinstance", "neptunesubnetgroup");
		alias(aliases, "timestream", "timestreamdatabase", "timestreamtable");
		alias(aliases, "kafka", "kafkamskcluster");

		alias(aliases, "kinesis", "kinesisstream");
		alias(aliases, "firehose", "kinesisfirehose");
		alias(aliases, "kinesisanalytics", "kinesisanalyticsapp");
		alias(aliases, "sqs", "sqsqueue", "sqsqueuepolicy");
		alias(aliases, "sns", "snstopic", "snssubscription");
		alias(aliases, "events", "eventbridgebus", "eventbridgerule");
		alias(aliases, "states", "stepfunctionsstatemachine", "stepfunctionsexecution");
		alias(aliases, "appconfig",
			"appconfigapplication", "appconfigenvironment", "appconfigconfigurationprofile");

		alias(aliases, "iam",
			"iamuser", "iamgroup", "iamrole", "iampolicy", "iaminstanceprofile",
			"iamaccesskey", "iamservercertificate", "iamsamlprovider",
			"iamopenidconnectprovider", "iamaccountpasswordpolicy");
		alias(aliases, "organizations",
			"organizationsaccount", "organizationsorganization", "organizationsorganizationalunit",
			"organizationspolicy", "organizationspolicyattachment");
		alias(aliases, "ssoadmin", "ssopermissionset", "ssoassignment");
		alias(aliases, "identitystore", "identitystoregroup", "identitystoreuser");
		alias(aliases, "kms", "kmskey", "kmsalias");
		alias(aliases, "secretsmanager", "secretsmanagersecret", "secretsmanagerrotation");
		alias(aliases, "ssm",
			"ssmparameter", "ssmparameterpolicy", "ssmmanagedinstance", "ssmassociation", "ssmdocument");
		alias(aliases, "wafv2", "wafwebacl", "wafrulegroup", "wafipset");
		alias(aliases, "shield", "shieldprotection");
		alias(aliases, "macie2", "maciefinding");
		alias(aliases, "guardduty", "guarddutydetector", "guarddutyfinding");
		alias(aliases, "securityhub", "securityhubhub", "securityhubfinding");
		alias(aliases, "config", "configrule", "configrecorder");

		alias(aliases, "logs", "cloudwatchloggroup", "cloudwatchlogstream");
		alias(aliases, "monitoring", "cloudwatchmetricalarm", "cloudwatchcompositealarm", "cloudwatchdashboard");
		alias(aliases, "cloudtrail", "cloudtrailtrail", "cloudtraileventdatastore");
		alias(aliases, "xray", "xraygroup", "xraysamplingrule", "xrayencryptionconfig");

		alias(aliases, "cloudformation",
			"cloudformationstack", "cloudformationstackset", "cloudformationchangeset");
		alias(aliases, "servicecatalog",
			"servicecatalogportfolio", "servicecatalogproduct", "servicecatalogprovisionedproduct");
		alias(aliases, "backup", "backupvault", "backupplan", "backupselection", "backuprecoverypoint");
		alias(aliases, "licensemanager", "licensemanagerlicenseconfiguration");
		alias(aliases, "resourcegroups", "resourcegroupsgroup");
		alias(aliases, "resourcegroupstaggingapi", "tageditorresource");

		alias(aliases, "codecommit", "codecommitrepository");
		alias(aliases, "codebuild", "codebuildproject");
		alias(aliases, "codepipeline", "codepipelinepipeline");
		alias(aliases, "codedeploy", "codedeployapplication", "codedeploydeploymentgroup");
		alias(aliases, "codeartifact", "codeartifactrepository");
		alias(aliases, "codeguruprofiler", "codeguruprofilergroup");
		alias(aliases, "codegurureviewer", "codegurureviewerrepositoryassociation");
		alias(aliases, "cloud9", "cloud9environment");

		alias(aliases, "glue",
			"gluedatabase", "gluetable", "gluecrawler", "gluejob", "gluedatacatalogresourcepolicy");
		alias(aliases, "lakeformation", "lakeformationdatalakesettings", "lakeformationresource");
		alias(aliases, "athena", "athenaworkgroup", "athenanamedquery");
		alias(aliases, "elasticmapreduce", "emrcluster", "emrinstancegroup");
		alias(aliases, "datapipeline", "datapipelinepipeline");

		alias(aliases, "sesv2", "sesconfigurationset", "sesidentity", "sesreceiptruleset", "sestemplate");
		alias(aliases, "account", "accountalternatecontact", "accountcontactinformation");
		alias(aliases, "budgets", "budgetsbudget");
		alias(aliases, "ce", "costcategory");
		alias(aliases, "workspaces", "workspacesworkspace", "workspacesdirectory", "workspacesapplication");
		alias(aliases, "appstream", "appstreamfleet", "appstreamstack");

		for (String key : new ArrayList<>(aliases.keySet()))
		{
			aliases.put(key + "service", aliases.get(key));
		}

		return Collections.unmodifiableMap(aliases);
	}

	private static void alias(Map<String, String> aliases, String serviceId, String... keys)
	{
		for (String key : keys)
		{
			aliases.put(normalize(key), serviceId);
		}
		aliases.put(normalize(serviceId), serviceId);
	}


}

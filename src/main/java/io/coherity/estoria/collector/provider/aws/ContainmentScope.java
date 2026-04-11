package io.coherity.estoria.collector.provider.aws;

// AWS
//└── Organization
//      └── Account
//            └── Region
//                  └── VPC
public enum ContainmentScope
{
	AWS_GLOBAL(1),      // AWS-owned, not yours
	ORGANIZATION(2),    // spans org (Organizations, SCPs)
	ACCOUNT_GLOBAL(3),  // in your account, not regional (IAM)
	ACCOUNT_REGIONAL(4),// in your account + region (ECR, SSM, EC2)
	VPC(5);             // inside a VPC (subnets, ENIs, ALB)

	private final int level;

	ContainmentScope(int level)
	{
		this.level = level;
	}

	public int getLevel()
	{
		return this.level;
	}

	/**
	 * Returns true if this requested scope includes the given collector scope.
	 * A broader (lower-level) requested scope includes all narrower (higher-level) collector scopes.
	 */
	public boolean includes(ContainmentScope collectorScope)
	{
		return collectorScope.level >= this.level;
	}
}
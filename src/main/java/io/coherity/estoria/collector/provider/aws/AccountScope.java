package io.coherity.estoria.collector.provider.aws;

public enum AccountScope
{
    MEMBER_ACCOUNT(1),
    MANAGEMENT_ACCOUNT(2);

    private final int level;

    AccountScope(int level)
    {
        this.level = level;
    }

    public int getLevel()
    {
        return this.level;
    }

    public boolean satisfies(AccountScope required)
    {
        return this.level >= required.level;
    }
}
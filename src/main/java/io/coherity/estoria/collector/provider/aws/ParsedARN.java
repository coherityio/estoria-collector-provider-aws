package io.coherity.estoria.collector.provider.aws;

import io.coherity.estoria.collector.provider.aws.ARNHelper.ResourceSeparator;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;


@Data
@Builder
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@NoArgsConstructor
@AllArgsConstructor
public class ParsedARN
{
    private String rawArn;
    private String partition;
    private String service;
    private String region;
    private String accountId;
    private String resource;
    private String resourceType;
    private String resourceId;
    private ResourceSeparator resourceSeparator;

}
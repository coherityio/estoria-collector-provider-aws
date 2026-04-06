package io.coherity.estoria.collector.provider.aws;

import io.coherity.estoria.collector.provider.aws.AwsHttpLoggingInterceptor;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

/**
 * Execution interceptor that logs low-level AWS HTTP requests and responses.
 *
 * This is intended for debugging and is wired into AWS SDK v2 clients
 * created by {@link AwsClientFactory}. Logs are emitted at DEBUG level.
 */
@Slf4j
class AwsHttpLoggingInterceptor implements ExecutionInterceptor
{
	@Override
	public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes executionAttributes)
	{
		if (!log.isDebugEnabled())
		{
			return;
		}

		SdkHttpRequest request = context.httpRequest();
		if (request == null)
		{
			return;
		}

		log.debug(
			"AWS HTTP request: method={}, uri={}, headers={}",
			request.method(),
			request.getUri(),
			request.headers());
	}

	@Override
	public void afterTransmission(Context.AfterTransmission context, ExecutionAttributes executionAttributes)
	{
		if (!log.isDebugEnabled())
		{
			return;
		}

		SdkHttpResponse response = context.httpResponse();
		if (response == null)
		{
			return;
		}

		log.debug(
			"AWS HTTP response: statusCode={}, statusText={}, headers={}",
			response.statusCode(),
			response.statusText().orElse(null),
			response.headers());
	}
}

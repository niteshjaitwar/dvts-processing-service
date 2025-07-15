package com.adp.esi.digitech.dvts.processing.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfiguration {
	
	@Value("${digitech.datastudio.server.url}")
	String dataStudioUrl;	
	
	@Value("${digitech.webclient.max.inMemory.buffer.size}")
	private int bufferSize;
	
	@Bean(name = "dataStudioWebClient")	
	public WebClient webClient() {
		return WebClient.builder()
				.baseUrl(dataStudioUrl)
				.defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.MULTIPART_FORM_DATA_VALUE)
				.codecs(codecs -> codecs
						.defaultCodecs()
						.maxInMemorySize(1024 * 1024 * bufferSize))
				.build();
	}

}

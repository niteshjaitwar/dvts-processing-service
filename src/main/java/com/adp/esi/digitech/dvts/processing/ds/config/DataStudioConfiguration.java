package com.adp.esi.digitech.dvts.processing.ds.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "digitech.datastudio")
@Data
public class DataStudioConfiguration {
	
	private String lovURI;
	private String complexLovURI;
	private String configurationURI;
	private String columnRelationURI;
	private String validationRuleURI;
	private String validationRuleByTypeURI;
	private String transformationRuleURI;
	private String columnConfigurationURI;
	private String validationRuleByTypeBatchURI;
	private String transformationRuleBatchURI;

}

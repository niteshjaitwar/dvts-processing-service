package com.adp.esi.digitech.dvts.processing.ds.service;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import com.adp.esi.digitech.dvts.processing.ds.config.DataStudioConfiguration;
import com.adp.esi.digitech.dvts.processing.ds.dto.DSResponseDTO;
import com.adp.esi.digitech.dvts.processing.ds.model.ColumnConfiguration;
import com.adp.esi.digitech.dvts.processing.ds.model.ColumnRelation;
import com.adp.esi.digitech.dvts.processing.ds.model.ComplexLovData;
import com.adp.esi.digitech.dvts.processing.ds.model.ConfigurationData;
import com.adp.esi.digitech.dvts.processing.ds.model.TransformationRule;
import com.adp.esi.digitech.dvts.processing.ds.model.ValidationRule;
import com.adp.esi.digitech.dvts.processing.enums.ValidationType;

@Service
public class DatastudioConfigurationService {
	
	@Autowired
	@Qualifier("dataStudioWebClient")	
	private WebClient webClient;
	
	private static final String BU = "bu";
	private static final String PLATFORM = "platform";
	private static final String DATA_CATEGORY = "dataCategory";
	private static final String RULE_TYPE = "ruleType";
	private static final String LOV_TYPE = "type";
	
	
	@Autowired
	DataStudioConfiguration dataStudioConfiguration;
	
	public Properties findAllProperties(String type) {	
		
		var response = webClient.post()
				 .uri(dataStudioConfiguration.getLovURI())
				 .body(BodyInserters
						 .fromFormData(LOV_TYPE, type))
				 .retrieve()
				 .bodyToMono(new ParameterizedTypeReference<DSResponseDTO<Properties>>() {})
				 .block();
		return response.getData();
	}
	
	public ComplexLovData findAllComplexProperties(String type) {
		
		var response=webClient.post()
				.uri(dataStudioConfiguration.getComplexLovURI())
				.body(BodyInserters.fromFormData(LOV_TYPE, type))
				.retrieve().bodyToMono(new ParameterizedTypeReference<DSResponseDTO<ComplexLovData>>() {})
				.block();
		return response.getData();
	}
	/*
	public ConfigurationData findConfigurationDataBy(String bu, String platform, String dataCategory) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getConfigurationURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<ConfigurationData>>() {})
				.block();
		return response.getData();
	}
	
	public Map<String,List<ColumnRelation>> findAllColumnRelationsMapBy(String bu, String platform, String dataCategory) {
		return findAllColumnRelationsBy(bu, platform, dataCategory).stream().collect(Collectors.groupingBy(ColumnRelation::getSourceKey));
	}
	
	public List<ColumnRelation> findAllColumnRelationsBy(String bu, String platform, String dataCategory) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getColumnRelationURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<List<ColumnRelation>>>() {})
				.block();
		return response.getData();
	}
	
	public Map<String,List<ValidationRule>> findAllValidationRulesGroupBy(String bu, String platform, String dataCategory) {
		return findAllValidationRulesBy(bu, platform, dataCategory).stream().collect(Collectors.groupingBy(ValidationRule::getValidationRuleType));
	}
	
	public List<ValidationRule> findAllValidationRulesBy(String bu, String platform, String dataCategory) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getValidationRuleURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<List<ValidationRule>>>() {})
				.block();
		return response.getData();
	}
	public ColumnConfiguration findProcessConfigurationBy(String bu, String platform, String dataCategory) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getColumnConfigurationURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<ColumnConfiguration>>() {})
				.block();
		return response.getData();
	}
	*/
	public Map<UUID,ValidationRule> findAllValidationRulesMapByRuleType(String bu, String platform, String dataCategory, ValidationType ruleType) {
		return findAllValidationRulesByRuleType(bu, platform, dataCategory, ruleType).stream().collect(Collectors.toMap(item -> UUID.fromString(item.getSourceColumn()) , Function.identity()));
	}
	
	public List<ValidationRule> findAllValidationRulesByRuleType(String bu, String platform, String dataCategory, ValidationType ruleType) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getValidationRuleByTypeURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory)
						.with(RULE_TYPE, ruleType.getValidationType()))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<List<ValidationRule>>>() {})
				.block();
		return response.getData();
	}
	
	
	public Map<UUID,TransformationRule> findAllTransformationRulesMapBy(String bu, String platform, String dataCategory){
		return findAllTransformationRulesBy(bu, platform, dataCategory).stream().collect(Collectors.toMap(item -> UUID.fromString(item.getSourceColumnName()) , Function.identity()));
	}
	
	public List<TransformationRule> findAllTransformationRulesBy(String bu, String platform, String dataCategory) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getTransformationRuleURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<List<TransformationRule>>>() {})
				.block();
		return response.getData();
	}
	
	public Map<UUID,ValidationRule> findAllValidationRulesMapByRuleTypeBatch(String bu, String platform, String dataCategory, ValidationType ruleType) {
		return findAllValidationRulesByRuleTypeBatch(bu, platform, dataCategory, ruleType).stream().collect(Collectors.toMap(item -> UUID.fromString(item.getSourceColumn()) , Function.identity()));
	}
	
	public List<ValidationRule> findAllValidationRulesByRuleTypeBatch(String bu, String platform, String dataCategory, ValidationType ruleType) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getValidationRuleByTypeBatchURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory)
						.with(RULE_TYPE, ruleType.getValidationType()))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<List<ValidationRule>>>() {})
				.block();
		return response.getData();
	}
	
	
	public Map<UUID,TransformationRule> findAllTransformationRulesMapByBatch(String bu, String platform, String dataCategory){
		return findAllTransformationRulesByBatch(bu, platform, dataCategory).stream().collect(Collectors.toMap(item -> UUID.fromString(item.getSourceColumnName()) , Function.identity()));
	}
	
	public List<TransformationRule> findAllTransformationRulesByBatch(String bu, String platform, String dataCategory) {
		var response = webClient.post()
				.uri(dataStudioConfiguration.getTransformationRuleBatchURI())
				.body(BodyInserters
						.fromFormData(BU, bu)
						.with(PLATFORM, platform)
						.with(DATA_CATEGORY, dataCategory))
				.retrieve()
				.bodyToMono(new ParameterizedTypeReference<DSResponseDTO<List<TransformationRule>>>() {})
				.block();
		return response.getData();
	}
	
	
}

package com.adp.esi.digitech.dvts.processing.service;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import com.adp.esi.digitech.dvts.processing.autowire.service.CustomFilterDynamicAutowireService;
import com.adp.esi.digitech.dvts.processing.ds.config.model.DataSetRules;
import com.adp.esi.digitech.dvts.processing.ds.model.ComplexLovData;
import com.adp.esi.digitech.dvts.processing.ds.model.TransformationRule;
import com.adp.esi.digitech.dvts.processing.ds.model.ValidationRule;
import com.adp.esi.digitech.dvts.processing.ds.service.DatastudioConfigurationService;
import com.adp.esi.digitech.dvts.processing.enums.ProcessType;
import com.adp.esi.digitech.dvts.processing.enums.ValidationType;
import com.adp.esi.digitech.dvts.processing.exception.ProcessException;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.filter.service.DataFilterService;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.DataMap;
import com.adp.esi.digitech.dvts.processing.model.DataSet;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.parser.DateParser;
import com.adp.esi.digitech.dvts.processing.parser.DecimalParser;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractDataProcessingService implements IDataProcessingService {
	
	public DatastudioConfigurationService datastudioConfigurationService;
	
	public ObjectMapper objectMapper;
	
	public RequestContext requestContext;	
	
	public CustomFilterDynamicAutowireService customFilterDynamicAutowireService;
	
	public static final String DROP_DOWN_DATA_TYPE = "Dropdown";
	
	@Value("${large.lov.file.path}")
	String lovFolderPath;
	
	@Autowired
	protected void setDatastudioConfigurationService(DatastudioConfigurationService datastudioConfigurationService) {
		this.datastudioConfigurationService = datastudioConfigurationService;
	}

	@Autowired
	protected void setObjectMapper(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Autowired
	protected void setCustomFilterDynamicAutowireService(
			CustomFilterDynamicAutowireService customFilterDynamicAutowireService) {
		this.customFilterDynamicAutowireService = customFilterDynamicAutowireService;
	}
	
	@Override
	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;		
	}

	public List<String> getLovTypeNamesFromValidationRule(Map<UUID, ValidationRule> validationRulesMap) {
		return validationRulesMap.values().stream()
				.filter(item -> DROP_DOWN_DATA_TYPE.equalsIgnoreCase(item.getDataType()) 
								&& ValidationUtil.isHavingValue(item.getLovCheckType())
								&& !ValidationUtil.isHavingValue(item.getDependsOn()))
				.map(item -> item.getLovCheckType())
				.distinct()
				.collect(Collectors.toList());				   
	}
	
	public List<String> getComplexLovTypeNamesFromValidationRule(Map<UUID, ValidationRule> validationRuleMap) {
		return validationRuleMap.values().stream()
				.filter(item -> DROP_DOWN_DATA_TYPE.equalsIgnoreCase(item.getDataType()) 
								&& ValidationUtil.isHavingValue(item.getLovCheckType())
								&& ValidationUtil.isHavingValue(item.getDependsOn()))
				.map(item -> item.getLovCheckType())
				.distinct()
				.collect(Collectors.toList());
	} 
	
	public List<String> getLovTypeNamesFromTransformationRule(Map<UUID, TransformationRule> transformationRulesMap) {
		return transformationRulesMap.values().stream()
				.filter(item -> DROP_DOWN_DATA_TYPE.equalsIgnoreCase(item.getDataType()) 
						&& ValidationUtil.isHavingValue(item.getLovCheckType())
						&& !ValidationUtil.isHavingValue(item.getDependsOn()))
				.map(item -> item.getLovCheckType())
				.distinct()
				.collect(Collectors.toList());				   
	}
	
	public List<String> getComplexLovTypeNamesFromTransformationRule(Map<UUID, TransformationRule> transformationRuleMap){
		return transformationRuleMap.values().stream()
				.filter(item -> DROP_DOWN_DATA_TYPE.equalsIgnoreCase(item.getDataType()) 
						&& ValidationUtil.isHavingValue(item.getLovCheckType())
						&& ValidationUtil.isHavingValue(item.getDependsOn()))
				.map(item -> item.getDependsOn())
				.distinct()
				.collect(Collectors.toList());
	}
	
	public Map<String, Properties> loadLovTypes(List<String> lovTypes) {
		return lovTypes.stream().collect(Collectors.toMap(item -> item, item -> loadLovType(item)));
	}
	
	private Properties loadLovType(String lovType) {		
		var file = new File(lovFolderPath + lovType +".properties");		
		if(file.exists())
			try(FileReader reader = new FileReader(file,StandardCharsets.UTF_16)) {
				//log.info("AbstractDataProcessingService - loadLovByType(), loading lov form file, lovType = {}", lovType);			
				Properties props = new Properties();
				props.load(reader);			
				return props;
			} catch (IOException e) {
				log.error("AbstractDataProcessingService - loadLovByType(), Failed to load lov form file, lovType = {}, Error Message = {}", lovType, e.getMessage());
			}	
		return datastudioConfigurationService.findAllProperties(lovType);
	}
	
	public Map<String, ComplexLovData> loadComplexLovTypes(List<String> complexLovTypes) {
		return complexLovTypes.stream().collect(Collectors.toMap(item -> item, item -> loadComplexLovType(item)));
	}

	private ComplexLovData loadComplexLovType(String complexLovType) {

		var complexlovData = datastudioConfigurationService.findAllComplexProperties(complexLovType);
		
		if(complexlovData != null && complexlovData.getLovDataJson()!=null) {
			try {
				List<Map<String, String>> lovDatamap=objectMapper.readValue(complexlovData.getLovDataJson(), new TypeReference<List<Map<String, String>>>() {});
				complexlovData.setLovDataMap(lovDatamap);			
			}catch(Exception e) {
				throw new RuntimeException("Failed to parse lovDataJson for type: {}"+ complexLovType, e);
			}
		}		
		return complexlovData;
	}
	
	public Map<UUID, TransformationRule> findDataParseRulesByTransformationRules(Map<UUID, TransformationRule> transformationRules) {
		return transformationRules.entrySet().stream().filter(entry -> Objects.nonNull(entry.getValue()) && ValidationUtil.isHavingValue(entry.getValue().getDataType())).filter(entry -> {
			var rule = entry.getValue();
			return rule.getDataType().equals("Date") || rule.getDataType().equals("Number");
		}).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
	}
	
	public Map<UUID, ValidationRule> findDataParseRulesByValidationRules(Map<UUID, ValidationRule> validationRules) {
		return validationRules.entrySet().stream().filter(entry -> {
			var rule = entry.getValue();
			return rule.getDataType().equals("Date") || rule.getDataType().equals("Number");
		}).collect(Collectors.toMap(Entry::getKey, Entry::getValue));
	}
	
	public List<Row> parseDataByTransformationRules(List<Row> rows, Map<UUID, TransformationRule> transformationRules) {
		
		var dataPasrseRules = findDataParseRulesByTransformationRules(transformationRules);
		
		if (Objects.isNull(dataPasrseRules) || dataPasrseRules.isEmpty()) {
			log.info("AbstractDataProcessingService - parseDataByTransformationRules(), No Rules Found to parse Date/Number fields");
			return rows;
		}
		
		log.info("AbstractDataProcessingService - process(), Found Date/Number Rules: {}", dataPasrseRules.size());
		
		return rows.parallelStream().map(row -> {
			dataPasrseRules.keySet().parallelStream().forEach(key -> {
				if (dataPasrseRules.containsKey(key)) {
					var rule = dataPasrseRules.get(key);
					var column = row.getColumns().get(key);
					if(Objects.nonNull(column) && ValidationUtil.isHavingValue(column.getSourceValue())) {
						try {
							if (rule.getDataType().equals("Date") && ValidationUtil.isHavingValue(rule.getDataFormat())) {
								var date = DateParser.parse(column.getSourceValue(), rule.getDataFormat());
								column.setValue(date);
								column.setTransformedValue(date);
							} else if (rule.getDataType().equals("Number") && ValidationUtil.isHavingValue(rule.getDataFormat()) && ValidationUtil.isValidJson(rule.getDataFormat())) {
								JSONObject numberFormatJson = new JSONObject(rule.getDataFormat());				
								var format = numberFormatJson.has("format") && !numberFormatJson.isNull("format") ? numberFormatJson.getString("format"):null;
								
								var value = DecimalParser.parse(column.getSourceValue(), format);
								column.setValue(value.toString());
								column.setTransformedValue(value.toString());
								column.setTargetValue(value.toString());
							}
						} catch (ParseException e) {
							log.error("DataTransformationProcessingService - process(), Error in parsing data, Column: {}, Error: {}", key, e.getMessage());
						}
					}
				}
			});		
			return row;
		}).collect(Collectors.toList());
	}
	
	public List<Row> parseDataByValidationRules(List<Row> rows, Map<UUID, ValidationRule> validationRules) {
		
		var dataPasrseRules = findDataParseRulesByValidationRules(validationRules);
		
		if (Objects.isNull(dataPasrseRules) || dataPasrseRules.isEmpty()) {
			log.info("AbstractDataProcessingService - parseDataByValidationRules(), No Rules Found to parse Date/Number fields");
			return rows;
		}
		
		log.info("AbstractDataProcessingService - process(), Found Date/Number Rules: {}", dataPasrseRules.size());
		
		return rows.parallelStream().map(row -> {
			dataPasrseRules.keySet().parallelStream().forEach(key -> {
				if (dataPasrseRules.containsKey(key)) {
					var rule = dataPasrseRules.get(key);
					var column = row.getColumns().get(key);
					if(Objects.nonNull(column) && ValidationUtil.isHavingValue(column.getSourceValue())) {
						try {
							if (rule.getDataType().equals("Date") && ValidationUtil.isHavingValue(rule.getDataFormat())) {
								var date = DateParser.parse(column.getSourceValue(), rule.getDataFormat());
								column.setValue(date);
								column.setTransformedValue(date);
							} else if (rule.getDataType().equals("Number") && ValidationUtil.isHavingValue(rule.getDataFormat())  && ValidationUtil.isValidJson(rule.getDataFormat())) {
								JSONObject numberFormatJson = new JSONObject(rule.getDataFormat());				
								var format = numberFormatJson.has("format") && !numberFormatJson.isNull("format") ? numberFormatJson.getString("format"):null;
								var value = DecimalParser.parse(column.getSourceValue(), format);
								column.setValue(value.toString());
								column.setTransformedValue(value.toString());
								column.setTargetValue(value.toString());
							}
						} catch (ParseException e) {
							log.error("DataTransformationProcessingService - process(), Error in parsing data, Column: {}, Error: {}", key, e.getMessage());
						}
					}
				}
			});		
			return row;
		}).collect(Collectors.toList());
	}
	
	public Map<UUID, ValidationRule> getValidationRules(String bu, String platform, String dataCategory, ValidationType validationType) {
		return datastudioConfigurationService.findAllValidationRulesMapByRuleType(bu, platform, dataCategory, validationType);
	}
	
	
	private <T> List<T> getRules(String json , TypeReference<List<T>> type) throws ProcessException {
		try {
			return objectMapper.readValue(json, type);
		} catch (JsonProcessingException e) {
			log.error("AbstractProcessorService - getRules(), Failed to process rules, rule = {}, uniqueId = {}, Error Message = {}", type.getClass().getName(), requestContext.getRequestUuid(), e.getMessage());
			var processException = new ProcessException("Configuration Error - Failed to process " + type.getClass().getName() + " Rules", e.getCause());
			throw processException;
		}		
	}
	
	public  List<DataSetRules> getDataSetRules(String json) {
		if (!ValidationUtil.isHavingValue(json))
			return null;
		return getRules(json, new TypeReference<List<DataSetRules>>() {});
	}
	
	public void applyDataRule(DataSet dataSet, DataSetRules dataSetRule, ProcessType processType, Map<String, String> dynamicClauseValues) throws TransformationException {
		
		if(Objects.isNull(dataSetRule)) {
			log.info("AbstractDataProcessingService -> applyDataRules() No Data Rules found, uniqueId = {}", requestContext.getUniqueId());
			return ;
		}
		
		log.info("AbstractDataProcessingService -> applyDataRules() Started applying data rules, uniqueId = {}, dataSet Name = {}", requestContext.getUniqueId(),dataSet.getName());
		
		var sourceData = dataSet.getData();
		sourceData = customFilterDynamicAutowireService.filter(DataFilterService.class, sourceData, dataSetRule, requestContext, processType, dynamicClauseValues);
		dataSet.setData(sourceData);
		
	}
	
	public List<Row> getRows(List<DataMap> dataMapRows) {
		return dataMapRows.parallelStream().map(dataMapRow -> {
			var columns = dataMapRow.getColumns().entrySet().parallelStream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
				var value = entry.getValue();
				var column = new Column(null, value, entry.getKey(), null);
				column.setTargetValue(value);
				column.setSourceValue(value);
				column.setTransformedValue(value);
				return column;
			}));
			
			return new Row(columns);
		}).collect(Collectors.toList());
	}

}

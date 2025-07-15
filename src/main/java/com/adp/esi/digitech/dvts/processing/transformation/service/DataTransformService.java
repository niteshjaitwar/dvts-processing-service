package com.adp.esi.digitech.dvts.processing.transformation.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.SerializationUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.autowire.service.CustomTransformUtilDynamicAutowireService;
import com.adp.esi.digitech.dvts.processing.ds.model.ComplexLovData;
import com.adp.esi.digitech.dvts.processing.ds.model.TransformationRule;
import com.adp.esi.digitech.dvts.processing.enums.TransformType;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.DataSet;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.transformation.util.ArithmeticTransformUtil;
import com.adp.esi.digitech.dvts.processing.transformation.util.CaseConvertionTransformUtil;
import com.adp.esi.digitech.dvts.processing.transformation.util.ConditionalDefaultTransformUtil;
import com.adp.esi.digitech.dvts.processing.transformation.util.DateArithmeticTransformUtil;
import com.adp.esi.digitech.dvts.processing.transformation.util.DateTransformUtil;
import com.adp.esi.digitech.dvts.processing.transformation.util.DecimalTransformUtil;
import com.adp.esi.digitech.dvts.processing.transformation.util.LengthFormatTransformUtil;
import com.adp.esi.digitech.dvts.processing.transformation.util.StringTransformUtil;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service("dataTransformService")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DataTransformService extends AbstractTransformService {

	@Autowired
	CustomTransformUtilDynamicAutowireService customTransformUtilDynamicAutowireService;
	
	private Map<UUID, TransformationRule> transformationRules;	
	
	@Autowired
	ObjectMapper objectMapper;

	public DataTransformService(RequestContext requestContext, Map<UUID, TransformationRule> transformationRules, Map<String, Properties> lovMetadataMap, Map<String, ComplexLovData> complexLovMetadatMap) {
		super(requestContext, lovMetadataMap, complexLovMetadatMap);
		this.transformationRules = transformationRules;		
	}

	@Override
	public List<Row> transform(DataSet dataSet) throws TransformationException {
		try {
			//log.info("DataTransformService - transform()  Started transforming dataSet, UniqueId {}, name = {}", requestContext.getUniqueId(), dataSet.getName());
		
			List<Row> sourceData = dataSet.getData();
			if(Objects.isNull(sourceData) || sourceData.isEmpty()) {
				log.info("DataTransformService - transform()  No Data found in dataSet to transform, UniqueId {}, name = {}", requestContext.getUniqueId(), dataSet.getName());
				return null;
			}
			
			UUID globalDataSetUuid = UUID.fromString(dataSet.getId());			
			
			var dataSetDataTransformationRules = new HashMap<UUID, JSONObject>();
			var dataSetdefaultValueRules = new HashMap<UUID, JSONObject>();
			var dataSetOrderedDefaultValueRules = new HashMap<Integer, List<UUID>>();
			var newColumnMap = new HashMap<UUID, Column>();
			//var targetFormatMap = new HashMap<UUID, TargetDataFormat>();
			
			/*
			validationRules.forEach((key, validationRule) -> {
				JSONObject rules = null;
				JSONObject specialCharactersToBeRemovedRuleJson = null;
				
				if(ValidationUtil.isHavingValue(validationRule.getDataTransformationRules()))
					rules = ValidationUtil.getDatasetRules(validationRule.getDataTransformationRules(), globalDataSetUuid);				
				
				if(ValidationUtil.isHavingValue(validationRule.getSpecialCharToBeRemoved()))
					specialCharactersToBeRemovedRuleJson = ValidationUtil.getDatasetRules(validationRule.getSpecialCharToBeRemoved(), globalDataSetUuid);
				
				if(Objects.nonNull(rules) && Objects.nonNull(specialCharactersToBeRemovedRuleJson)) 
					dataSetDataTransformationRules.put(key, mergeJSONObjects(rules, specialCharactersToBeRemovedRuleJson));
				else if(Objects.nonNull(rules))
					dataSetDataTransformationRules.put(key, rules);
				else if(Objects.nonNull(specialCharactersToBeRemovedRuleJson))
					dataSetDataTransformationRules.put(key, specialCharactersToBeRemovedRuleJson);
			});
			*/
			
			
			transformationRules.forEach((key, transformationRule) -> {
				
				JSONObject rules = null;
				JSONObject specialCharactersToBeRemovedRuleJson = null;
				
				if(ValidationUtil.isHavingValue(transformationRule.getDataTransformationRules()))
					rules = ValidationUtil.getDatasetRules(transformationRule.getDataTransformationRules(), globalDataSetUuid);				
				
				if(ValidationUtil.isHavingValue(transformationRule.getSpecialCharToBeRemoved()))
					specialCharactersToBeRemovedRuleJson = ValidationUtil.getDatasetRules(transformationRule.getSpecialCharToBeRemoved(), globalDataSetUuid);
				
				if(Objects.nonNull(rules) && Objects.nonNull(specialCharactersToBeRemovedRuleJson)) 
					dataSetDataTransformationRules.put(key, mergeJSONObjects(rules, specialCharactersToBeRemovedRuleJson));
				else if(Objects.nonNull(rules))
					dataSetDataTransformationRules.put(key, rules);
				else if(Objects.nonNull(specialCharactersToBeRemovedRuleJson))
					dataSetDataTransformationRules.put(key, specialCharactersToBeRemovedRuleJson);
				
				
				//!validationRules.containsKey(key) && 
				if(transformationRule.getTargetFileName().equalsIgnoreCase(dataSet.getId())) {
					var column = columnObjProvider.getObject(transformationRule.getTargetColumnName(), null, key, null);
					newColumnMap.put(key, column);
				
					JSONObject rules1 = null;
					if(ValidationUtil.isHavingValue(transformationRule.getDefaultValue())) 
						rules1 = new JSONObject(transformationRule.getDefaultValue());
						
					
					if(Objects.nonNull(rules1)) {
						dataSetdefaultValueRules.put(key, rules1);
						
						var seq = Objects.nonNull(transformationRule.getColumnSequence()) ? transformationRule.getColumnSequence() : 0;
						
						dataSetOrderedDefaultValueRules.computeIfAbsent(seq, k -> new ArrayList<UUID>()).add(key);
						
					}
				}
				
			});
			
			
			var orderedDefaultValueRules = dataSetOrderedDefaultValueRules.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (oldValue, newValue) -> oldValue, LinkedHashMap::new));
			
			
			Row temprow = newColumnMap != null && !newColumnMap.isEmpty() ? new Row(newColumnMap) : null;
			
			
			
			var transformedRows  = sourceData.parallelStream().map(row -> {
					if(temprow != null) {
						var clonedRow = SerializationUtils.clone(temprow);
						row.getColumns().putAll(clonedRow.getColumns());
					}
				
					
					row.getColumns().entrySet().stream().forEach(entity -> {
						var column = entity.getValue();
						
						if(transformationRules.containsKey(entity.getKey())) {
							var transformationRule = transformationRules.get(entity.getKey());						
							var dataType = ValidationUtil.isHavingValue(transformationRule.getDataType()) ? transformationRule.getDataType() : "Text";
							//LOV Transformations
							if(dataType.equalsIgnoreCase("Dropdown")) {
								if(ValidationUtil.isHavingValue(transformationRule.getLovCheckType())
									&& "Y".equalsIgnoreCase(transformationRule.getTransformationRequired())) {
									//log.info("DataTransformService - transform()  getDependsOn{}", transformationRule.getDependsOn());
									if(ValidationUtil.isHavingValue(transformationRule.getDependsOn())) {
										if(Objects.nonNull(complexLovMetadatMap) && complexLovMetadatMap.containsKey(transformationRule.getLovCheckType())) {
											var complexLovData = complexLovMetadatMap.get(transformationRule.getLovCheckType());
											try {
												var dependsOnArray = objectMapper.readValue(transformationRule.getDependsOn(), String[].class);
												var lovDataList = complexLovData.getLovDataMap();
												lovDataList.parallelStream()
														.filter(lovDataMap -> IntStream.range(0, dependsOnArray.length).allMatch(index -> {															
															var uuid = dependsOnArray[index];
															var value = row.getColumns().get(UUID.fromString(uuid)).getSourceValue();
															return lovDataMap.get(String.valueOf(index)).equals(value);
															
														})).findFirst().ifPresent(item -> {
															
															var value = item.get("VALUE");
															//log.info("value = {}", value);
															column.setValue(value);
															column.setTransformedValue(value);											
															column.setTargetValue(value);
														});
											} catch (Exception e) {
												log.error("DataTransformService - transform()  Failed transforming dependsOnArray, UniqueId {}, name = {}, message = {}", requestContext.getUniqueId(), dataSet.getName(), e.getMessage());
												
												var transformationException = new TransformationException("Transformation Error - " + e.getMessage(), e.getCause());
												transformationException.setRequestContext(requestContext);
												throw transformationException;
											} 
										}										
									} else {
										if(Objects.nonNull(column.getValue()) && column.getValue() instanceof String && Objects.nonNull(lovMetadataMap) && lovMetadataMap.containsKey(transformationRule.getLovCheckType())) {
											var lovProperties = lovMetadataMap.get(transformationRule.getLovCheckType());
											var lovNameValue = (String) column.getValue();
											
											if(Objects.nonNull(lovProperties) && lovProperties.containsKey(lovNameValue)) {
												column.setValue(lovProperties.get(lovNameValue));
												column.setTransformedValue(lovProperties.get(lovNameValue));											
												column.setTargetValue(lovProperties.get(lovNameValue).toString());
											}										
										}
									}
								}
							}
							//remove_characters
							if(dataSetDataTransformationRules.containsKey(entity.getKey()) && Objects.nonNull(column.getValue())) {
								var rules = dataSetDataTransformationRules.get(entity.getKey());	
								if(!dataType.equalsIgnoreCase("Date") && rules.has("remove_characters") && !rules.isNull("remove_characters")) {
									String value = column.getValue().toString();		
									String specialCharactersToBeRemoved = rules.getString("remove_characters");
									for(int i=0; i<specialCharactersToBeRemoved.length(); i++){
										value = value.replace(Character.toString(specialCharactersToBeRemoved.charAt(i)), "");
									}
									column.setValue(value);
									column.setTransformedValue(value);
									column.setTargetValue(value);
								}
								
								// Source_Date_Format- In Default value calculations conditional one using date fields as output
								if (dataType.equalsIgnoreCase("Date") && rules.has("date_format") && !rules.isNull("date_format")) {
									//log.info("Started date formatting  for column {} of the request {}", validationRule.getSourceColumn(), requestContext.getUniqueId());
									customTransformUtilDynamicAutowireService.transform(DateTransformUtil.class, row, column, rules, requestContext, TransformType.DATA_TRANSFORMATION_VALUE);
								}
							}
							
								
						}
						
					});
					
					//DEFAULT_VALUE With Column Order
					orderedDefaultValueRules.entrySet().stream().forEach(entry -> {
                        var list = entry.getValue();
                        list.stream().forEach(key -> {
                        	var column = row.getColumns().get(key);                        	
                        	if(dataSetdefaultValueRules.containsKey(key)) {
    							JSONObject rules = dataSetdefaultValueRules.get(key);
    							if (rules.has("currentdate") && !rules.isNull("currentdate")) {    								
    								customTransformUtilDynamicAutowireService.transform(DateTransformUtil.class, row, column, rules, requestContext, TransformType.DEFAULT_VALUE);
    							} else if (rules.has("current_time") && !rules.isNull("current_time")) {    								
    								customTransformUtilDynamicAutowireService.transform(DateTransformUtil.class, row, column, rules, requestContext, TransformType.DEFAULT_VALUE);
    							} else if (rules.has("conditional") && !rules.isNull("conditional")) {    								
    								customTransformUtilDynamicAutowireService.transform(ConditionalDefaultTransformUtil.class, row, column, rules, requestContext, TransformType.DEFAULT_VALUE);
    							} else if (rules.has("arithmetic_operations") && !rules.isNull("arithmetic_operations")) {    								
    								customTransformUtilDynamicAutowireService.transform(ArithmeticTransformUtil.class, row, column, rules, requestContext, TransformType.DEFAULT_VALUE);
    							} else if (rules.has("length_format") && !rules.isNull("length_format")) {    								
    								customTransformUtilDynamicAutowireService.transform(LengthFormatTransformUtil.class, row, column, rules, requestContext, TransformType.DEFAULT_VALUE);
    							} else if (rules.has("Case_Conversion") && !rules.isNull("Case_Conversion")) {    								
    								customTransformUtilDynamicAutowireService.transform(CaseConvertionTransformUtil.class, row, column, rules, requestContext, TransformType.DEFAULT_VALUE);
    							} else if ((rules.has("split_operations") && !rules.isNull("split_operations")) || (rules.has("trim") && !rules.isNull("trim")) 
    								|| (rules.has("defaultValue") && !rules.isNull("defaultValue")) || (rules.has("concatinate") && !rules.isNull("concatinate"))
									|| (rules.has("field_concat") && !rules.isNull("field_concat")) || (rules.has("Field_Value") && !rules.isNull("Field_Value"))) {    								
    								customTransformUtilDynamicAutowireService.transform(StringTransformUtil.class, row, column, rules, requestContext, TransformType.DEFAULT_VALUE);
    							}
    							
    							if (rules.has("target_format") && !rules.isNull("target_format")) {    								
    								customTransformUtilDynamicAutowireService.transform(DecimalTransformUtil.class, row, column, rules, requestContext, TransformType.DEFAULT_VALUE);
    							}
    						}
                        });
                        
                        
					});
					
					//DATA_TRANSFORMATION_RULES
					dataSetDataTransformationRules.entrySet().parallelStream().forEach(entity -> {
						var colUUID = entity.getKey();
						var rules = entity.getValue();
						var column = row.getColumns().get(colUUID);
						var transformationRule = transformationRules.get(entity.getKey());
						var dataType = ValidationUtil.isHavingValue(transformationRule.getDataType()) ? transformationRule.getDataType() : "Text";
						
						if(rules != null) {
							// Date Arithmatic_Operations
							if (dataType.equalsIgnoreCase("Date") && rules.has("arithmetic_operations") && !rules.isNull("arithmetic_operations")) {
								customTransformUtilDynamicAutowireService.transform(DateArithmeticTransformUtil.class, row, column, rules, requestContext, TransformType.DATA_TRANSFORMATION_VALUE);
							}							
							// Arithmetic Operations on number date types
							if (dataType.equalsIgnoreCase("Number") && rules.has("arithmetic_operations") && !rules.isNull("arithmetic_operations")) {
								customTransformUtilDynamicAutowireService.transform(ArithmeticTransformUtil.class, row, column, rules, requestContext, TransformType.DATA_TRANSFORMATION_VALUE);
							}
							//split_operations
							if (rules.has("split_operations") && !rules.isNull("split_operations")) {
								customTransformUtilDynamicAutowireService.transform(StringTransformUtil.class, row, column, rules, requestContext, TransformType.DATA_TRANSFORMATION_VALUE);
							}							
							// Date_Format
							if (rules.has("date_format") && !rules.isNull("date_format")) {
								customTransformUtilDynamicAutowireService.transform(DateTransformUtil.class, row, column, rules, requestContext, TransformType.DATA_TRANSFORMATION_VALUE);
							}
							// Target_format Operations on number date types
							if (dataType.equalsIgnoreCase("Number") && rules.has("target_format") && !rules.isNull("target_format")) {
								customTransformUtilDynamicAutowireService.transform(DecimalTransformUtil.class, row, column, rules, requestContext, TransformType.DATA_TRANSFORMATION_VALUE);
							}
							
							// Length_format
							if (rules.has("length_format") && !rules.isNull("length_format")) {
								customTransformUtilDynamicAutowireService.transform(LengthFormatTransformUtil.class, row, column, rules, requestContext, TransformType.DATA_TRANSFORMATION_VALUE);
							}							
							// Case_Conversion
							if (rules.has("Case_Conversion") && !rules.isNull("Case_Conversion")) {
								customTransformUtilDynamicAutowireService.transform(CaseConvertionTransformUtil.class, row, column, rules, requestContext, TransformType.DATA_TRANSFORMATION_VALUE);
							}
							if (rules.has("trim") && !rules.isNull("trim")) {
								customTransformUtilDynamicAutowireService.transform(StringTransformUtil.class, row, column, rules, requestContext, TransformType.DATA_TRANSFORMATION_VALUE);
							}
						}
					});
					
					
				return row;
			}).collect(Collectors.toList());
			log.info("DataTransformService - transform()  Completed transforming dataSet, UniqueId {}, name = {}", requestContext.getUniqueId(), dataSet.getName());
			
			return transformedRows;
		} catch (TransformationException e) {
			log.error("DataTransformService - transform()  Failed transforming dataSet, UniqueId {}, name = {}, message = {}", requestContext.getUniqueId(), dataSet.getName(), e.getMessage());
			throw e;
		} catch (Exception e) {
			log.error("DataTransformService - transform()  Failed transforming dataSet, UniqueId {}, name = {}, message = {}", requestContext.getUniqueId(), dataSet.getName(), e.getMessage());
			
			var transformationException = new TransformationException("Transformation Error - " + e.getMessage(), e.getCause());
			transformationException.setRequestContext(requestContext);
			throw transformationException;
		}
		
		
	}
	
	private JSONObject mergeJSONObjects(JSONObject json1, JSONObject json2) {
        JSONObject mergedJSON = new JSONObject();
        try {
            // getNames(): Get an array of field names from a JSONObject.
            mergedJSON = new JSONObject(json1, JSONObject.getNames(json1));
            for (String crunchifyKey : JSONObject.getNames(json2)) {
                // get(): Get the value object associated with a key.
                mergedJSON.put(crunchifyKey, json2.get(crunchifyKey));
            }
        } catch (JSONException e) {
            // RunttimeException: Constructs a new runtime exception with the specified detail message.
            // The cause is not initialized, and may subsequently be initialized by a call to initCause.
            throw new RuntimeException("JSON Exception" + e);
        }
        return mergedJSON;
    }
}

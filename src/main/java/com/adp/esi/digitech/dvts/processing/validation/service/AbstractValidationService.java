package com.adp.esi.digitech.dvts.processing.validation.service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;

import com.adp.esi.digitech.dvts.processing.autowire.service.CustomValidationUtilDynamicAutowireService;
import com.adp.esi.digitech.dvts.processing.ds.model.ComplexLovData;
import com.adp.esi.digitech.dvts.processing.ds.model.ValidationRule;
import com.adp.esi.digitech.dvts.processing.exception.DataValidationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;
import com.adp.esi.digitech.dvts.processing.validation.util.ConditionalValidationConstructUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractValidationService<T> implements IValidationService<T> {
	
	
	protected RequestContext requestContext;
	protected Map<String, Properties> lovMetadataMap;	
	protected Map<String, ComplexLovData> complexLovMetadataMap;
	protected List<String> columnsToValidate;
	
	public UUID globalDataSetUuid;	
	
	public AbstractValidationService(RequestContext requestContext, Map<String, Properties> lovMetadataMap, Map<String, ComplexLovData> complexLovMetadataMap, List<String> columnsToValidate) {
		this.requestContext = requestContext;
		this.lovMetadataMap = lovMetadataMap;
		this.complexLovMetadataMap = complexLovMetadataMap;
		this.columnsToValidate = columnsToValidate;
		
	}

	CustomValidationUtilDynamicAutowireService customValidationUtilDynamicAutowireService;	
	ConditionalValidationConstructUtils conditionalValidationConstructUtils;

	
	@Autowired
	public void setCustomValidationUtilDynamicAutowireService(
			CustomValidationUtilDynamicAutowireService customValidationUtilDynamicAutowireService) {
		this.customValidationUtilDynamicAutowireService = customValidationUtilDynamicAutowireService;
	}
	
	@Autowired
	public void setConditionalValidationConstructUtils(
			ConditionalValidationConstructUtils conditionalValidationConstructUtils) {
		this.conditionalValidationConstructUtils = conditionalValidationConstructUtils;
	}	
	
	
	public Row validate(Row row, Map<UUID, ValidationRule> validationRulesMap) throws DataValidationException {
		row.getColumns().entrySet().parallelStream().forEach(entity -> {		
			
			if(validationRulesMap.containsKey(entity.getKey())) {
				if(Objects.isNull(columnsToValidate) || columnsToValidate.isEmpty()) {				
					Column column = entity.getValue();
					validate(row, column,validationRulesMap.get(entity.getKey()));	
				} else {
					if(columnsToValidate.contains(entity.getKey().toString())) {
						Column column = entity.getValue();
						validate(row, column,validationRulesMap.get(entity.getKey()));	
					}
				}
			}
		});	
		boolean isErrosFound = row.getColumns().entrySet().parallelStream().anyMatch(entity -> (Objects.nonNull(entity.getValue().getErrors()) && !entity.getValue().getErrors().isEmpty()));
		if(isErrosFound) {
			return row;
		}
		
		return null;
	}
	
	public void validate(Row row, Column column, ValidationRule validationRule) throws DataValidationException {		
			
		Function<ValidationRule, ValidationRule> ruleFun = rule -> {
			if(ValidationUtil.isHavingValue(rule.getConditionalValidationRule())) {
				var conditionalValidationRuleJson = new JSONObject(rule.getConditionalValidationRule());
				var temp = conditionalValidationConstructUtils.construct(row, column, conditionalValidationRuleJson);
				
				temp.setId(rule.getId());
				temp.setBu(rule.getBu());
				temp.setPlatform(rule.getPlatform());
				temp.setDataCategory(rule.getDataCategory());
				temp.setSubDataCategory(rule.getSubDataCategory());
				temp.setSourceColumn(rule.getSourceColumn());
				temp.setSourceColumnName(rule.getSourceColumnName());
				temp.setDataType(rule.getDataType());
				temp.setDataFormat(rule.getDataFormat());
				temp.setLovCheckType(rule.getLovCheckType());
				temp.setTransformationRequired(rule.getTransformationRequired());
				temp.setColumnRequiredInErrorFile(rule.getColumnRequiredInErrorFile());
				temp.setUniqueValueInColumn(rule.getUniqueValueInColumn());
				temp.setDataTransformationRules(rule.getDataTransformationRules());
				temp.setSpecialCharToBeRemoved(rule.getSpecialCharToBeRemoved());
				temp.setValidationRuleType(rule.getValidationRuleType());
				temp.setUseremail(rule.getUseremail());
				temp.setUserrole(rule.getUserrole());
				temp.setSkipValidaitons(rule.isSkipValidaitons());
				temp.setLovValidationRequired(rule.getLovValidationRequired());
				return temp;
			}
			return rule;
		};
		
		if(ValidationUtil.isHavingValue(validationRule.getDataExclusionRules())) {
			var dataExclusionRulesJson = ValidationUtil.getDatasetRules(validationRule.getDataExclusionRules(), globalDataSetUuid);
			if(dataExclusionRulesJson.has("values") && !dataExclusionRulesJson.isNull("values")) {
				var sourceValue = column.getSourceValue();
				var jsonArrObj = dataExclusionRulesJson.getJSONArray("values");
				if(Objects.nonNull(jsonArrObj) && !jsonArrObj.isEmpty()) {
					IntStream.range(0, jsonArrObj.length()).filter(index -> {
						var jsonObj = jsonArrObj.getJSONObject(index);
						return jsonObj.has("sourceValue") && !jsonObj.isNull("sourceValue") ? jsonObj.getString("sourceValue").equals(sourceValue) : false;						
					}).findFirst().ifPresent(selectedIndex -> {
						var jsonObj = jsonArrObj.getJSONObject(selectedIndex);
						if(jsonObj.has("proxyValue"))
							if(jsonObj.isNull("proxyValue")) {
								column.setValue(null);
								column.setTransformedValue(null);
							} else {
								column.setValue(jsonObj.get("proxyValue"));
								column.setTransformedValue(jsonObj.get("proxyValue"));
							}
					});
				}
				
			}
			
		}
		
		Object lovs = null;
		if(ValidationUtil.isHavingValue(validationRule.getDependsOn())) {
			lovs = ValidationUtil.isHavingValue(validationRule.getLovCheckType()) ? complexLovMetadataMap.get(validationRule.getLovCheckType()) : null;
		} else {
			lovs = ValidationUtil.isHavingValue(validationRule.getLovCheckType()) ? lovMetadataMap.get(validationRule.getLovCheckType()) : null;
		}

		var errors = customValidationUtilDynamicAutowireService.validate(row, column, ruleFun.apply(validationRule), requestContext, lovs);
			
		if(Objects.nonNull(column.getErrors()))
			column.getErrors().addAll(errors);
		else 
			column.setErrors(errors);
	}
}

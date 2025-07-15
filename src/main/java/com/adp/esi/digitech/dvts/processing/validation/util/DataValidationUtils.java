package com.adp.esi.digitech.dvts.processing.validation.util;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.ds.model.ComplexLovData;
import com.adp.esi.digitech.dvts.processing.ds.model.ValidationRule;
//import com.adp.digitech.fileprocessing.v2.autowire.service.CustomValidatorUtilDynamicAutowireService;
import com.adp.esi.digitech.dvts.processing.enums.OperatorType;
import com.adp.esi.digitech.dvts.processing.exception.DataValidationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.parser.DateParser;
import com.adp.esi.digitech.dvts.processing.parser.DecimalParser;
import com.adp.esi.digitech.dvts.processing.util.DateUtil;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Service("dataValidationUtils")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DataValidationUtils extends AbstractValidationUtils<ValidationRule> {
	
	public static final String DROP_DOWN_DATA_TYPE = "Dropdown";
	private Object lovData;	
	
	@Autowired
	public DataValidationUtils(RequestContext requestContext, Object lovData) {
		super(requestContext);
		this.lovData = lovData;
	}
	
	
	@Autowired
	Environment environment;	
	
	@Autowired
	ObjectMapper objectMapper;
	
	@Override
	public ArrayList<String> validate(Row row, Column column, ValidationRule validationRule)  throws DataValidationException {
		
		try {
			String data = (String) column.getValue();
			String dataType = validationRule.getDataType();
			ArrayList<String> errors = new ArrayList<>();
			
			//MANDATORY
			var isMandatory = ValidationUtil.isHavingValue(validationRule.getIsMandatory()) ? validationRule.getIsMandatory() : "N";
			errors.addAll(validateMandatory(column, isMandatory));
			
			if(ValidationUtil.isHavingValue(data)) {
				//STRING_CHECK_RULES
				String stringCheckRule = validationRule.getStringCheckRule();
				if(ValidationUtil.isHavingValue(stringCheckRule)) {
					JSONObject stringCheckRuleJson = new JSONObject(stringCheckRule);
					errors.addAll(validateStringCheckRule(row, column, stringCheckRuleJson));
				}
				
				//DATA_TYPE				
				if(ValidationUtil.isHavingValue(dataType)) {
					String dataFormat = validationRule.getDataFormat();
					errors.addAll(validateDataType(column, dataType, dataFormat));
				}
				
				if(column.getValue() != null && !(column.getValue() instanceof Date)) {
					//MAX_LENGTH
					errors.addAll(validateMaxCharacterLength(column, validationRule.getMaxLengthAllowed()));
					
					//MIN_LENGTH
					errors.addAll(validateMinCharcterLength(column, validationRule.getMinLengthAllowed()));
					
					//MAX_VALUE
					errors.addAll(validateMaxValue(column, validationRule.getMaxValue()));
					
					//MIN_VALUE
					errors.addAll(validateMinValue(column, validationRule.getMinValue()));
					
					//SPECIAL_CHAR_NOT_ALLOWED
					errors.addAll(validateSpecialCharacters(column, validationRule.getSpecialCharNotAllowed()));
					
					//LOV_CHECK_TYPE
					//Pending with loading of Lov Values			
					if(DROP_DOWN_DATA_TYPE.equalsIgnoreCase(dataType) && ValidationUtil.isHavingValue(validationRule.getLovValidationRequired()) 
							&& "Y".equalsIgnoreCase(validationRule.getLovValidationRequired())) {
						if (validationRule.getDependsOn() == null || validationRule.getDependsOn().trim().isEmpty()) {
							errors.addAll(validateLovCheckType(column, validationRule.getLovCheckType()));
						}
					}
					
				}	
			}
			//COMPLEX_LOV_CHECK_TYPE			
			if(DROP_DOWN_DATA_TYPE.equalsIgnoreCase(dataType) && ValidationUtil.isHavingValue(validationRule.getLovValidationRequired()) 
					&& "Y".equalsIgnoreCase(validationRule.getLovValidationRequired())) {
				if (ValidationUtil.isHavingValue(validationRule.getLovCheckType()) && ValidationUtil.isHavingValue(validationRule.getDependsOn())) {
					errors.addAll(validateComplexLovCheckType(row, column, validationRule.getDependsOn()));
				}
			}
			return errors;
		} catch (DataValidationException e) {
			log.error("DataValidationUtils - validate(), Failed Data Validation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"", e.getMessage());
			throw e;
		} catch (Exception e) {
			log.error("DataValidationUtils - validate(), Failed Data Validation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"", e.getMessage());
			var dataValidationException = new DataValidationException("Data Validation Error -  " + e.getMessage(), e.getCause());			
			throw dataValidationException;
		}
		
	}
	
	
	//MANDATORY
	private ArrayList<String> validateMandatory(Column column, String isMandatory) {
		String data = (String) column.getValue();
		ArrayList<String> colErrorRecordsArr = new ArrayList<>();
		if(ValidationUtil.isHavingValue(isMandatory) && isMandatory.equals("Y") && !ValidationUtil.isHavingValue(data)) {
			String errorMessage = "Field cannot be blank";
			colErrorRecordsArr.add(errorMessage);
		}		
		return colErrorRecordsArr;
	}
	
	//STRING_CHECK_RULES
	private ArrayList<String> validateStringCheckRule(Row row, Column column, JSONObject rule) {
		String data = (String) column.getValue();
		ArrayList<String> colErrorRecordsArr = new ArrayList<>();
		
		if(rule.has(OperatorType.Begins.getOperatorType()) && !rule.isNull(OperatorType.Begins.getOperatorType())) {
			if(!validateStringCheckRuleWithConstraint(column, row, rule, OperatorType.Begins.getOperatorType())) {				
				String errorMessage = "Field is not started with " + (String)getRhsColumn(row,rule,OperatorType.Begins.getOperatorType()).getValue();
				colErrorRecordsArr.add(errorMessage);
			}			
		}
		if(rule.has(OperatorType.Contains.getOperatorType()) && !rule.isNull(OperatorType.Contains.getOperatorType())) {
			if(!validateStringCheckRuleWithConstraint(column, row, rule, OperatorType.Contains.getOperatorType())) {
				String errorMessage = "Field does not contain  " + (String)getRhsColumn(row,rule,OperatorType.Contains.getOperatorType()).getValue();
				colErrorRecordsArr.add(errorMessage);
			}
		}
		if(rule.has(OperatorType.Ends.getOperatorType()) && !rule.isNull(OperatorType.Ends.getOperatorType())) {
			if(!validateStringCheckRuleWithConstraint(column, row, rule, OperatorType.Ends.getOperatorType())) {
				String errorMessage = "Field does not end with  "+  (String)getRhsColumn(row,rule,OperatorType.Ends.getOperatorType()).getValue();
				colErrorRecordsArr.add(errorMessage);
			}
		}
		if(rule.has(OperatorType.GreaterThan.getOperatorType()) && !rule.isNull(OperatorType.GreaterThan.getOperatorType())) {
			if(!validateStringCheckRuleWithConstraint(column, row, rule, OperatorType.GreaterThan.getOperatorType())) {
				String errorMessage = "Field is not greater than  " + (String)getRhsColumn(row,rule,OperatorType.GreaterThan.getOperatorType()).getValue();
				colErrorRecordsArr.add(errorMessage);
			}
		}
		if(rule.has(OperatorType.LessThan.getOperatorType()) && !rule.isNull(OperatorType.LessThan.getOperatorType())) {	
			if(!validateStringCheckRuleWithConstraint(column, row, rule, OperatorType.LessThan.getOperatorType())) {
				String errorMessage = "Field is not less than  " + (String)getRhsColumn(row,rule,OperatorType.LessThan.getOperatorType()).getValue();
				colErrorRecordsArr.add(errorMessage);
			}
		}
		
		if(rule.has(OperatorType.Equals.getOperatorType()) && !rule.isNull(OperatorType.Equals.getOperatorType())) {	
			if(!validateStringCheckRuleWithConstraint(column, row, rule, OperatorType.Equals.getOperatorType())) {
				String errorMessage = "Field not equals " + (String)getRhsColumn(row,rule,OperatorType.Equals.getOperatorType()).getValue();
				colErrorRecordsArr.add(errorMessage);
			}
		}
		if(rule.has(OperatorType.NotEquals.getOperatorType()) && !rule.isNull(OperatorType.NotEquals.getOperatorType())) {	
			if(!validateStringCheckRuleWithConstraint(column, row, rule, OperatorType.NotEquals.getOperatorType())) {
				String errorMessage = "Field should not be equal to " + (String)getRhsColumn(row,rule,OperatorType.NotEquals.getOperatorType()).getValue();
				colErrorRecordsArr.add(errorMessage);
			}
		}
		if(rule.has(OperatorType.DoesNotContain.getOperatorType()) && !rule.isNull(OperatorType.DoesNotContain.getOperatorType())) {
			if(!validateStringCheckRuleWithConstraint(column, row, rule, OperatorType.DoesNotContain.getOperatorType())) {
				String errorMessage = "Field should not contain  " + (String)getRhsColumn(row,rule,OperatorType.DoesNotContain.getOperatorType()).getValue();
				colErrorRecordsArr.add(errorMessage);
			}
		}
		if(rule.has(OperatorType.NULL.getOperatorType()) && !rule.isNull(OperatorType.NULL.getOperatorType())) {
			if(data != null) {
				String errorMessage = "Field should be null" ;
				colErrorRecordsArr.add(errorMessage);
			}
		}
		if(rule.has(OperatorType.NOTNULL.getOperatorType()) && !rule.isNull(OperatorType.NOTNULL.getOperatorType())) {
			
			if(data == null) {
				String errorMessage = "Field should not be null" ;
				colErrorRecordsArr.add(errorMessage);
			}
		}
		if(rule.has(OperatorType.Regex.getOperatorType()) && !rule.isNull(OperatorType.Regex.getOperatorType())) {
			if(!validateStringCheckRuleWithConstraint(column, row, rule, OperatorType.Regex.getOperatorType())) {
				String errorMessage = "Field does not match  " + (String)getRhsColumn(row,rule,OperatorType.Regex.getOperatorType()).getValue();
				colErrorRecordsArr.add(errorMessage);
			}
		}
		return colErrorRecordsArr;
	}
	
	//DATA_TYPE
	private ArrayList<String> validateDataType(Column column, String dataType, String dataFormat) {
		String data = (String) column.getValue();
		ArrayList<String> colErrorRecordsArr = new ArrayList<>();
		switch (dataType.toUpperCase()) {
		case "EMAIL":
			if(ValidationUtil.isHavingValue(dataFormat)) {
				Pattern pattern = Pattern.compile(dataFormat); 
				Matcher matcher = pattern.matcher(data); 
			
				if(matcher.matches() == false) { //Format = "^(.+)@(.+)$";
					String errorMessage = "Invalid Email Id format";
					colErrorRecordsArr.add(errorMessage);
				}
			}			
			break;
			
		case "DATE":	
			if(!ValidationUtil.isHavingValue(dataFormat)) {
				String errorMessage = "Date Format is requied for field with Date type";
				colErrorRecordsArr.add(errorMessage);
			}
			
			if(ValidationUtil.isHavingValue(dataFormat)) {
				if(!DateUtil.isDateValid(data, dataFormat)) {
					String errorMessage = "Date value is expected in "+ dataFormat +" format";
					colErrorRecordsArr.add(errorMessage);
					break;
				}
				
				try {
					var date = DateParser.parse(data, dataFormat);
					column.setValue(date);
					column.setTransformedValue(date);
				} catch (ParseException e) {
					log.info("DataValidationUtils -> validateDataType(), failed to fomat date, sourceKey = {} column = {}, value = {}", column.getSourceKey(), column.getName(), column.getValue());
				}
			}
			break;	
		
		case "NUMBER":			
			if(ValidationUtil.isHavingValue(dataFormat) && ValidationUtil.isValidJson(dataFormat)) {				
				JSONObject numberFormatJson = new JSONObject(dataFormat);				
				var format = numberFormatJson.has("format") && !numberFormatJson.isNull("format") ? numberFormatJson.getString("format"):null;
				var pattern = "";
				
				if(Objects.nonNull(format)) {									
					switch (format.toLowerCase()) {
					case "xx xxx,xx":
						pattern = environment.getProperty("decimal.regex.key1");						
						break;
					case "xx.xxx,xx":
						pattern = environment.getProperty("decimal.regex.key2");						
						break;
						
					case "xx,xxx.xx":
						pattern = environment.getProperty("decimal.regex.key3");						
						break;
						
					case "xx'xxx,xx":
						pattern = environment.getProperty("decimal.regex.key4");						
						break;
					case "xxxxx.xx":
						pattern = environment.getProperty("decimal.regex.key5");						
						break;
					case "xxxxx,xx":
						pattern = environment.getProperty("decimal.regex.key6");						
						break;	
					default:
						break;
					}
					
					if(!Pattern.matches(pattern, data)) {
						String errorMessage = "Only number value is allowed for this column and Provided value is not in valid format";
						colErrorRecordsArr.add(errorMessage);
						break;
					}
					
					try {
						 var temp = DecimalParser.parse(data, format);
						 column.setValue(temp.toString());
						 column.setTransformedValue(temp.toString());
						 column.setTargetValue(temp.toString());
					} catch (ParseException e) {
						String errorMessage = "Only number value is allowed for this column";
						colErrorRecordsArr.add(errorMessage);
					}
				}
			} else if(!NumberUtils.isParsable(data)) {
				String errorMessage = "Only number value is allowed for this column";
				colErrorRecordsArr.add(errorMessage);
			} 
			break;
			
		case "BOOLEAN":					
			if(!(data.equalsIgnoreCase("true") || data.equalsIgnoreCase("false"))) {
				String errorMessage = "Only True or False will be allowed for this column";
				colErrorRecordsArr.add(errorMessage);
			}
			break;		
			
		case "REGEX":
			if(ValidationUtil.isHavingValue(dataFormat)) {
				if(!Pattern.matches(dataFormat, data)) {
					String errorMessage = "Provided value is not in valid format";
					colErrorRecordsArr.add(errorMessage);
				}
			}
			break;
		}	
		return colErrorRecordsArr;
	}
	
	//MAX_LENGTH
	private ArrayList<String> validateMaxCharacterLength(Column column, String maxLengthAllowed) {
		String data = (String) column.getValue();
		ArrayList<String> colErrorRecordsArr = new ArrayList<>();
		
		if(ValidationUtil.isHavingValue(maxLengthAllowed) && data.length() > Double.parseDouble(maxLengthAllowed)) {
			String errorMessage = "Field should not have more than " + maxLengthAllowed + " characters";
			colErrorRecordsArr.add(errorMessage);
		}
		return colErrorRecordsArr;
	}
	
	//MIN_LENGTH
	private ArrayList<String> validateMinCharcterLength(Column column, String minLengthAllowed) {
		String data = (String) column.getValue();
		ArrayList<String> colErrorRecordsArr = new ArrayList<>();
		
		if(ValidationUtil.isHavingValue(minLengthAllowed) && data.length() < Double.parseDouble(minLengthAllowed)) {			
			String errorMessage = "Field should have minimum "+ minLengthAllowed +" characters";
			colErrorRecordsArr.add(errorMessage);
		}
		return colErrorRecordsArr;
	}
	
	//MAX_VALUE
	private ArrayList<String> validateMaxValue(Column column, String maxValue) {
		String data = (String) column.getValue();
		ArrayList<String> colErrorRecordsArr = new ArrayList<>();
		if(ValidationUtil.isHavingValue(maxValue) && NumberUtils.isParsable(data) && Double.parseDouble(data) > Double.parseDouble(maxValue)) {
			String errorMessage = "Provided value should be less than " + maxValue;
			colErrorRecordsArr.add(errorMessage);
		}
		return colErrorRecordsArr;
		
	}
	
	//MIN_VALUE
	private ArrayList<String> validateMinValue(Column column, String minValue) {
		String data = (String) column.getValue();
		ArrayList<String> colErrorRecordsArr = new ArrayList<>();
		
		if(ValidationUtil.isHavingValue(minValue) && NumberUtils.isParsable(data) && Double.parseDouble(data) < Double.parseDouble(minValue)) {
			String errorMessage = "Provided value should be greater than " + minValue;
			colErrorRecordsArr.add(errorMessage);
		}
		return colErrorRecordsArr;
	}
	
	//SPECIAL_CHAR_NOT_ALLOWED
	@SuppressWarnings("unused")
	private ArrayList<String> validateSpecialCharactersOld(Column column, String specialCharacters) {
		String data = (String) column.getValue();
		ArrayList<String> colErrorRecordsArr = new ArrayList<>();
		
		if(ValidationUtil.isHavingValue(specialCharacters)) {	
			for(int i=0; i<specialCharacters.length(); i++){
			    if(data.contains(Character.toString(specialCharacters.charAt(i)))){
					String errorMessage = "Provided value can't have special characters " + specialCharacters;
					colErrorRecordsArr.add(errorMessage);
			        break;
			    }
			}				
		}
		return colErrorRecordsArr;
	}
	
	//SPECIAL_CHAR_NOT_ALLOWED
	private ArrayList<String> validateSpecialCharacters(Column column, String specialCharacters) {
		String data = (String) column.getValue();
		ArrayList<String> colErrorRecordsArr = new ArrayList<>();
		if(ValidationUtil.isValidJsonArray(specialCharacters)) {
			JSONArray spCharJSArray = new JSONArray(specialCharacters);
			var errors = IntStream.range(0, spCharJSArray.length()).parallel().mapToObj(index -> {
				JSONObject spCharJSONObj = spCharJSArray.getJSONObject(index);
				String errorMessage = null;
				if (Objects.nonNull(spCharJSONObj) && spCharJSONObj.has("character") && spCharJSONObj.has("max") && spCharJSONObj.has("min")) {
					var sChar = spCharJSONObj.getString("character");					
					char character =sChar.equalsIgnoreCase("<space>") ? ' ' : sChar.charAt(0);
					int maxCharLen = (int) spCharJSONObj.get("max");
					int minCharLen = (int) spCharJSONObj.get("min");
					int cnt = StringUtils.countMatches(data, character);
					if(cnt > 0 && (maxCharLen == 0 && minCharLen == 0)) {
				    	errorMessage = "Provided value can't have special characters " + sChar;						
					} else if(cnt > 0 && maxCharLen > 0 && minCharLen >= 0) {				    	
				    	if(cnt < minCharLen || cnt > maxCharLen) {
				    		errorMessage = "Provided value can have a minimum of "+ minCharLen +" " +"and a maximum of "+ maxCharLen +" "+ sChar +" special characters only";				    	
						}
				    }
				} else {
					errorMessage = "Special characters Configuration is invalid.";
				}
				
				return errorMessage;
			}).filter(ValidationUtil::isHavingValue).toList();
			
			if (errors.size() > 0) {
				var speCharError = errors.stream()
						.filter(speChar -> speChar.contains("Provided value can't have special characters"))
						.map(scErr -> {
							return scErr.substring(scErr.length() - 1);
						}).collect(Collectors.joining(","));
				speCharError = ValidationUtil.isHavingValue(speCharError)
						? "Provided value can't have special characters " + speCharError
						: "";
				
				var scLengthErr = errors.stream()
						.filter(lengthErr -> lengthErr.contains("Provided value can have a minimum of"))
						.collect(Collectors.joining("\n"));
				
				if ((ValidationUtil.isHavingValue(speCharError)) && (ValidationUtil.isHavingValue(scLengthErr))) {
					colErrorRecordsArr.addAll(List.of(speCharError.concat("\n" + scLengthErr)));
				} else if (ValidationUtil.isHavingValue(speCharError)) {
					colErrorRecordsArr.addAll(List.of(speCharError));
				} else if (ValidationUtil.isHavingValue(scLengthErr)) {
					colErrorRecordsArr.addAll(List.of(scLengthErr));
				}
			}
		}
		return colErrorRecordsArr;
	}
	
	//LOV_CHECK_TYPE
	private ArrayList<String> validateLovCheckType(Column column, String lovCheckType) {
		Properties lovMetadataProperties = (Properties) lovData;
		String data = (String) column.getValue();
		ArrayList<String> colErrorRecordsArr = new ArrayList<>();
		
		if(ValidationUtil.isHavingValue(lovCheckType)) {
			if(Objects.isNull(lovMetadataProperties) || lovMetadataProperties.isEmpty() || !lovMetadataProperties.containsKey(data)) {
				String errorMessage = "Provided value is not a valid value";
				colErrorRecordsArr.add(errorMessage);
			}
		}
		return colErrorRecordsArr;
	}
	
	//COMPLEX_LOV_CHECK_TYPE
	private ArrayList<String> validateComplexLovCheckType(Row row, Column column, String dependsOn) {
		ComplexLovData complexLovData = (ComplexLovData) lovData;
		ArrayList<String> errors = new ArrayList<>();
		String[] dependsOnArray;

		try {
			dependsOnArray = objectMapper.readValue(dependsOn, String[].class);
		} catch (Exception e) {
			errors.add("Invalid dependsOn JSON format");
			return errors;
		}

		var lovDataList = complexLovData.getLovDataMap();
		boolean macthFound = lovDataList.parallelStream()
				.filter(lovDataMap -> IntStream.range(0, dependsOnArray.length).allMatch(index -> {
					try {

						var uuid = dependsOnArray[index];
						var value = row.getColumns().get(UUID.fromString(uuid)).getSourceValue();
						return lovDataMap.get(String.valueOf(index)).equals(value);
					} catch (Exception e) {
						return false;
					}
				})).findFirst().isPresent();
		//log.info("DataValidationUtils - validateComplexLovCheckType(), macthFound = {}, dependsOn = {}", macthFound, dependsOnArray);
		if (!macthFound) {
			errors.add("No matching LOV data found for the given dependsOn criteria");
		}

		return errors;
	}
	
	private boolean validateStringCheckRuleWithConstraint(Column lhs, Row row, JSONObject rule, String constraint) {
			Column rhs = getRhsColumn(row,rule,constraint);
			return ConditionalValidationConstructUtils.checkConstraint(lhs, rhs, constraint);
	}
	private Column getRhsColumn(Row row, JSONObject rule, String constraint) {
		
		String rhsVal = rule.getString(constraint);

		Column rhs = rhsVal.startsWith("{{") && rhsVal.endsWith("}}")
				? row.getColumns().get(UUID.fromString(rhsVal.substring(2, rhsVal.length() - 2)))
				: columnObjProvider.getObject(null,rhsVal,null,null);

		return rhs;
	}
}

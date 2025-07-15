package com.adp.esi.digitech.dvts.processing.ds.model;

import lombok.Data;

@Data
public class ValidationRule {
	
	private Long id;
		
	private String bu;
	
	private String platform;
	
	private String dataCategory;	
	
	private String subDataCategory;	
	
	private String sourceColumn;
	
	private String sourceColumnName;
	
	private String dataType;
	
	private String isMandatory;
	
	private String maxLengthAllowed;
	
	private String dataFormat;
	
	private String minValue;
	
	private String maxValue;
	
	private String specialCharNotAllowed;
	
	private String lovCheckType;
	
	private String transformationRequired;
	
	private String columnRequiredInErrorFile;
	
	private String uniqueValueInColumn;
	
	private String conditionalValidationRule;
	
	private String dataTransformationRules;
	
	private String specialCharToBeRemoved;
	
	private String dataExclusionRules;
	
	private String validationRuleType;
	
	private String minLengthAllowed;
	
	private String stringCheckRule;
		
	private String useremail;
	
	private String userrole;
	
	private boolean isSkipValidaitons;
	
	private String lovValidationRequired;
	
	private String dependsOn;

}

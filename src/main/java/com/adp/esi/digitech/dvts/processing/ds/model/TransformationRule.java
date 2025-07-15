package com.adp.esi.digitech.dvts.processing.ds.model;

import lombok.Data;

@Data
public class TransformationRule {
	
	private Long id;
	
	private String bu;
	
	private String platform;
	
	private String dataCategory;
	
	private String subDataCategory;
		
	private String sourceColumnName;
	
	private String dataType;
	
	private String dataFormat;
	
	private String targetColumnName;
	
	private String targetFileName;	
	
	private Integer columnSequence;
	
	private String defaultValue;
	
	private String dataTransformationRules;
	
	private String specialCharToBeRemoved;
	
	private String lovCheckType;
	
	private String transformationRequired;
	
	private String useremail;
	
	private String userrole;
	
	private String lovValidationRequired;
	
	private String dependsOn;

}

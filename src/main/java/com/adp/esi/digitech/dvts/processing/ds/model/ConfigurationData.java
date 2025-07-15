package com.adp.esi.digitech.dvts.processing.ds.model;

import lombok.Data;

@Data
public class ConfigurationData {

	private Long id;
	
	private String bu;
		
	private String platform;
	
	private String dataCategory;
	
	private String subDataCategory;	
	
	private String outputFileRules;
	
	private String appCode;	
	
	private String source;	
	
	private String inputRules;
	
	private String useremail;
	
	private String userrole;
	
	private String filesInfo;
	
	private String dataRules;
	
	private String targetLocation;
	
	private String targetPath;
	
	private String processType;
	
	private String processSteps;
}

package com.adp.esi.digitech.dvts.processing.ds.model;

import lombok.Data;

@Data
public class ColumnRelation {
	
	private Long id;
	
	private String bu;
		
	private String platform;
	
	private String dataCategory;
		
	private String sourceKey;
	
	private String columnName;	
	
	private Long position;
	
	private String aliasName;
	
	private String uuid;
	
	private String required;
	
	private String columnRequiredInErrorFile;
	
	private String useremail;
	
	private String userrole;
	

}

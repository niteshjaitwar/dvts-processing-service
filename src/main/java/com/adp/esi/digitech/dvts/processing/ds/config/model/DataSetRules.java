package com.adp.esi.digitech.dvts.processing.ds.config.model;

import lombok.Data;

@Data
public class DataSetRules {
	
	private String dataSetId;
	private String dataSetName;
	private DataFilter filters;
	private DataGroupBy groupBy;

}

package com.adp.esi.digitech.dvts.processing.model;

import java.util.List;
import java.util.Map;

import com.adp.esi.digitech.dvts.processing.ds.config.model.DataSetRules;
import com.adp.esi.digitech.dvts.processing.enums.ValidationType;

import lombok.Data;

@Data
public class DataPayload {	
	
	private String datasetId;
	
	private String datasetName;
	
	private int batchSize;
	
	private String batchName;
	
	private ValidationType validationType;
	
	private RequestContext requestContext;
	
	private List<DataMap> data;
	
	private Map<String, String> clause;
	
	private DataSetRules dataSetRule;
	
	private List<String> columnsToValidate;	
}

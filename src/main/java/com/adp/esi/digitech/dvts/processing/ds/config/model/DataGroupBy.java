package com.adp.esi.digitech.dvts.processing.ds.config.model;

import java.util.List;

import lombok.Data;

@Data
public class DataGroupBy {
	
	private List<DynamicClause> clause;
	
	private String[] columns;
	
	private List<DataFilter> filters;
	
	private List<DataAggregation> aggregations;

}

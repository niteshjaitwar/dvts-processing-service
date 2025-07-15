package com.adp.esi.digitech.dvts.processing.ds.config.model;

import java.util.List;

import lombok.Data;

@Data
public class DataFilter {

	private String operator;
	private String select;
	private List<Rule> rules;
	private List<Condition> conditions;
	
}

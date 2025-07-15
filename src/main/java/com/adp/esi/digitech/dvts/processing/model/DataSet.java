package com.adp.esi.digitech.dvts.processing.model;

import java.util.List;

import com.adp.esi.digitech.dvts.processing.enums.ValidationType;

import lombok.Data;

@Data
public class DataSet {
	
	private String id;
	
	private String name;
	
	private ValidationType validationType;
	
	private List<Row> data;
	

	
}

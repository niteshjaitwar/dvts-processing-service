package com.adp.esi.digitech.dvts.processing.ds.model;

import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class ComplexLovData {
	
	private Long id;
	private String lovType;
	private String lovDataJson;
	private int lovRecordsCount;
	private String lovSchema;
	private List<Map<String, String>> lovDataMap;
}

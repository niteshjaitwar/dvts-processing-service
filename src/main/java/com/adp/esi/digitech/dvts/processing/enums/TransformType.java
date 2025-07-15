package com.adp.esi.digitech.dvts.processing.enums;

public enum TransformType {

	DEFAULT_VALUE("DEFAULT_VALUE"), DATA_TRANSFORMATION_VALUE("DATA_TRANSFORMATION_VALUE");
	
	TransformType(String transformUtilType) {
		this.transformUtilType = transformUtilType;
	}
	
	
	private String transformUtilType;
	
	public String getTransformUtilType() {
		return this.transformUtilType;
	}
}

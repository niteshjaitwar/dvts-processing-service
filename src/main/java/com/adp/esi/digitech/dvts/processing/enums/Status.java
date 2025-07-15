package com.adp.esi.digitech.dvts.processing.enums;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Status {
	SUCCESS("success"), ERROR("error"), FAILED("failed"), 
	CLIENT_DATA_VALIDATION("CLIENT_DATA_VALIDATION"), 
	CAM_DATA_VALIDATION("CAM_DATA_VALIDATION"),
	DATA_TRANSFORMATION("DATA_TRANSFORMATION");
	
	private final String status;
	
	Status(String status) {
		this.status = status;
	}
	
	@JsonValue
	public String getStatus() {
		return status;
	}
}

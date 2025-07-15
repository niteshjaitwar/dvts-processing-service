package com.adp.esi.digitech.dvts.processing.enums;

public enum ProcessType {
	
	sync("sync"),async("async"), chunks("chunks"), in_memory("in_memory");
	
	ProcessType(String processType) {
		this.processType = processType;
	}
	
	
	private String processType;
	
	public String getProcessType() {
		return this.processType;
	}

}

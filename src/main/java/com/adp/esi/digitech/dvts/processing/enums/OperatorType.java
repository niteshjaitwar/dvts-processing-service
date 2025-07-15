package com.adp.esi.digitech.dvts.processing.enums;

public enum OperatorType {
	
	Equals("Equals"), NotEquals("NotEquals"), Greater("Greater"), Lesser("Lesser"), Begins("Begins"), Ends("Ends"),
	Contains("Contains"), DoesNotContain("DoesNotContain"), MinLength("MinLength"), MaxLength("MaxLength"),
	MinValue("MinValue"), MaxValue("MaxValue"), Mandatory("Mandatory"), SpecialCharNotAllowed("SpecialCharNotAllowed"),
	GreaterThan("GreaterThan"), LessThan("LessThan"), Regex("Regex"), NULL("<null>"), NOTNULL("<notnull>"), In("In");

	OperatorType(String operatorType) {
		this.operatorType = operatorType;
		
		
	}
	
	private String operatorType;

	public String getOperatorType() {
		return this.operatorType;
	}
}

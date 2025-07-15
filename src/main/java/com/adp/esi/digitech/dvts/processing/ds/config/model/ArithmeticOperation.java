package com.adp.esi.digitech.dvts.processing.ds.config.model;

import java.util.List;

import lombok.Data;

@Data
public class ArithmeticOperation {
	
	private String columns;
	
	private List<Operation> operations;

}

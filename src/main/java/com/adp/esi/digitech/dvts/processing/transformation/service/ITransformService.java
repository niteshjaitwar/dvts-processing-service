package com.adp.esi.digitech.dvts.processing.transformation.service;

import java.util.List;

import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.DataSet;
import com.adp.esi.digitech.dvts.processing.model.Row;

public interface ITransformService {

	List<Row> transform(DataSet dataSet) throws TransformationException;
}

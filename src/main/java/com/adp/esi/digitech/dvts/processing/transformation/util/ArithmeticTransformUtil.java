package com.adp.esi.digitech.dvts.processing.transformation.util;

import java.util.Objects;
import java.util.UUID;

import org.json.JSONObject;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;
import com.adp.esi.digitech.dvts.processing.ds.config.model.ArithmeticOperation;
import com.adp.esi.digitech.dvts.processing.enums.TransformType;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.Row;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("arithmeticTransformUtil")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ArithmeticTransformUtil extends AbstractTransformUtil{
	
	@Override
	public void transform(Row row, Column column, JSONObject rule) throws TransformationException {
		try {
			
			if (rule != null && rule.has("arithmetic_operations") && !rule.isNull("arithmetic_operations")) {
				
				var json = rule.get("arithmetic_operations");
				
				var obj = objectMapper.readValue(json.toString(), ArithmeticOperation.class);
				
				transform(row, column, obj);
			}
			
		} catch (Exception e) {
			log.error("ArithmeticTransformUtil - transform(), Failed Data Transformation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"", e.getMessage());
			
			var transformationException = new TransformationException("Transformation Error - " + e.getMessage(), e.getCause());
			transformationException.setRequestContext(requestContext);
			throw transformationException;
		}
	}
	
	public void transform(Row row, Column column, ArithmeticOperation rule) throws TransformationException {
		try {
			
			if (rule != null) {
					//rule.has("arithmetic_operations") && !rule.isNull("arithmetic_operations")) {
				
				var operations = rule.getOperations();
				
				String result = null;
				
				if(Objects.nonNull(operations) && Objects.nonNull(column)) {
					
					var uuid = rule.getColumns();
					var currentColumn = row.getColumns().get(UUID.fromString(uuid));
					double[] value = {getValue(currentColumn.getValue())};
					//value = tempValue;
					operations.stream().forEach(obj -> {
						String operator = obj.getOperator().toLowerCase();

						Double temp = null;
						if (obj.getColumns()!= null) {
							var columnUUid = UUID.fromString(obj.getColumns());
							var configColumn = row.getColumns().get(columnUUid);
							temp = getValue(configColumn.getValue());
						} else {
							temp = Double.parseDouble(obj.getValue());
						}
						double valuefinal = temp.doubleValue();
						
						switch (operator) {
						case "addition":
							value[0] = (value[0] + valuefinal);											
							break;
						case "subtraction":
							value[0] = (value[0] - valuefinal);												
							break;
						case "multiplication":
							value[0] = (value[0] * valuefinal);											
							break;
						case "division":
							value[0] = (value[0] / valuefinal);													
							break;
						}
					});
					
					result = Double.toString(value[0]);	
				}
				
				if(Objects.nonNull(result)) {
					result = result.contains(".") ? result.replaceAll("0*$", "").replaceAll("\\.$", "") : result;
					if(this.transformationType.getTransformUtilType().equals(TransformType.DEFAULT_VALUE.getTransformUtilType())) {
						column.setValue(result);
					}
					column.setTransformedValue(result);
					column.setTargetValue(result);
				}
			}
			
		} catch (Exception e) {
			log.error("ArithmeticTransformUtil - transform(), Failed Data Transformation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"", e.getMessage());
			
			var transformationException = new TransformationException("Transformation Error - " + e.getMessage(), e.getCause());
			transformationException.setRequestContext(requestContext);
			throw transformationException;
		}
		
	}	
	
	private double getValue(Object value) {
		if(value == null || !ValidationUtil.isHavingValue(value.toString())) return 0.0;
		return (value instanceof Double) ? (Double) value : Double.parseDouble(value.toString());
	}
}

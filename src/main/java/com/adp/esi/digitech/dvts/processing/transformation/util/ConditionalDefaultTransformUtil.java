package com.adp.esi.digitech.dvts.processing.transformation.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.util.Precision;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.autowire.service.CustomTransformUtilDynamicAutowireService;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConditionalDefaultTransformUtil extends AbstractTransformUtil {
	
	@Autowired
	CustomTransformUtilDynamicAutowireService customTransformUtilDynamicAutowireService;
	
	@Override
	public void transform(Row row, Column column, JSONObject rule) throws TransformationException {
		try {
			
			if (ValidationUtil.isHavingValue(rule.toString()) && rule.has("conditional") && !rule.isNull("conditional")) {	
				JSONArray conditionalJsonArray = rule.getJSONArray("conditional");
				
				OptionalInt selectedIndex = IntStream.range(0, conditionalJsonArray.length())
													 .parallel().filter(index -> {
														 JSONObject conditionalJson = conditionalJsonArray.getJSONObject(index);
														 var operator = conditionalJson.has("operator") && !conditionalJson.isNull("operator") ? conditionalJson.getString("operator"): "and";
														 
														 if(conditionalJson.has("if") && !conditionalJson.isNull("if"))
															 return checkIfConditions(row, conditionalJsonArray.getJSONObject(index).getJSONArray("if"), operator);	
														 return false;
													 }).findFirst();
				if(selectedIndex.isPresent()) {
					int index = selectedIndex.getAsInt();
					JSONObject selectedConditional = conditionalJsonArray.getJSONObject(index);
					JSONObject thenRule = selectedConditional.getJSONObject("then"); 
					calculateValue(row, column, thenRule);
					return;
				}
				
				JSONObject elseRule = rule.getJSONObject("else");
				calculateValue(row, column, elseRule);
			}
			
		} catch (Exception e) {
			log.error("ConditionalDefaultTransformUtil - transform(), Failed Data Transformation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"",  e.getMessage());
			var transformationException = new TransformationException("Transformation Error - " + e.getMessage(), e.getCause());
			transformationException.setRequestContext(requestContext);
			throw transformationException;
		}
			
	}
	
	private void calculateValue(Row row, Column column, JSONObject rule) {
		String type = rule.getString("type").toLowerCase();
		
		switch (type) {
		case "arithmetic":
			if(column.getValue() instanceof Date)
				customTransformUtilDynamicAutowireService.transform(DateArithmeticTransformUtil.class, row, column, rule, requestContext, this.transformationType);
			else
				customTransformUtilDynamicAutowireService.transform(ArithmeticTransformUtil.class, row, column, rule, requestContext, this.transformationType);			
			break;
		case "concatination": case "split": case "trim":
			customTransformUtilDynamicAutowireService.transform(StringTransformUtil.class, row, column, rule, requestContext, this.transformationType);
			break;
		case "case_conversion":
			customTransformUtilDynamicAutowireService.transform(CaseConvertionTransformUtil.class, row, column, rule, requestContext, this.transformationType);
			break;
		default:
			JSONObject operation = rule.getJSONObject("operation");
			transformDefaultOperation(row, column, operation);
			break;
		}
		
	}
	
	

	
	
	private boolean checkIfConditions(Row row, JSONArray ifConditions, String operator) {
		
		var conditionList = IntStream.range(0, ifConditions.length()).parallel().mapToObj(index -> {
			JSONObject ifCondition = ifConditions.getJSONObject(index);
			if (ifCondition == null) {
				return (Predicate<Boolean>)(lhsValue) -> false;
			}
			String constraint = ifCondition.getString("constraint");
			String lhsVal = ifCondition.getString("lhs");
			String rhsVal = ifCondition.getString("rhs");
			Column lhs;
			Column rhs;

			lhs = lhsVal.startsWith("{{") && lhsVal.endsWith("}}") ? row.getColumns().get(UUID.fromString(lhsVal.substring(2, lhsVal.length() - 2))) : columnObjProvider.getObject(null, lhsVal, null, null);
			rhs = rhsVal.startsWith("{{") && rhsVal.endsWith("}}") ? row.getColumns().get(UUID.fromString(rhsVal.substring(2, rhsVal.length() - 2))) : columnObjProvider.getObject(null, rhsVal, null, null);

			return check(lhs, rhs, constraint);			

		});
		if (operator.equalsIgnoreCase("or"))
			return conditionList.parallel().reduce(x -> false, Predicate::or).test(true);
		
		return conditionList.parallel().reduce(x -> true, Predicate::and).test(true);
		
	}

	private Predicate<Boolean> check(Column lhs, Column rhs, String constraint) {
        return (lhsValue) -> {
            return checkConstraint(lhs, rhs, constraint);
        };
	}
	private boolean checkConstraint(Column lhs, Column rhs, String constraint) {
		//log.info("ConditionalDefaultTransformUtil - transform(), Starting Data Transformation requestUuid = {}, lhs = {}, rhs = {}", requestContext.getRequestUuid(), lhs, rhs);
		switch (constraint) {
		case "<notnull>":
				return lhs != null && lhs.getValue() != null ? (lhs.getValue() instanceof String ? ValidationUtil.isHavingValue((String)lhs.getValue()): true) : false;
		case "<null>":
				return lhs != null && lhs.getValue() == null;
		case "equals":
				return (lhs != null && rhs != null && lhs.getValue() != null && rhs.getValue() != null)? ((lhs.getValue() instanceof Date && rhs.getValue() instanceof Date) ? 
						((Date)lhs.getValue()).equals((Date)rhs.getValue()) : NumberUtils.isParsable(lhs.getValue().toString()) && NumberUtils.isParsable(rhs.getValue().toString()) ? Precision.equals(Double.parseDouble(lhs.getValue().toString()), Double.parseDouble(rhs.getValue().toString()))  : (lhs.getValue().toString()).equalsIgnoreCase(rhs.getValue().toString())):false;
		case "notequals":
			return (lhs != null && rhs != null && lhs.getValue() != null && rhs.getValue() != null)? ((lhs.getValue() instanceof Date && rhs.getValue() instanceof Date) ? 
					!((Date)lhs.getValue()).equals((Date)rhs.getValue()) : !(lhs.getValue().toString()).equalsIgnoreCase(rhs.getValue().toString())):false;			
		case "greaterthan":
			//(ValidationUtil.isHavingValue((String)lhs.getValue()) && ValidationUtil.isHavingValue((String)rhs.getValue())) ? Long.parseLong((String)lhs.getValue()) > Long.parseLong((String)rhs.getValue())
			return (lhs != null && rhs != null && lhs.getValue() != null && rhs.getValue() != null)? ((lhs.getValue() instanceof Date && rhs.getValue() instanceof Date) ? 
					((Date)lhs.getValue()).after((Date)rhs.getValue()) : NumberUtils.isParsable(lhs.getValue().toString()) && NumberUtils.isParsable(rhs.getValue().toString()) ? Double.parseDouble(lhs.getValue().toString()) > Double.parseDouble(rhs.getValue().toString())  :false):false; 
		case "lesserthan":
			//(ValidationUtil.isHavingValue((String)lhs.getValue()) && ValidationUtil.isHavingValue((String)rhs.getValue())) ? Long.parseLong((String)lhs.getValue()) < Long.parseLong((String)rhs.getValue())
			return (lhs != null && rhs != null && lhs.getValue() != null && rhs.getValue() != null)? ((lhs.getValue() instanceof Date && rhs.getValue() instanceof Date) ? 
					((Date)lhs.getValue()).before((Date)rhs.getValue()) :NumberUtils.isParsable(lhs.getValue().toString()) && NumberUtils.isParsable(rhs.getValue().toString()) ? Double.parseDouble(lhs.getValue().toString()) < Double.parseDouble(rhs.getValue().toString())  : false): false;
		default:
			break;
		}
		return false;
	}
	
	
	
	
	private void transformDefaultOperation(Row row, Column column, JSONObject rule) {
		ArrayList<String> dataList = new ArrayList<String>();
		String value = "";
		String seperator = "";
		if(rule.has("default") && !rule.isNull("default") && ValidationUtil.isHavingValue(rule.getString("default")))
			dataList.add(rule.getString("default"));
		
		if(rule.has("columns") && !rule.isNull("columns") && rule.getJSONArray("columns") != null && rule.getJSONArray("columns").length() > 0) {
			var columnsJsonArray = rule.getJSONArray("columns");
			
			var columnsData= IntStream.range(0, columnsJsonArray.length()).mapToObj(index -> {
				UUID key = UUID.fromString(columnsJsonArray.getString(index));
				return (row.getColumns().get(key).getValue() != null) ? row.getColumns().get(key).getValue() instanceof Date ? row.getColumns().get(key).getTargetValue() :row.getColumns().get(key).getValue().toString():null;
			}).filter(Objects::nonNull).collect(Collectors.toList());
			dataList.addAll(columnsData);
			/*
			rule.getJSONArray("columns").forEach(col -> {
				UUID key = UUID.fromString(String.valueOf(col));
				dataList.add(row.getColumns().get(key).getValue().toString());
			});*/
		}
		
		if(rule.has("seperator") && ValidationUtil.isHavingValue(rule.getString("seperator")))
			seperator = rule.getString("seperator");
		
		value = dataList.stream().collect(Collectors.joining(seperator));
		column.setValue(value);
		column.setTransformedValue(value);
		column.setTargetValue(value);
			
	}
}

package com.adp.esi.digitech.dvts.processing.transformation.util;

import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.SerializationUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;
import com.adp.esi.digitech.dvts.processing.enums.TransformType;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.Row;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("stringTransformUtil")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class StringTransformUtil extends AbstractTransformUtil {
	
	private static final String SPLIT_OPERATIONS = "split_operations";
	private static final String FIELD_CONCAT = "field_concat";
	private static final String CONCATINATE = "concatinate";
	private static final String DEFAULT_VALUE = "defaultValue";
	private static final String COLUMNS = "columns";
	private static final String COLUMN = "column";
	private static final String OPERATIONS = "operations";
	private static final String SEPERATOR = "seperator";
	private static final String TRIM = "trim";
	private static final String FIELD_VALUE = "Field_Value";
	
	@Override
	public void transform(Row row, Column column, JSONObject rule) throws TransformationException {
		try {
			if (rule != null)
				if (rule.has(StringTransformUtil.CONCATINATE) && !rule.isNull(StringTransformUtil.CONCATINATE))
					concatinate(row, column, rule);
				else if (rule.has(StringTransformUtil.FIELD_CONCAT) && !rule.isNull(StringTransformUtil.FIELD_CONCAT))
					fieldConcat(row, column, rule);
				else if (rule.has(StringTransformUtil.DEFAULT_VALUE) && !rule.isNull(StringTransformUtil.DEFAULT_VALUE))
					defaultValue(column, rule);
				else if(rule.has(StringTransformUtil.SPLIT_OPERATIONS)  && !rule.isNull(StringTransformUtil.SPLIT_OPERATIONS))
					splitOperations(row, column, rule);
				else if(rule.has(StringTransformUtil.TRIM)  && !rule.isNull(StringTransformUtil.TRIM))
					trimOperation(row, column, rule);
				else if(rule.has(StringTransformUtil.FIELD_VALUE)  && !rule.isNull(StringTransformUtil.FIELD_VALUE))
					fieldValue(row, column, rule);
					
			
		} catch(Exception e) {
			log.error("StringTransformUtil - transform(), Failed Data Transformation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"", e.getMessage());
			
			var transformationException = new TransformationException("Transformation Error - " + e.getMessage(), e.getCause());
			transformationException.setRequestContext(requestContext);
			throw transformationException;
		}

	}

	private void concatinate(Row row, Column column, JSONObject rule) {
		JSONObject concat = rule.getJSONObject(StringTransformUtil.CONCATINATE);
		JSONArray columns = concat.getJSONArray(StringTransformUtil.COLUMNS);
		
		
		String data = IntStream.range(0, columns.length())
				.mapToObj(index -> {
					var key = UUID.fromString(columns.getString(index));
					return Objects.nonNull(row.getColumns().get(key)) && Objects.nonNull(row.getColumns().get(key).getValue())
	                        ? row.getColumns().get(key).getValue() instanceof Date ? row.getColumns().get(key).getTargetValue() : row.getColumns().get(key).getValue().toString() : "";
	        				
				}).collect(Collectors.joining(concat.getString(StringTransformUtil.SEPERATOR)));
				
				
				
				
		column.setValue(data);
		column.setTransformedValue(data);
		column.setTargetValue(data);

	}

	private void fieldConcat(Row row, Column column, JSONObject rule) {
		var fieldconcatJSON = rule.getJSONObject(StringTransformUtil.FIELD_CONCAT);
		
		var operationsJSONArray = fieldconcatJSON.has(StringTransformUtil.OPERATIONS) && !fieldconcatJSON.isNull(StringTransformUtil.OPERATIONS) ? fieldconcatJSON.getJSONArray(StringTransformUtil.OPERATIONS) : null;
		
		if (Objects.isNull(operationsJSONArray) || operationsJSONArray.length() <= 0)			
		    return;
		
		String data = IntStream.range(0, operationsJSONArray.length()).mapToObj(index -> {
			var operation = operationsJSONArray.getJSONObject(index);
			var key = UUID.fromString(operation.getString(StringTransformUtil.COLUMNS));
			var celldata = Objects.nonNull(row.getColumns().get(key)) && Objects.nonNull(row.getColumns().get(key).getValue()) 
					? row.getColumns().get(key).getValue() instanceof Date ? row.getColumns().get(key).getTargetValue() : row.getColumns().get(key).getValue().toString() : "";

			if (ValidationUtil.isHavingValue(celldata))
				celldata = new StringBuilder().append(operation.getString("pre")).append(celldata).append(operation.getString("post")).toString();
			return celldata;
		}).collect(Collectors.joining(""));

		column.setValue(data);
		column.setTransformedValue(data);
		column.setTargetValue(data);
	}

	private void defaultValue(Column column, JSONObject rule) {
		column.setValue(rule.getString(StringTransformUtil.DEFAULT_VALUE));
		column.setTransformedValue(rule.getString(StringTransformUtil.DEFAULT_VALUE));
		column.setTargetValue(rule.getString(StringTransformUtil.DEFAULT_VALUE));
	}
	
	private void fieldValue(Row row, Column column, JSONObject rule) {
		var fieldValue = rule.optJSONObject(StringTransformUtil.FIELD_VALUE);
		BiFunction<Row, String, Column> columnFunction = (rowData,uuid) -> rowData.getColumns().get(UUID.fromString(uuid));
		
		if(Objects.nonNull(fieldValue)) {
			var tempColumn = columnFunction.apply(row, fieldValue.optString(COLUMN));
			if(tempColumn != null) {
				var colnedCol = SerializationUtils.clone(tempColumn);
				column.setValue(colnedCol.getValue());
				column.setTransformedValue(colnedCol.getTransformedValue());
				column.setTargetValue(colnedCol.getTargetValue());
				column.setSourceKey(colnedCol.getSourceKey());
				column.setTargetName(colnedCol.getTargetName());
				column.setSourceValue(colnedCol.getSourceValue());
			}
			
		}
			
		
		

	}
	
	private void splitOperations(Row row, Column column, JSONObject rule) {
		
		BiFunction<Row, String, Column> columnFunction = (rowData,uuid) -> rowData.getColumns().get(UUID.fromString(uuid));
		
		var splitOperations = rule.getJSONObject(StringTransformUtil.SPLIT_OPERATIONS);
		
		var operationsJSONArray = splitOperations.has(StringTransformUtil.OPERATIONS) && !splitOperations.isNull(StringTransformUtil.OPERATIONS)
				? splitOperations.getJSONArray(StringTransformUtil.OPERATIONS)
				: null;
		var seperator = splitOperations.has(StringTransformUtil.SEPERATOR) && !splitOperations.isNull(StringTransformUtil.SEPERATOR)
				? splitOperations.getString(StringTransformUtil.SEPERATOR)
				: "";
		
		if (Objects.nonNull(operationsJSONArray) && operationsJSONArray.length() > 0) {		

			var data = IntStream.range(0, operationsJSONArray.length()).mapToObj(index -> {
				var configOperation = operationsJSONArray.getJSONObject(index);
				
				if(configOperation.has(StringTransformUtil.COLUMNS)) {
					
					return !configOperation.isNull(StringTransformUtil.COLUMNS) ? splitOperations(columnFunction.apply(row, configOperation.getString(StringTransformUtil.COLUMNS)), configOperation) : null;
				}
				return splitOperations(column, configOperation);
				
			}).filter(Objects::nonNull).collect(Collectors.joining(seperator));
			
			if(this.transformationType.getTransformUtilType().equals(TransformType.DEFAULT_VALUE.getTransformUtilType())) {
				column.setValue(data);
			}
			
			column.setTransformedValue(data);
			column.setTargetValue(data);
		}
	}

	private String splitOperations(Column column, JSONObject configOperation) {
		var LENGTH = "length";
		var POSITION = "position";
		var DIRECTION = "direction";
		
		if(column.getValue() == null) 
			return null;
		
		var cellData = column.getValue().toString();

		var direction = configOperation.has(DIRECTION) && !configOperation.isNull(DIRECTION)
				? configOperation.getString(DIRECTION)
				: "Start";

		int position = configOperation.has(POSITION) && !configOperation.isNull(POSITION)
				? configOperation.getInt(POSITION)
				: 1;

		int length = configOperation.has(LENGTH) && !configOperation.isNull(LENGTH)
				? configOperation.getInt(LENGTH)
				: -1;

		if (position == 1 && length == -1)
			return cellData;

		if (ValidationUtil.isHavingValue(cellData) && position <= cellData.length()) {
			if ("Start".equalsIgnoreCase(direction)) {
				var temp = cellData.substring(position - 1);
				if (temp.length() > length)
					temp = temp.substring(0, length);
				return temp;
			} else if ("End".equalsIgnoreCase(direction)) {
				var endTemp = cellData.substring(0, cellData.length() - position + 1);
				if (endTemp.length() > length)
					endTemp = endTemp.substring(endTemp.length() - length);
				return endTemp;
			}
		}

		return cellData;

	}
	
	private void trimOperation(Row row, Column column, JSONObject rule) {
		
		BiFunction<Row, String, Column> columnFunction = (rowData,uuid) -> rowData.getColumns().get(UUID.fromString(uuid));
		
		JSONObject trimOperation = rule.getJSONObject(StringTransformUtil.TRIM);
		if(!trimOperation.isEmpty()) {
			var position = trimOperation.has("position") && !trimOperation.isNull("position") ? trimOperation.getString("position") : "";
			var celldata = "";
			var tempColumn = trimOperation.has(COLUMNS) && !trimOperation.isNull(COLUMNS) ? columnFunction.apply(row, trimOperation.getString(COLUMNS)) : null;
			celldata = Objects.nonNull(tempColumn) && Objects.nonNull(tempColumn.getTransformedValue()) ? tempColumn.getTransformedValue().toString() : "";
			if(this.transformationType.getTransformUtilType().equals(TransformType.DEFAULT_VALUE.getTransformUtilType())) {
				if(Objects.isNull(tempColumn))
					return;
				
				celldata = getTrimmedValue(position, celldata);
				column.setValue(celldata);
			} else {
				if(Objects.isNull(tempColumn))
					celldata = Objects.nonNull(column.getTransformedValue()) ? column.getTransformedValue().toString() : "";
				celldata = getTrimmedValue(position, celldata);
			}
			column.setTransformedValue(celldata);
			column.setTargetValue(celldata);
		}
	}
	
	private String getTrimmedValue(String position, String cellData) {
		if(!ValidationUtil.isHavingValue(cellData)) 
			return cellData;
			
		return switch(position) {
			case "start" -> cellData.stripLeading();
			case "end" -> cellData.stripTrailing();
			case "start_end" ->  cellData.strip();
			case "all" -> cellData.strip().replaceAll("\\s+", " ");
			case "middle" -> {
				var leadingSpaces = cellData.replaceFirst("^(\\s*).*", "$1");
				var trailingSpaces = cellData.replaceFirst(".*?(\\s*)$", "$1");
				var middle = cellData.trim().replaceAll("\\s+", " ");
				yield leadingSpaces+middle+trailingSpaces;
			}
			default -> cellData;
		};
	}

}

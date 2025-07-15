package com.adp.esi.digitech.dvts.processing.transformation.util;

import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;

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
@Service("lengthFormatTransformUtil")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LengthFormatTransformUtil extends AbstractTransformUtil {

	@Override
	public void transform(Row row, Column column, JSONObject rule)throws TransformationException {
		try {
			
			BiFunction<Row, String, Column> columnFunction = (rowData,uuid) -> rowData.getColumns().get(UUID.fromString(uuid));
			if(rule != null && rule.has("length_format") && !rule.isNull("length_format")) {				
				JSONObject format = rule.getJSONObject("length_format");
				if(!format.isEmpty()) {
					int character_length = format.has("target_length_size") && !format.isNull("target_length_size") ? (int) format.get("target_length_size") : 0;
					 var targetFiller = format.optString("target_filler");
					if(this.transformationType.getTransformUtilType().equals(TransformType.DEFAULT_VALUE.getTransformUtilType())) {
						var COLUMNS = "columns";
						var tempColumn = format.has(COLUMNS) && !format.isNull(COLUMNS) ? columnFunction.apply(row, format.getString(COLUMNS)) : null;
						if(Objects.isNull(tempColumn))
							return;
						var celldata = Objects.nonNull(tempColumn.getTransformedValue()) ? tempColumn.getTransformedValue().toString() : "";
						celldata = getTranformedValue(character_length, celldata, targetFiller);
						column.setValue(celldata);
						column.setTransformedValue(celldata);
						column.setTargetValue(celldata);
					} else {
						var celldata = Objects.nonNull(column.getTransformedValue()) ? column.getTransformedValue().toString() : "";
						celldata = getTranformedValue(character_length, celldata, targetFiller);
						column.setTransformedValue(celldata);
						column.setTargetValue(celldata);
					}
				}
			}
							
		}
		catch(Exception e) {
			log.error("LengthFormatTransformUtil - transform(), Failed Data Transformation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"", e.getMessage());
			
			var transformationException = new TransformationException("Transformation Error - " + e.getMessage(), e.getCause());
			transformationException.setRequestContext(requestContext);
			throw transformationException;
		}
	}
	
	private String getTranformedValue(int character_length, String celldata, String targetFiller) {
		if(!ValidationUtil.isHavingValue(celldata) || celldata.length() >= character_length) 
			return celldata;			
			
		return String.valueOf(targetFiller).repeat(character_length - celldata.length())+celldata;
	}
	
}

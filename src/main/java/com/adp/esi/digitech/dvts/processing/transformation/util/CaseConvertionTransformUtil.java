package com.adp.esi.digitech.dvts.processing.transformation.util;

import java.util.Date;
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
@Service("caseConvertionTransformUtil")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CaseConvertionTransformUtil extends AbstractTransformUtil {

	@Override
	public void transform(Row row, Column column, JSONObject rule) throws TransformationException {
			try {
				
				BiFunction<Row, String, Column> columnFunction = (rowData,uuid) -> rowData.getColumns().get(UUID.fromString(uuid));
				
				if(rule != null && rule.has("Case_Conversion") && !rule.isNull("Case_Conversion")) {	
					JSONObject format = rule.getJSONObject("Case_Conversion");
					if(!format.isEmpty()) {
						var tCase = format.has("Case") && !format.isNull("Case") ? format.getString("Case") : "";
						if(this.transformationType.getTransformUtilType().equals(TransformType.DEFAULT_VALUE.getTransformUtilType())) {
							var COLUMNS = "columns";
							var tempColumn = format.has(COLUMNS) && !format.isNull(COLUMNS) ? columnFunction.apply(row, format.getString(COLUMNS)) : null;
							if(Objects.isNull(tempColumn))
								return;
							
							var celldata = Objects.nonNull(tempColumn.getTransformedValue()) ? tempColumn.getTransformedValue().toString() : "";
							celldata = getTranformedValue(tCase, celldata);
							column.setValue(celldata);
							column.setTransformedValue(celldata);
							column.setTargetValue(celldata);
						} else {
							var celldata = Objects.nonNull(column.getTransformedValue()) ? column.getTransformedValue() instanceof Date ? column.getTargetValue() : column.getTransformedValue().toString() : "";
							celldata = getTranformedValue(tCase, celldata);
							column.setTransformedValue(celldata);
							column.setTargetValue(celldata);
						}
					}
				}				
					
				
			} catch (Exception e) {
				log.error("CaseConvertionTransformUtil - transform(), Failed Data Transformation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"", e.getMessage());
				
				var transformationException = new TransformationException("Transformation Error - " + e.getMessage(), e.getCause());
				transformationException.setRequestContext(requestContext);
				throw transformationException;
				
			}
		
	}
	
	private String getTranformedValue(String tCase, String celldata) {
		if(!ValidationUtil.isHavingValue(celldata)) 
			return celldata;
			
		return tCase.equalsIgnoreCase("UpperCase") ? celldata.toUpperCase() : tCase.equalsIgnoreCase("LowerCase")? celldata.toLowerCase() : tCase.equalsIgnoreCase("TitleCase")? toTitleCase(celldata) : celldata;
	}
	
	private static String toTitleCase(String input) {
		if(!ValidationUtil.isHavingValue(input)) 
			return input;
		
        StringBuilder titleCase = new StringBuilder(input.length());
        boolean nextTitleCase = true;

        for (char c : input.toCharArray()) {
            if (Character.isSpaceChar(c)) {
                nextTitleCase = true;
            } else if (nextTitleCase) {
                c = Character.toTitleCase(c);
                nextTitleCase = false;
            }
            
            titleCase.append(c);
        }

        return titleCase.toString();
    } 
}

package com.adp.esi.digitech.dvts.processing.transformation.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

import org.json.JSONObject;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("dateTransformUtil")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DateTransformUtil extends AbstractTransformUtil {

	@Override
	public void transform(Row row, Column column, JSONObject rule)throws TransformationException {
		try {
			
			if (rule != null)
				if (rule.has("date_format") && !rule.isNull("date_format"))
					format(column, rule);
				else if (rule.has("currentdate") && !rule.isNull("currentdate"))
					now(column, rule);
				else if (rule.has("current_time") && !rule.isNull("current_time"))
					nowTime(column, rule);
				  
			
		} catch(Exception e) {
			log.error("DateTransformUtil - transform(), Failed Data Transformation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"", e.getMessage());
			
			var transformationException = new TransformationException("Transformation Error - " + e.getMessage(), e.getCause());
			transformationException.setRequestContext(requestContext);
			throw transformationException;
		}
	}

	private void format(Column column, JSONObject rule) throws ParseException {
		var data = column.getTransformedValue();
		JSONObject format = rule.getJSONObject("date_format");
		SimpleDateFormat targetDateFormat = new SimpleDateFormat(format.getString("target_date_format"));
		if(data != null && data instanceof Date) {
			column.setTargetValue(targetDateFormat.format(data));
		} else {			
			SimpleDateFormat sourceDateFormat = new SimpleDateFormat(format.getString("source_date_format"));
			String celldata = (String) column.getTransformedValue();
			
			if (ValidationUtil.isHavingValue(celldata)) {
				column.setValue(sourceDateFormat.parse(celldata));
				
				column.setTargetValue(targetDateFormat.format(sourceDateFormat.parse(celldata)));
			}
		}
	}

	private void now(Column column, JSONObject rule) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(rule.getString("currentdate"));
		var date = new Date();
		column.setValue(date);
		String dateStr = simpleDateFormat.format(date);
		column.setTargetValue(dateStr);
	}
	
	private void nowTime(Column column, JSONObject rule) {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(rule.getString("current_time"));
		var date = new Date();
		column.setValue(date);
		String dateStr = simpleDateFormat.format(date);
		column.setTargetValue(dateStr);
	}
	
}
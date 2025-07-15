package com.adp.esi.digitech.dvts.processing.transformation.util;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;

import org.apache.commons.lang3.math.NumberUtils;
import org.json.JSONObject;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import com.adp.esi.digitech.dvts.processing.ds.config.model.ArithmeticOperation;
import com.adp.esi.digitech.dvts.processing.ds.config.model.Operation;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("dateArithmeticTransformUtil")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DateArithmeticTransformUtil extends AbstractTransformUtil {

	@Override
	public void transform(Row row, Column column, JSONObject rule) throws TransformationException {
		try {
			
			if(!(column.getValue() instanceof Date)) {
				var transformationException = new TransformationException("Transformation Error - Require Date type for transformation");
				transformationException.setRequestContext(requestContext);
				throw transformationException;
			}
			var operationsRule = rule.get("arithmetic_operations");	
			var ruleObj = objectMapper.readValue(operationsRule.toString(), ArithmeticOperation.class);
			transform(row, column, ruleObj);

			
		} catch (TransformationException e) {
			log.error("DateArithmeticTransformUtil - transform(), Failed Data Transformation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"", e.getMessage());
			throw e;

		} catch (Exception e) {
			log.error("DateArithmeticTransformUtil - transform(), Failed Data Transformation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"", e.getMessage());
			var transformationException = new TransformationException("Transformation Error - " + e.getMessage(), e.getCause());
			transformationException.setRequestContext(requestContext);
			throw transformationException;

		}

	}

	public void transform(Row row, Column column, ArithmeticOperation rule) throws TransformationException {
		if(!(column.getValue() instanceof Date))
			return;
		
		Date date = (Date) column.getValue();

		if (Objects.nonNull(date) && !CollectionUtils.isEmpty(rule.getOperations())) {
			LocalDate localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			
		var result = rule.getOperations().stream()
					.reduce(localDate,(currentDate, operation) -> applyOperation(row, column, currentDate, operation), (currentDate, appliedDate) -> appliedDate);
			
			column.setTransformedValue(Date.from(result.atStartOfDay(ZoneId.systemDefault()).toInstant()));
		}
	}

	private LocalDate applyOperation(Row row, Column column, LocalDate localDate, Operation operation) {
		var days = getDays(row, operation);
		var operator = operation.getOperator();
		var exclude = operation.getExclude();

		if (days == null || days == 0L) {
			logError("No data configurations found for days", column, row);
			throwTransformationException("Transformation Error -  No data configurations found for days", null);
		}

		if (!ValidationUtil.isHavingValue(operator)) {
			logError("No ADDITION/SUBTRACTION configurations found for operator", column, row);
			throwTransformationException("Transformation Error - No ADDITION/SUBTRACTION configurations found for operator", null);
		}

		return switch (operator.toUpperCase()) {
		case "ADDITION" -> applyExclusion(localDate, days, exclude, true);
		case "SUBTRACTION" -> applyExclusion(localDate, days, exclude, false);
		default -> localDate;
		};

	}
	
	private Long getDays(Row row, Operation operation) {
		return ValidationUtil.isHavingValue(operation.getValue()) ? Long.valueOf(operation.getValue()) : getDays().apply(row, operation);
	}
	
	private BiFunction<Row, Operation, Long> getDays() {
		return (row, operation) -> {
			var result = 0L;
			var uuidStr = operation.getColumns();
			if (!ValidationUtil.isHavingValue(uuidStr)) {
				return result;
			}					
			var uuid = UUID.fromString(uuidStr);
			
			if (row.getColumns().containsKey(uuid)) {
				var configColumn = row.getColumns().get(uuid);
				var value = configColumn.getValue();
				if (Objects.nonNull(value) && NumberUtils.isParsable(value.toString())) {
					result = Long.parseLong(value.toString());
				}
			}
			return result;
		};
	}	
	
	private LocalDate applyExclusion(LocalDate localDate, long days, String exclude, boolean isAddition) {
		if (!ValidationUtil.isHavingValue(exclude))
			return isAddition ? localDate.plusDays(days) : localDate.minusDays(days);
		int addedDays = 0;
		while (addedDays < days) {
			localDate = isAddition ? localDate.plusDays(NumberUtils.INTEGER_ONE) : localDate.minusDays(NumberUtils.INTEGER_ONE);
			var dayOfWeek = localDate.getDayOfWeek();
			
			if(!isDayToExclude(dayOfWeek, exclude))
				addedDays++;
		}
		return localDate;
	}

	private boolean isDayToExclude(DayOfWeek dayOfWeek, String exclude) {
		return switch (exclude) {
		case "SATURDAY ONLY" -> dayOfWeek == DayOfWeek.SATURDAY;
		case "SUNDAY ONLY" -> dayOfWeek == DayOfWeek.SUNDAY;
		case "SATURDAY & SUNDAY" -> dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY;
		default -> true;
		};
	}

	private void throwTransformationException(String message, Throwable cause) throws TransformationException {
		var transformationException = new TransformationException(message, cause);
		transformationException.setRequestContext(requestContext);
		throw transformationException;
	}

	private void logError(String message, Column column, Row row) {
		log.error("DateArithmeticTransformUtil - transform(), Failed Data Transformation requestUuid = {}, column = {}, message = {}", requestContext.getRequestUuid(), Objects.nonNull(column)? column.getName():"", message);
	}
}

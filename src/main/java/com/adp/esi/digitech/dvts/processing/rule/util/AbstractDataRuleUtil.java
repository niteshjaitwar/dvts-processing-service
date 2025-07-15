package com.adp.esi.digitech.dvts.processing.rule.util;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.apache.commons.lang3.math.NumberUtils;

import com.adp.esi.digitech.dvts.processing.ds.config.model.Condition;
import com.adp.esi.digitech.dvts.processing.enums.OperatorType;
import com.adp.esi.digitech.dvts.processing.model.RequestContext;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;

public abstract class AbstractDataRuleUtil<T,R> implements IDataRuleUtil<T,R> {

	Map<String, String> dynamicValues;
	
	RequestContext requestContext;	
	
	String datasetName;
	
	public void setRequestContext(RequestContext requestContext) {
		this.requestContext = requestContext;
	}

	public void setDynamicValues(Map<String, String> dynamicValues) {
		this.dynamicValues = dynamicValues;
	}
	
	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}

	public Predicate<Row> constructPredicate(Condition condition) {
		BiFunction<Row, UUID, Object> rowFun = (row, uuid) -> row.getColumns().get(uuid).getValue();
		var operator = condition.getOperator();
		operator = operator.startsWith("<") && operator.endsWith(">") ? operator.substring(1, operator.length()-1).toUpperCase() : operator;

		UUID columnUUId = UUID.fromString(condition.getColumn());

		switch (OperatorType.valueOf(operator)) {
		case NOTNULL:
			return row -> {
				Object rowValue = rowFun.apply(row, columnUUId);
				return Objects.nonNull(rowValue);
			};
		case NULL:
			return row -> {
				Object rowValue = rowFun.apply(row, columnUUId);
				return Objects.isNull(rowValue);
			};
		case Begins:
			return row -> {
				Object rowValue = rowFun.apply(row, columnUUId);
				Object conditionValue = getValue(row, condition);
				return (Objects.nonNull(rowValue) && Objects.nonNull(conditionValue))
						? String.valueOf(rowValue).startsWith(String.valueOf(conditionValue)) : false;
			};
		case Ends:
			return row -> {
				Object rowValue = rowFun.apply(row, columnUUId);
				Object conditionValue = getValue(row, condition);
				return (Objects.nonNull(rowValue) && Objects.nonNull(conditionValue))
						? ValidationUtil.isHavingValue((String) rowValue)
						&& String.valueOf(rowValue).endsWith(String.valueOf(conditionValue)) : false;
			};
		case Contains:
			return row -> {
				Object rowValue = rowFun.apply(row, columnUUId);
				Object conditionValue = getValue(row, condition);
				return (Objects.nonNull(rowValue) && Objects.nonNull(conditionValue)) 
						? ValidationUtil.isHavingValue((String) rowValue)
						&& String.valueOf(rowValue).contains(String.valueOf(conditionValue)) : false;
			};
		case Equals:
			return row -> {
				Object rowValue = rowFun.apply(row, columnUUId);
				Object conditionValue = getValue(row, condition);				
				return (Objects.nonNull(rowValue) && Objects.nonNull(conditionValue))
						? (rowValue instanceof Date && conditionValue instanceof Date)? ((Date) rowValue).equals((Date) conditionValue) : 
							checkParseble(String.valueOf(rowValue)) && checkParseble(String.valueOf(conditionValue))? 
										Long.parseLong(String.valueOf(rowValue)) == Long.parseLong(String.valueOf(conditionValue)):
								 String.valueOf(rowValue).equals(String.valueOf(conditionValue))
						: false;
			};
		case NotEquals:
			return row -> {
				Object rowValue = rowFun.apply(row, columnUUId);
				Object conditionValue = getValue(row, condition);
				return (Objects.nonNull(rowValue) && Objects.nonNull(conditionValue))
						? ((rowValue instanceof Date && conditionValue instanceof Date)
								? !((Date) rowValue).equals((Date) conditionValue) :
									checkParseble(String.valueOf(rowValue)) && checkParseble(String.valueOf(conditionValue))?
											Long.parseLong(String.valueOf(rowValue)) != Long.parseLong(String.valueOf(conditionValue)):
								 !String.valueOf(rowValue).equals(String.valueOf(conditionValue)))
						: false;
			};
		case Greater:
			return row -> {
				Object rowValue = rowFun.apply(row, columnUUId);
				Object conditionValue = getValue(row, condition);
				return (Objects.nonNull(rowValue) && Objects.nonNull(conditionValue))
						? ((rowValue instanceof Date && conditionValue instanceof Date)
								? ((Date) rowValue).after((Date) conditionValue)
								: (checkParseble(String.valueOf(rowValue)) && checkParseble(String.valueOf(conditionValue))
										? Long.parseLong(String.valueOf(rowValue)) > Long
												.parseLong(String.valueOf(conditionValue))
										: false))
						: false;
			};
		case Lesser:
			return row -> {
				Object rowValue = rowFun.apply(row, columnUUId);
				Object conditionValue = getValue(row, condition);
				return (Objects.nonNull(rowValue) && Objects.nonNull(conditionValue))
						? ((rowValue instanceof Date && conditionValue instanceof Date)
								? ((Date) rowValue).before((Date) conditionValue)
								: (checkParseble(String.valueOf(rowValue)) && checkParseble(String.valueOf(conditionValue))
										? Long.parseLong(String.valueOf(rowValue)) < Long
												.parseLong(String.valueOf(conditionValue))
										: false))
						: false;
			};
		default:
			break;
		}
		return row -> false;
	}
	
	public Predicate<Row> constructPredicate(String operator, List<Predicate<Row>> conditions) {
		if(operator.equalsIgnoreCase("and"))
			return conditions.stream().reduce(x -> true, Predicate::and);
		
		return operator.equalsIgnoreCase("or") ? conditions.stream().reduce(x -> false, Predicate::or) : null;
	}
	
	public Object getValue(Row row, Condition condition) {
		var isDynamic = condition.getDynamic();
		var value = condition.getValue();
		if (isDynamic.equalsIgnoreCase("Y")) {
			return dynamicValues.get(value.substring(2, value.length() - 2));
		} else {
			if (value.startsWith("{{") && value.endsWith("}}")) {
				String columnUUID = value.substring(2, value.length() - 2);
				return row.getColumns().get(UUID.fromString(columnUUID)).getValue();
			}
		}

		return value;
	}
	
	public static boolean checkParseble(String value) {
		return ValidationUtil.isHavingValue(value) && NumberUtils.isParsable(value);
	}
}

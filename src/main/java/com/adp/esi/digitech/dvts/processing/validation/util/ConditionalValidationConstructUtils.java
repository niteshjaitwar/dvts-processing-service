package com.adp.esi.digitech.dvts.processing.validation.util;

import java.util.Date;
import java.util.OptionalInt;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.math3.util.Precision;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;
import com.adp.esi.digitech.dvts.processing.ds.model.ValidationRule;
import com.adp.esi.digitech.dvts.processing.enums.OperatorType;
import com.adp.esi.digitech.dvts.processing.exception.ProcessException;
import com.adp.esi.digitech.dvts.processing.model.Column;
import com.adp.esi.digitech.dvts.processing.model.Row;

@Service("conditionalValidationConstructUtils")
public class ConditionalValidationConstructUtils{
	
	@Autowired
	ObjectProvider<Column> columnObjProvider;
	
	public ValidationRule construct(Row row, Column column, JSONObject conditionalValidationJson) {
		var OPERATOR = "operator";
		ValidationRule defaultValue = new ValidationRule();
		try {
			if (ValidationUtil.isHavingValue(conditionalValidationJson.toString())
					&& conditionalValidationJson.has("conditions") && !conditionalValidationJson.isNull("conditions")) {
				JSONArray conditionalJsonArray = conditionalValidationJson.getJSONArray("conditions");

				OptionalInt selectedIndex = IntStream.range(0, conditionalJsonArray.length())
						.filter(index -> {
							JSONObject conditionalJson = conditionalJsonArray.getJSONObject(index);
							var operator = conditionalJson.has(OPERATOR) && !conditionalJson.isNull(OPERATOR)
									? conditionalJson.getString(OPERATOR)
									: "And";
							if (conditionalJson.has("if") && !conditionalJson.isNull("if"))
								return checkIfConditions(row, conditionalJson.getJSONArray("if"), operator);
							return false;
						}).findFirst();
				if (selectedIndex.isPresent()) {
					int index = selectedIndex.getAsInt();
					JSONObject selectedConditional = conditionalJsonArray.getJSONObject(index);
					JSONObject thenRule = selectedConditional.getJSONObject("then");
					defaultValue = construct(row, column, thenRule, defaultValue);
					return defaultValue;
				}
			}
		} catch (Exception e) {
			var processException = new ProcessException("Processing Error - " + e.getMessage(), e.getCause());
			throw processException;
		}
		return defaultValue;
	}
	
	private ValidationRule construct(Row row, Column column, JSONObject rule, ValidationRule defaultValue) {
		if (rule.has("validations") && !rule.isNull("validations")) {
			JSONArray conditionalJsonArray = rule.getJSONArray("validations");
			JSONObject stringCheckRules = new JSONObject();
			IntStream.range(0, conditionalJsonArray.length()).parallel().forEach(index -> {
				JSONObject validationJson = conditionalJsonArray.getJSONObject(index);
				var operation = validationJson.getString("operation");
				var tempOperation = operation.startsWith("<") && operation.endsWith(">") ? operation.substring(1, operation.length()-1).toUpperCase() : operation;
				switch (OperatorType.valueOf(tempOperation)) {
				case MinLength:
					defaultValue.setMinLengthAllowed(validationJson.getString("rhs"));
					break;
				case MaxLength:
					defaultValue.setMaxLengthAllowed(validationJson.getString("rhs"));
					break;
				case MinValue:
					defaultValue.setMinValue(validationJson.getString("rhs"));
					break;
				case MaxValue:
					defaultValue.setMaxValue(validationJson.getString("rhs"));
					break;
				case Mandatory:
					defaultValue.setIsMandatory(validationJson.getString("rhs"));
					break;
				case NOTNULL:
					defaultValue.setIsMandatory("Y");
					break;
				case SpecialCharNotAllowed:
					var rhs = validationJson.getJSONArray("rhs").toString();
					defaultValue.setSpecialCharNotAllowed(rhs);
					break;	
				case Begins:
				case Ends:
				case Contains:
				case GreaterThan:
				case LessThan:
				case NULL:
				case Equals:
				case NotEquals:
				case DoesNotContain:
				case Regex:
					stringCheckRules.put(operation, validationJson.getString("rhs"));
					break;
				default:
					break;

				}
			});

			if (!stringCheckRules.isEmpty())
				defaultValue.setStringCheckRule(stringCheckRules.toString());

		}
		return defaultValue;
	}
	
	
	private boolean checkIfConditions(Row row, JSONArray ifConditions, String operator) {
		if ("And".equalsIgnoreCase(operator)) 
			return IntStream.range(0, ifConditions.length()).parallel().allMatch(index -> {
				return checkIfConditions(row, ifConditions, index);
			});
		
			
		return IntStream.range(0, ifConditions.length()).parallel().anyMatch(index -> {
			return checkIfConditions(row, ifConditions, index);

		});
		
	}
	
	private Column InitiateColumn(Row row, String value) {
        return value.startsWith("{{") && value.endsWith("}}")
				? row.getColumns().get(UUID.fromString(value.substring(2, value.length() - 2)))
				: columnObjProvider.getObject(null,value,null,null);
	}
	
	private boolean checkIfConditions(Row row, JSONArray ifConditions, int index) {
		JSONObject ifCondition = ifConditions.getJSONObject(index);
		if (ifCondition != null) {
			String constraint = ifCondition.getString("constraint");			
			String lhsVal = ifCondition.getString("lhs");
			Column lhs = InitiateColumn(row, lhsVal);
			constraint = constraint.startsWith("<") && constraint.endsWith(">") ? constraint.substring(1, constraint.length()-1).toUpperCase() : constraint;
			switch (OperatorType.valueOf(constraint)) {
				case In:					
					JSONArray rhsValArray = ifCondition.getJSONArray("rhs");					
					return IntStream.range(0, rhsValArray.length()).parallel().anyMatch(idx -> {
						var rhsVal = rhsValArray.getString(idx);						
						Column rhs = InitiateColumn(row, rhsVal);
						return checkConstraint(lhs, rhs, OperatorType.Equals.getOperatorType());

					});
				default:
					String rhsVal = ifCondition.getString("rhs");
					Column rhs = InitiateColumn(row, rhsVal);
					return checkConstraint(lhs, rhs, constraint);
					
            
			}
		}

		return false;
	}
	
	
	
	public static boolean checkConstraint(Column lhs, Column rhs, String constraint) {
		constraint = constraint.startsWith("<") && constraint.endsWith(">") ? constraint.substring(1, constraint.length()-1).toUpperCase() : constraint;
		switch (OperatorType.valueOf(constraint)) {
		case NOTNULL:
				return lhs.getValue() != null ? (lhs.getValue() instanceof String ? ValidationUtil.isHavingValue((String)lhs.getValue()): true) : false;
		case NULL:
				return lhs.getValue() == null || !ValidationUtil.isHavingValue(lhs.getValue().toString());
		case Equals:
				return (lhs.getValue() != null && rhs.getValue() != null)? ((lhs.getValue() instanceof Date && rhs.getValue() instanceof Date) ? 
						((Date)lhs.getValue()).equals((Date)rhs.getValue()) : 
							NumberUtils.isParsable(lhs.getValue().toString()) && NumberUtils.isParsable(lhs.getValue().toString()) ? 
								Precision.equals(Double.parseDouble(lhs.getValue().toString()), Double.parseDouble(rhs.getValue().toString())) : 
									(lhs.getValue().toString()).equalsIgnoreCase(rhs.getValue().toString())):false;
		case NotEquals:
			return (lhs.getValue() != null && rhs.getValue() != null)? ((lhs.getValue() instanceof Date && rhs.getValue() instanceof Date) ? 
					!((Date)lhs.getValue()).equals((Date)rhs.getValue()) : !(lhs.getValue().toString()).equalsIgnoreCase(rhs.getValue().toString())):false;			
		case GreaterThan:
			return (lhs.getValue() != null && rhs.getValue() != null)? ((lhs.getValue() instanceof Date && rhs.getValue() instanceof Date) ? 
					((Date)lhs.getValue()).after((Date)rhs.getValue()) : 
						NumberUtils.isParsable(lhs.getValue().toString()) && NumberUtils.isParsable(lhs.getValue().toString()) ? 
								Double.parseDouble(lhs.getValue().toString()) > Double.parseDouble(rhs.getValue().toString()) :false):false; 
		case LessThan:
			return (lhs.getValue() != null && rhs.getValue() != null)? ((lhs.getValue() instanceof Date && rhs.getValue() instanceof Date) ? 
					((Date)lhs.getValue()).before((Date)rhs.getValue()) : 
						NumberUtils.isParsable(lhs.getValue().toString()) && NumberUtils.isParsable(lhs.getValue().toString()) ? 
								Double.parseDouble(lhs.getValue().toString()) < Double.parseDouble(rhs.getValue().toString()) :false):false; 
		case Begins:
			return (lhs.getValue() != null && rhs.getValue() != null)? (ValidationUtil.isHavingValue(lhs.getValue().toString()) && ValidationUtil.isHavingValue(rhs.getValue().toString()) && (lhs.getValue().toString()).startsWith(rhs.getValue().toString())) : false;
		case Ends:
			return (lhs.getValue() != null && rhs.getValue() != null)? (ValidationUtil.isHavingValue(lhs.getValue().toString()) && ValidationUtil.isHavingValue(rhs.getValue().toString()) && (lhs.getValue().toString()).endsWith(rhs.getValue().toString())) : false;
		case Contains:
			return (lhs.getValue() != null && rhs.getValue() != null)? (ValidationUtil.isHavingValue(lhs.getValue().toString()) && ValidationUtil.isHavingValue(rhs.getValue().toString()) && (lhs.getValue().toString()).contains(rhs.getValue().toString())) : false;
		case DoesNotContain:
			return (lhs.getValue() != null && rhs.getValue() != null)? (ValidationUtil.isHavingValue(lhs.getValue().toString()) && ValidationUtil.isHavingValue(rhs.getValue().toString()) && !(lhs.getValue().toString()).contains(rhs.getValue().toString())) : false;
		case Regex:
			return ((lhs.getValue() != null && rhs.getValue() != null) && (ValidationUtil.isHavingValue(lhs.getValue().toString()) && ValidationUtil.isHavingValue(rhs.getValue().toString())) ?  Pattern.matches(rhs.getValue().toString(), lhs.getValue().toString()): false);
		default:
			break;
		}
		return false;
	}
}

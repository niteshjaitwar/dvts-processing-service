package com.adp.esi.digitech.dvts.processing.rule.util;

import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.adp.esi.digitech.dvts.processing.ds.config.model.DataFilter;
import com.adp.esi.digitech.dvts.processing.exception.TransformationException;
import com.adp.esi.digitech.dvts.processing.model.Row;
import com.adp.esi.digitech.dvts.processing.util.ValidationUtil;

import lombok.extern.slf4j.Slf4j;

@Service("dataGroupbyFilterRuleUtil")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Slf4j
public class DataGroupbyFilterRuleUtil extends AbstractDataRuleUtil<Predicate<Row>, DataFilter> {

	@Override
	public Predicate<Row> construct(DataFilter dataFilter)  throws TransformationException {
		try {
			//log.info("DataGroupbyFilterRuleUtil - construct(), Started costructing predicate, uniqueId = {}, datasetName = {}",	requestContext.getRequestUuid(), datasetName);
			
			var conditionsList = dataFilter.getConditions().stream().map(condition -> constructPredicate(condition)).collect(Collectors.toList());
			
			var operator = dataFilter.getOperator();
			
			if (conditionsList.size() > 1 && !ValidationUtil.isHavingValue(operator)) {
				var transformationException = new TransformationException("Configuration Error - condition operator is required");
				transformationException.setRequestContext(requestContext);
				throw transformationException;
			}
			
			if(conditionsList.size() == 1)
				return conditionsList.stream().reduce(x -> true, Predicate::and);
			
			//log.info("DataGroupbyFilterRuleUtil - construct(), filter operator, uniqueId = {}, datasetName = {}, operator = {}",requestContext.getRequestUuid(), datasetName, operator);
			
			var predicate = this.constructPredicate(operator, conditionsList);
			
			//log.info("DataGroupbyFilterRuleUtil - construct(), Completed costructing predicate, uniqueId = {}, datasetName = {}", requestContext.getRequestUuid(), datasetName);
			
			return predicate;
		} catch (Exception e) {
			log.error("DataGroupbyFilterRuleUtil - construct()  Failed costructing predicate, UniqueId {}, name = {}, message = {}",
					requestContext.getUniqueId(), datasetName, e.getMessage());
			var transformationException = new TransformationException("Transformation Error - " + e.getMessage(),e.getCause());
			transformationException.setRequestContext(requestContext);
			throw transformationException;
		}
	}
}

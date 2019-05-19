package mn.bplogic.api;

import mn.bplogic.api.expressions.DoubleExpression;
import mn.bplogic.api.expressions.DoubleFetchValue;
import mn.bplogic.api.expressions.FetchExpression;
import mn.bplogic.api.expressions.IntExpression;
import mn.bplogic.api.expressions.IntFetchValue;
import mn.bplogic.api.expressions.NumberExpression;
import mn.bplogic.rowsources.BPRow;
import mn.bplogic.rowsources.BPRowSource;

public class PriorityExpressionAggregator implements AggregateConverter {
	final private String[]         intFieldLabels;
	final private String[]         doubleFieldLabels;
	final private int[]            intTypes;
	final private NumberExpression priorityExpression;
	final private int              implementationMethod;
	final private int              swapArgument;

	final private static int       METHOD_INT_FETCH       = 0;
	final private static int       METHOD_DOUBLE_FETCH    = 1;
	final private static int       METHOD_INT_EVALUATE    = 2;
	final private static int       METHOD_DOUBLE_EVALUATE = 3;

	public PriorityExpressionAggregator(BPRowSource input, String priorityExpression){
		this(input, priorityExpression, true);
	}
	public PriorityExpressionAggregator(BPRowSource input, String priorityExpression, boolean highNumberIsHighPrio){
		this(input, FetchExpression.getInstance(priorityExpression, input), highNumberIsHighPrio);
	}
	public PriorityExpressionAggregator(BPRowSource input, NumberExpression priorityExpression){
		this(input, priorityExpression, true);
	}
	public PriorityExpressionAggregator(BPRowSource input, NumberExpression priorityExpression, boolean highNumberIsHighPrio){
		this.priorityExpression = priorityExpression;
		this.intFieldLabels     = input.getIntFieldLabels();
		this.doubleFieldLabels  = input.getDoubleFieldLabels();
		this.intTypes           = input.getIntTypes();

		int tempImplementationMethod;
		if (priorityExpression instanceof IntFetchValue){
			tempImplementationMethod = METHOD_INT_FETCH;
		} else if (priorityExpression instanceof DoubleFetchValue){
			tempImplementationMethod = METHOD_DOUBLE_FETCH;
		} else if (priorityExpression instanceof IntExpression){
			tempImplementationMethod = METHOD_INT_EVALUATE;
		} else if (priorityExpression instanceof DoubleExpression){
			tempImplementationMethod = METHOD_DOUBLE_EVALUATE;
		} else {
			throw new RuntimeException("Unknown implementation of NumberExpression");
		}
		this.implementationMethod = tempImplementationMethod;
		this.swapArgument = highNumberIsHighPrio ? -1 : 1;

	}
	public String[] getIntFieldLabels() {
		return intFieldLabels;
	}

	public String[] getDoubleFieldLabels() {
		return doubleFieldLabels;
	}

	public int[] getIntTypes() {
		return intTypes;
	}

	public BPRow convert(BPRow[] inputs) {
		int      index = 0;
		int      iV;
		int      col;
		double   dV;
		int[]    tmpI;
		double[] tmpD;

		switch(implementationMethod){
		case METHOD_INT_FETCH:
			col = ((IntFetchValue) priorityExpression).getIndex();
			iV = swapArgument * inputs[0].intValues[col];
			for (int i=1; i<inputs.length; i++){
				if (swapArgument * inputs[i].intValues[col] < iV){
					index = i;
					iV = swapArgument * inputs[i].intValues[col];
				}
			}
			break;
		case METHOD_DOUBLE_FETCH:
			col = ((DoubleFetchValue) priorityExpression).getIndex();
			dV = swapArgument * inputs[0].doubleValues[col];
			for (int i=1; i<inputs.length; i++){
				if (swapArgument * inputs[i].doubleValues[col] < dV){
					index = i;
					dV = swapArgument * inputs[i].doubleValues[col];
				}
			}
			break;
		case METHOD_INT_EVALUATE:
			tmpI = ((IntExpression) priorityExpression).evaluate(inputs);
			iV = swapArgument * tmpI [0];
			for (int i=1; i<inputs.length; i++){
				if (swapArgument * tmpI[i] < iV){
					index = i;
					iV = swapArgument * tmpI[i];
				}
			}
			break;
		case METHOD_DOUBLE_EVALUATE:
			tmpD = ((DoubleExpression) priorityExpression).evaluate(inputs);
			dV = swapArgument * tmpD[0];
			for (int i=1; i<inputs.length; i++){
				if (swapArgument * tmpD[i] < dV){
					index = i;
					dV = swapArgument * tmpD[i];
				}
			}
			break;
		}
		return inputs[index].clone();
	}

}

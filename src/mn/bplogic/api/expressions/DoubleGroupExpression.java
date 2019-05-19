package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public abstract class DoubleGroupExpression implements DoubleExpression{
	public double[] evaluate(BPRow input[]){
		return new double[]{evaluateGroup(input)};
	}
	public abstract double evaluateGroup(BPRow input[]);
}

package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public interface DoubleExpression extends NumberExpression{
	public double[] evaluate(BPRow input[]);
}
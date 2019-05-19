package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public interface IntExpression extends NumberExpression{
	public int getIntType();
	public int[] evaluate(BPRow input[]);
}

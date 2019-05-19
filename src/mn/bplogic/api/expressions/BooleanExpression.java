package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public interface BooleanExpression {
	public boolean[] evaluate(BPRow input[]);
}

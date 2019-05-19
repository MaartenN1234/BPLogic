package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public abstract class IntGroupExpression implements IntExpression{
	public int[] evaluate(BPRow input[]){
		return new int[]{evaluateGroup(input)};
	}
	public abstract int evaluateGroup(BPRow input[]);
}

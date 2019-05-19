package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public class FALSE implements BooleanExpression {
	final public static FALSE STATIC = new FALSE();

	public boolean[] evaluate(BPRow[] inputs) {
		return FALSE.staticVal(inputs.length);
	}
	public static boolean[] staticVal(int size) {
		boolean [] staticVals = new boolean[size];
		return staticVals;
	}
}

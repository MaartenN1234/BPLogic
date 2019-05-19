package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public class TRUE implements BooleanExpression {
	final public static TRUE STATIC = new TRUE();

	public boolean[] evaluate(BPRow[] inputs) {
		return TRUE.staticVal(inputs.length);
	}

	public static boolean[] staticVal(int size) {
		boolean [] staticVals = new boolean[size];
		for (int i=0; i<size; i++){
			staticVals[i] = true;
		}
		return staticVals;
	}
}

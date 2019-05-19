package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;
import mn.bplogic.rowsources.BPRowSource;

public class IntSum implements IntExpression {
	private IntExpression    [] neIA;

	public IntSum (IntExpression [] neA){
		this.neIA = neA;
	}


	public int[] evaluate(BPRow[] inputs) {
		int size = inputs.length;
		int [] output = new int[size];
		for (int i=0; i <size; i++){
			output[i] = 0;
		}
		for (IntExpression ie  : neIA){
			int [] tmp = ie.evaluate(inputs);
			for (int i=0; i < size; i++){
				output[i] += tmp[i];
			}
		}
		return output;
	}

	public int getIntType() {
		return BPRowSource.intType;
	}
}

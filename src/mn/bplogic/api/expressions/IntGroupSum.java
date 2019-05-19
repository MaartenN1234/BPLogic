package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;
import mn.bplogic.rowsources.BPRowSource;

public class IntGroupSum extends IntGroupExpression {
	private IntExpression    [] neIA;

	public IntGroupSum (IntExpression [] neA){
		this.neIA = neA;
	}


	public int evaluateGroup(BPRow[] inputs) {
		int output = 0;
		for (IntExpression ie  : neIA){
			for (int i : ie.evaluate(inputs)){
				output += i;
			}
		}
		return output;
	}

	public int getIntType() {
		return BPRowSource.intType;
	}
}

package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;
import mn.bplogic.rowsources.BPRowSource;

public class IntGroupCount implements IntExpression {
	public IntGroupCount (){
	}


	public int[] evaluate(BPRow[] inputs) {
		return new int[]{inputs.length};
	}

	public int getIntType() {
		return BPRowSource.intType;
	}
}

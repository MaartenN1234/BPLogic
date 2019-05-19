package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public class IntGroupFirst extends IntGroupExpression {
	private IntExpression     ne;

	public IntGroupFirst (IntExpression ne){
		this.ne = ne;
	}
	public IntGroupFirst (IntExpression [] neA){
		this(neA[0]);
	}

	public int evaluateGroup(BPRow[] inputs) {
		for (int i : ne.evaluate(inputs))
			return i;

		return 0;
	}

	public int getIntType() {
		return ne.getIntType();
	}
}

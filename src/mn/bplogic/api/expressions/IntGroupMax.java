package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public class IntGroupMax  extends IntGroupExpression  {
	private IntExpression    [] neIA;

	public IntGroupMax (IntExpression ne){
		this(new IntExpression[]{ne});
	}
	public IntGroupMax (IntExpression [] neA){
		this.neIA = neA;
	}

	public int evaluateGroup(BPRow[] inputs) {
		int output = Integer.MIN_VALUE;
		for (IntExpression ie  : neIA)
			for (int i : ie.evaluate(inputs))
				if (i>output)
					output = i;

		return output;
	}

	public int getIntType() {
		return neIA[0].getIntType();
	}
}

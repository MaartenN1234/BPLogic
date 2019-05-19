package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public class AND implements BooleanExpression {
	final private BooleanExpression [] bes;
	final private boolean isStatic;
	final private boolean staticValue;

	public AND (BooleanExpression [] bes){
		this.bes = bes;
		if (bes.length == 0){
			throw new IllegalArgumentException("AND Needs at least one source BooleanExpression");
		}
		boolean allStaticTrue  = true;
		boolean oneStaticFalse = false;
		BPRow[] staticTestDummy = new BPRow[1];
		boolean[] staticTestOutput;
		for (BooleanExpression be : bes){
			try{
				staticTestOutput = be.evaluate(staticTestDummy);
				if(!staticTestOutput[0]){
					oneStaticFalse = true;
					allStaticTrue  = false;
				}
			}catch (NullPointerException e){
				// Needs BPRow, therefore not-static
				allStaticTrue = false;
			}
		}

		isStatic    = oneStaticFalse || allStaticTrue;
		staticValue = allStaticTrue;
	}

	public boolean[] evaluate(BPRow[] inputs) {
		int size = inputs.length;
		if (isStatic)
			return STATICBOOL.staticVal(size, staticValue);

		boolean [] output = bes[0].evaluate(inputs);
		for (int i = 1; i< bes.length; i++){
			boolean [] tmp   = bes[i].evaluate(inputs);
			for (int j=0; j< size; j++)
				output[j] = output[j] && tmp[j];
		}
		return output;
	}

	public boolean isStatic() {
		return isStatic;
	}
}

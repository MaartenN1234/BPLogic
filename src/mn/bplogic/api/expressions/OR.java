package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public class OR implements BooleanExpression {
	final private BooleanExpression [] bes;
	final private boolean isStatic;
	final private boolean staticValue;

	public OR (BooleanExpression [] bes){
		this.bes = bes;
		if (bes.length == 0){
			throw new IllegalArgumentException("OR Needs at least one source BooleanExpression");
		}
		boolean allStaticFalse = true;
		boolean oneStaticTrue  = false;
		BPRow[] staticTestDummy = new BPRow[1];
		boolean[] staticTestOutput;

		for (BooleanExpression be : bes){
			try{
				staticTestOutput = be.evaluate(staticTestDummy);
				if(staticTestOutput[0]){
					oneStaticTrue  = true;
					allStaticFalse = false;
				}
			}catch (NullPointerException e){
				// Needs BPRow, therefore not-static
				allStaticFalse = false;
			}
		}

		isStatic   = allStaticFalse || oneStaticTrue;
		staticValue = oneStaticTrue;
	}

	public boolean isStatic(){
		return isStatic;
	}


	public boolean [] evaluate(BPRow[] inputs) {
		int size = inputs.length;
		if (isStatic)
			return STATICBOOL.staticVal(size, staticValue);

		boolean [] output = bes[0].evaluate(inputs);
		for (int i = 1; i< bes.length; i++){
			boolean [] tmp   = bes[i].evaluate(inputs);
			for (int j=0; j< size; j++)
				output[j] = output[j] || tmp[j];
		}
		return output;
	}
}

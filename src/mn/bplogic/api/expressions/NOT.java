package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public class NOT implements BooleanExpression {
	final private BooleanExpression be;
	final private boolean isStatic;
	final private boolean staticValue;

	public NOT (BooleanExpression be){
		this.be  = be;

		boolean isStaticTmp;
		boolean staticValueTmp = false;
		BPRow[] staticTestDummy = new BPRow[1];
		boolean[] staticTestOutput;
		try{
			staticTestOutput = be.evaluate(staticTestDummy);
			isStaticTmp = true;
			if (!staticTestOutput[0])
				staticValueTmp = true;
		}catch (NullPointerException e){
			// Needs BPRow, therefore not-static
			isStaticTmp = false;
		}
		isStatic = isStaticTmp;
		staticValue = staticValueTmp;
	}


	public boolean[] evaluate(BPRow[] inputs) {
		int size = inputs.length;
		if (isStatic)
			return STATICBOOL.staticVal(size, staticValue);

		boolean [] tmp    = be.evaluate(inputs);
		for (int i=0; i< size; i++)
			tmp[i] = !tmp[i];

		return tmp;
	}
}

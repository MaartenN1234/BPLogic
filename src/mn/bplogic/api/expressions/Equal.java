package mn.bplogic.api.expressions;

import mn.bplogic.rowsources.BPRow;

public class Equal implements BooleanExpression {
	private BooleanExpression [] bes;
	private IntExpression     [] ies;
	private DoubleExpression  [] des;
	private boolean              unEqualFlip;
	private int                  source;

	public Equal(BooleanExpression [] bes){
		this(bes, false);
	}
	public Equal(IntExpression [] bes){
		this(bes, false);
	}
	public Equal(DoubleExpression [] bes){
		this(bes, false);
	}
	public Equal(BooleanExpression [] bes, boolean unEqual){
		unEqualFlip = unEqual;
		if (bes.length < 2){
			source = -1;
		} else {
			this.bes = bes;
			source   = 0;
		}
	}
	public Equal(IntExpression [] ies, boolean unEqual){
		unEqualFlip = unEqual;
		if (ies.length < 2){
			source = -1;
		} else {
			this.ies = ies;
			source   = 1;
		}
	}
	public Equal(DoubleExpression [] des, boolean unEqual){
		unEqualFlip = unEqual;
		if (des.length < 2){
			source = -1;
		} else {
			this.des = des;
			source   = 2;
		}
	}


	public boolean[] evaluate(BPRow[] input) {
		int size = input.length;
		boolean [] output;
		switch(source){
		case -1:
			if (unEqualFlip){
				return STATICBOOL.staticVal(size, false);
			} else {
				return STATICBOOL.staticVal(size, true);
			}

		case 0:
			boolean [] refB = bes[0].evaluate(input);
			output = STATICBOOL.staticVal(size, true);
			for (int i=1; i< bes.length; i++){
				boolean tmp [] = bes[i].evaluate(input);
				for (int j=0; j< size; j++)
					if(refB[j] != tmp[j])
						output[j] = false;
			}
			if(unEqualFlip)
				for (int j=0; j< size; j++)
					output[j] = !output[j];
			return output;
		case 1:
			int [] refI = ies[0].evaluate(input);
			output = STATICBOOL.staticVal(size, true);
			for (int i=1; i< ies.length; i++){
				int tmp [] = ies[i].evaluate(input);
				for (int j=0; j< size; j++)
					if(refI[j] != tmp[j])
						output[j] = false;
			}
			if(unEqualFlip)
				for (int j=0; j< size; j++)
					output[j] = !output[j];
			return output;
		case 2:
			double [] refD = des[0].evaluate(input);
			output = STATICBOOL.staticVal(size, true);
			for (int i=1; i< des.length; i++){
				double tmp [] = des[i].evaluate(input);
				for (int j=0; j< size; j++)
					if(refD[j] != tmp[j])
						output[j] = false;
			}
			if(unEqualFlip)
				for (int j=0; j< size; j++)
					output[j] = !output[j];
			return output;
		}
		return null;
	}

}

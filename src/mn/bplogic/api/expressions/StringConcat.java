package mn.bplogic.api.expressions;

import mn.bplogic.main.TranslateMap;
import mn.bplogic.rowsources.BPRow;
import mn.bplogic.rowsources.BPRowSource;

public class StringConcat implements IntExpression {
	final private IntExpression    [] neIA;
	final private int expCount;
	static private TranslateMap<String> translateMapString = TranslateMap.getStringMap();

	public StringConcat (IntExpression ne1, IntExpression ne2){
		this(new IntExpression []{ne1,ne2});
	}
	public StringConcat (IntExpression [] neA){
		this.neIA = neA;
		expCount  = neA.length;
	}


	public int[] evaluate(BPRow [] inputs) {
		int size = inputs.length;
		if (size ==0) return new int[0];

		if (expCount > 2){
			int [] output = new int[size];
			StringBuffer [] sbs = new StringBuffer [size];
			for (int i=0; i <size; i++){
				sbs[i] = new StringBuffer();
			}
			for (IntExpression ie  : neIA){
				int [] tmp = ie.evaluate(inputs);
				for (int i=0; i <size; i++){
					sbs[i].append(translateMapString.translate(tmp[i]));
				}
			}
			for (int i=0; i <size; i++){
				output[i] = translateMapString.translate(sbs[i].toString());
			}

			return output;
		} else if (expCount == 1){
			return neIA[0].evaluate(inputs);
		}

		int [] sl = neIA[0].evaluate(inputs);
		int [] sr = neIA[1].evaluate(inputs);
		int bufferInL = sl[0];
		int bufferInR = sr[0];
		int lastOut   = translateMapString.translate(translateMapString.translate(bufferInL)+translateMapString.translate(bufferInR));
		sl[0] = lastOut;
		for (int i = 1; i< size; i++){
			if (sl[i] != bufferInL || sr[i] != bufferInR){
				bufferInL = sl[i];
				bufferInR = sr[i];
				lastOut = translateMapString.translate(translateMapString.translate(bufferInL)+translateMapString.translate(bufferInR));
			}
			sl[i] = lastOut;
		}
		return sl;
	}
	public int getIntType() {
		return BPRowSource.stringType;
	}
}

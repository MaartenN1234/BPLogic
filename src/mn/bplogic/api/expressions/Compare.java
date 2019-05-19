package mn.bplogic.api.expressions;

import mn.bplogic.main.TranslateMap;
import mn.bplogic.rowsources.BPRow;

public class Compare implements BooleanExpression {
	public final static int EQUAL                       = 0;
	public final static int UNEQUAL                     = 1;
	public final static int LESS_THAN                   = 2;
	public final static int LESS_THAN_OR_EQUAL          = 3;
	public final static int BIGGER_THAN                 = 4;
	public final static int BIGGER_THAN_OR_EQUAL        = 5;
	public final static int STRING_LESS_THAN            = 100;
	public final static int STRING_LESS_THAN_OR_EQUAL   = 101;
	public final static int STRING_BIGGER_THAN          = 102;
	public final static int STRING_BIGGER_THAN_OR_EQUAL = 103;
	public final static int STRING_EQUAL_IGNORE_CASE    = 104;
	public final static int STRING_UNEQUAL_IGNORE_CASE  = 105;

	// internal only, caller should not case about input type
	private final static int DOUBLE_EQUAL               = -1;
	private final static int DOUBLE_LESS_THAN           = -2;
	private final static int DOUBLE_BIGGER_THAN         = -3;

	private int              method;
	private boolean          result;
	final private IntExpression    ie1;
	final private IntExpression    ie2;
	final private DoubleExpression de1;
	final private DoubleExpression de2;
	final static private TranslateMap<String> translateMapString = TranslateMap.getStringMap();

	public Compare(IntExpression ie1, IntExpression ie2, int method){
		fillMethodInt(method);
		this.ie1 = ie1;
		this.ie2 = ie2;
		this.de1 = null;
		this.de2 = null;
	}
	public Compare(DoubleExpression de1, DoubleExpression de2, int method){
		fillMethodDouble(method);
		this.de1 = de1;
		this.de2 = de2;
		this.ie1 = null;
		this.ie2 = null;
	}

	private void fillMethodDouble(int method){
		switch(method){
		case EQUAL:
			this.method     = DOUBLE_EQUAL;
			this.result = true;
			break;
		case UNEQUAL:
			this.method     = DOUBLE_EQUAL;
			this.result = false;
			break;
		case LESS_THAN:
			this.method     = DOUBLE_LESS_THAN;
			this.result = true;
			break;
		case BIGGER_THAN_OR_EQUAL:
			this.method     = DOUBLE_LESS_THAN;
			this.result = false;
			break;
		case BIGGER_THAN:
			this.method     = DOUBLE_BIGGER_THAN;
			this.result = true;
			break;
		case LESS_THAN_OR_EQUAL:
			this.method     = DOUBLE_BIGGER_THAN;
			this.result = false;
			break;
		case STRING_LESS_THAN:
		case STRING_BIGGER_THAN_OR_EQUAL:
		case STRING_BIGGER_THAN:
		case STRING_LESS_THAN_OR_EQUAL:
		case STRING_EQUAL_IGNORE_CASE:
		case STRING_UNEQUAL_IGNORE_CASE:
			throw new IllegalArgumentException("String compare not applicable on double values.");
		default:
			throw new IllegalArgumentException("Unknown compare type.");
		}
	}
	private void fillMethodInt(int method){
		switch(method){
		case EQUAL:
			this.method     = EQUAL;
			this.result = true;
			break;
		case UNEQUAL:
			this.method     = EQUAL;
			this.result = false;
			break;
		case LESS_THAN:
			this.method     = LESS_THAN;
			this.result = true;
			break;
		case BIGGER_THAN_OR_EQUAL:
			this.method     = LESS_THAN;
			this.result = false;
			break;
		case BIGGER_THAN:
			this.method     = BIGGER_THAN;
			this.result = true;
			break;
		case LESS_THAN_OR_EQUAL:
			this.method     = BIGGER_THAN;
			this.result = false;
			break;
		case STRING_LESS_THAN:
			this.method     = STRING_LESS_THAN;
			this.result = true;
			break;
		case STRING_BIGGER_THAN_OR_EQUAL:
			this.method     = STRING_LESS_THAN;
			this.result = false;
			break;
		case STRING_BIGGER_THAN:
			this.method     = STRING_BIGGER_THAN;
			this.result = true;
			break;
		case STRING_LESS_THAN_OR_EQUAL:
			this.method     = STRING_BIGGER_THAN;
			this.result = false;
			break;
		case STRING_EQUAL_IGNORE_CASE:
			this.method     = STRING_EQUAL_IGNORE_CASE;
			this.result = true;
			break;
		case STRING_UNEQUAL_IGNORE_CASE:
			this.method     = STRING_EQUAL_IGNORE_CASE;
			this.result = false;
			break;
		default:
			throw new IllegalArgumentException("Unknown compare type.");
		}
	}


	public boolean [] evaluate(BPRow[] inputs) {
		int size = inputs.length;
		boolean [] output = new boolean[size];
		int [] il;
		int [] ir;
		double [] dl;
		double [] dr;
		switch(method){
		case EQUAL:
			il = ie1.evaluate(inputs);
			ir = ie2.evaluate(inputs);
			for (int i = 0; i<size; i++){
				output[i] = (il[i] == ir[i]) ? result : !result;
			}
			return output;
		case LESS_THAN:
			il = ie1.evaluate(inputs);
			ir = ie2.evaluate(inputs);
			for (int i = 0; i<size; i++){
				output[i] = (il[i] < ir[i]) ? result : !result;
			}
			return output;
		case BIGGER_THAN:
			il = ie1.evaluate(inputs);
			ir = ie2.evaluate(inputs);
			for (int i = 0; i<size; i++){
				output[i] = (il[i] > ir[i]) ? result : !result;
			}
			return output;
		case DOUBLE_EQUAL:
			dl = de1.evaluate(inputs);
			dr = de2.evaluate(inputs);
			for (int i = 0; i<size; i++){
				output[i] = (dl[i] == dr[i]) ? result : !result;
			}
			return output;
		case DOUBLE_LESS_THAN:
			dl = de1.evaluate(inputs);
			dr = de2.evaluate(inputs);
			for (int i = 0; i<size; i++){
				output[i] = (dl[i] < dr[i]) ? result : !result;
			}
			return output;
		case DOUBLE_BIGGER_THAN:
			dl = de1.evaluate(inputs);
			dr = de2.evaluate(inputs);
			for (int i = 0; i<size; i++){
				output[i] = (dl[i] > dr[i]) ? result : !result;
			}
			return output;
		case STRING_LESS_THAN:
			il = ie1.evaluate(inputs);
			ir = ie2.evaluate(inputs);
			for (int i = 0; i<size; i++){
				String s1 = translateMapString.translate(il[i]);
				String s2 = translateMapString.translate(ir[i]);
				output[i] = (s1.compareTo(s2) == -1) ? result : !result;
			}
			return output;
		case STRING_BIGGER_THAN:
			il = ie1.evaluate(inputs);
			ir = ie2.evaluate(inputs);
			for (int i = 0; i<size; i++){
				String s1 = translateMapString.translate(il[i]);
				String s2 = translateMapString.translate(ir[i]);
				output[i] = (s1.compareTo(s2) == 1) ? result : !result;
			}
			return output;
		case STRING_EQUAL_IGNORE_CASE:
			il = ie1.evaluate(inputs);
			ir = ie2.evaluate(inputs);
			for (int i = 0; i<size; i++){
				String s1 = translateMapString.translate(il[i]);
				String s2 = translateMapString.translate(ir[i]);
				output[i] = (s1.equalsIgnoreCase(s2)) ? result : !result;
			}
			return output;
		}
		throw new IllegalArgumentException("Internal assertion error in Compare, unknown compare type.");
	}
	public boolean isStatic() {
		return false;
	}
}

package mn.bplogic.api.expressions;

public class STATICBOOL {
	public static boolean[] staticVal(int size, boolean value) {
		if (value){
			return TRUE.staticVal(size);
		}
		return FALSE.staticVal(size);
	}
}

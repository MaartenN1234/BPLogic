package mn.bplogic.main;

import java.util.HashSet;

public class WarningList {
	private static WarningList warningListStorage = null;

	private HashSet<String> list;

	private WarningList(){
		list = new HashSet<String>();
	}

	private static synchronized void ensureExists(){
		if (warningListStorage == null){
			warningListStorage = new WarningList();
		}
	}
	public static void append(String string) {
		ensureExists();
		if(warningListStorage.list.add(string)){
			System.err.println(string);
		}
	}
	public String toString(){
		return list.toString();
	}
	public static String toStringStatic(){
		ensureExists();
		return warningListStorage.toString();
	}

}

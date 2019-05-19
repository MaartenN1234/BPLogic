package mn.bplogic.main;
import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;

public class TranslateMap<K> {
	private static TranslateMap<String> sm;
	private static TranslateMap<Date>   dm;
	private static TranslateMap<Object> om;

	private HashMap<K, Integer> map1;
	private K []                map2;
	private int genint;

	@SuppressWarnings("unchecked")
	private TranslateMap(){
		map1   = new HashMap<K, Integer>();
		map2   = (K[]) (new Object[100]);
		genint = 0;
	}


	public static synchronized TranslateMap<String> getStringMap(){
		if (sm == null) {
			sm = new TranslateMap<String>();
		}
		return sm;
	}
	public static synchronized TranslateMap<Date> getDateMap(){
		if (dm == null) {
			dm = new TranslateMap<Date>();
		}
		return dm;
	}
	public static synchronized TranslateMap<Object> getObjectMap(){
		if (om == null) {
			om = new TranslateMap<Object>();
		}
		return om;
	}


	public int translate(K s){
		Integer k = map1.get(s);
		if (k == null){
			synchronized(this){
				map1.put(s, genint);
				if(genint >= map2.length){
					map2 = Arrays.copyOf(map2, map2.length*2);
				}
				map2[genint] = s;
				k = genint;
				genint++;
			}
		}
		return k;
	}
	public K translate(int i){
		if (i<0)
			return null;
		return map2[i];
	}
}

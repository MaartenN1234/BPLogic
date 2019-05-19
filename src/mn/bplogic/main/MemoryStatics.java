package mn.bplogic.main;
import java.lang.ref.WeakReference;
import java.util.ArrayList;


public class MemoryStatics {
	private static MemoryStatics memoryStaticsStorage = null;
	private ArrayList<WeakReference<MemoryStaticsReporter>> wmsrl;
	private ArrayList<KeepAliveMemoryStaticsReporter>       kamsrl;
	private MemoryStatics(){
		wmsrl  = new ArrayList<WeakReference<MemoryStaticsReporter>>();
		kamsrl = new ArrayList<KeepAliveMemoryStaticsReporter>();
	}

	private static void ensureExists(){
		if (memoryStaticsStorage == null){
			memoryStaticsStorage = new MemoryStatics();
		}
	}

	public static void register(MemoryStaticsReporter msr){
		ensureExists();
		if (msr instanceof KeepAliveMemoryStaticsReporter){
			memoryStaticsStorage.kamsrl.add((KeepAliveMemoryStaticsReporter) msr);
		} else{
			memoryStaticsStorage.wmsrl.add(new WeakReference<MemoryStaticsReporter>(msr));
		}

	}
	public static String getTechnicalFootprint(){
		ensureExists();
		StringBuffer sb = new StringBuffer();
		for(MemoryStaticsReporter msr : memoryStaticsStorage.kamsrl){
			sb.append(msr.getTechnicalFootprint());
			sb.append("\n");
		}
		for(WeakReference<MemoryStaticsReporter> wmsr : memoryStaticsStorage.wmsrl){
			MemoryStaticsReporter msr = wmsr.get();
			if (msr != null){
				sb.append(msr.getTechnicalFootprint());
				sb.append("\n");
			}
		}
		return sb.toString();
	}

}

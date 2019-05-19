package mn.bplogic.main;


public class MemoryStaticsReporterMessage implements KeepAliveMemoryStaticsReporter {
	private String message;
	public MemoryStaticsReporterMessage(String message){
		this.message = message;
	}
	public String getTechnicalFootprint() {
		return message;
	}

}

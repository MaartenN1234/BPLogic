package mn.bplogic.api;

import mn.bplogic.rowsources.BPRow;

public interface RegularConverter extends Converter{
	public BPRow[] convert(BPRow[] inputs);
}

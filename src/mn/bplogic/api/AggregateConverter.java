package mn.bplogic.api;

import mn.bplogic.rowsources.BPRow;

public interface AggregateConverter extends Converter {
	public BPRow convert(BPRow[] inputs);
}

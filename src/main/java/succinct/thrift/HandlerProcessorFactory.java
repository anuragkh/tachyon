package succinct.thrift;

import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransport;

public class HandlerProcessorFactory extends TProcessorFactory {
	private String[] hostNames;

	public HandlerProcessorFactory(TProcessor processor) {
		super(processor);
	}
	public HandlerProcessorFactory(String[] hostNames) {
		super(null);
		this.hostNames = hostNames;
	}

	@Override
	public TProcessor getProcessor(TTransport trans) {
		SuccinctServiceHandler succinctService = new SuccinctServiceHandler(hostNames);
		return new SuccinctService.Processor<SuccinctServiceHandler>(succinctService);
	}
}
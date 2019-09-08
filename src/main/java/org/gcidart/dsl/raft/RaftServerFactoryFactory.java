package org.gcidart.dsl.raft;

import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.XmlRpcRequest;
import org.apache.xmlrpc.server.RequestProcessorFactoryFactory;

/* https://ws.apache.org/xmlrpc/handlerCreation.html */
public class RaftServerFactoryFactory implements RequestProcessorFactoryFactory {
	private final RequestProcessorFactory factory = new MasterFactory();
	private final RaftServer raft;
	public RaftServerFactoryFactory(RaftServer raft)
	{
		this.raft = raft;
	}
	public RequestProcessorFactory getRequestProcessorFactory(@SuppressWarnings("rawtypes") Class aClass)
	         throws XmlRpcException {
	      return factory;
	}
	private class MasterFactory implements RequestProcessorFactory {
	      public Object getRequestProcessor(XmlRpcRequest xmlRpcRequest)
	          throws XmlRpcException {
	        return raft;
	      }
	}
	
}

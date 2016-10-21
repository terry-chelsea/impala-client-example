package com.terry.impala.thrift;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import org.apache.hive.service.cli.thrift.TCloseOperationReq;
import org.apache.hive.service.cli.thrift.TCloseSessionReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementReq;
import org.apache.hive.service.cli.thrift.TExecuteStatementResp;
import org.apache.hive.service.cli.thrift.TFetchResultsReq;
import org.apache.hive.service.cli.thrift.TFetchResultsResp;
import org.apache.hive.service.cli.thrift.TOpenSessionReq;
import org.apache.hive.service.cli.thrift.TOpenSessionResp;
import org.apache.hive.service.cli.thrift.TOperationHandle;
import org.apache.hive.service.cli.thrift.TProtocolVersion;
import org.apache.hive.service.cli.thrift.TRow;
import org.apache.hive.service.cli.thrift.TRowSet;
import org.apache.hive.service.cli.thrift.TSessionHandle;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TTransport;

import javax.security.sasl.Sasl;

import java.util.*;

import com.cloudera.beeswax.api.Query;
import com.cloudera.beeswax.api.QueryHandle;
import com.cloudera.beeswax.api.QueryState;
import com.cloudera.beeswax.api.Results;
import com.cloudera.impala.thrift.ImpalaHiveServer2Service;
import com.cloudera.impala.thrift.ImpalaService;

public class ImpalaThriftClientTest
{
    private static String host;
    private static int port=21000;
    private static String stmt;
    private static String protocol;
    private static String serverName;
    private static int timeout = 60;
    private static boolean useKerberos = false;
    
    private static final String HOSTNAME = "impala.host";
    private static final String PORT = "impala.port";
    private static final String PRINCPAL = "impala.server.principal";
    private static final String TIMEOUT = "impala.query.timeout.seconds";
    
    private static Properties loadConfig() throws IOException {
    	String filename = ImpalaThriftClientTest.class.getSimpleName() + ".conf";
        InputStream input = ImpalaThriftClientTest.class.getClassLoader().getResourceAsStream(filename);
        Properties prop = new Properties();
        prop.load(input);
        input.close();
        return prop;
    }
    
    public static void main(String [] args) throws IOException {
    	Properties prop = loadConfig();
        host = prop.getProperty(HOSTNAME);
        if(prop.getProperty(PORT) != null) {
        	port = Integer.valueOf(prop.getProperty(PORT));
        }
        if(prop.getProperty(PRINCPAL) != null) {
        	String principal = prop.getProperty(PRINCPAL);
        	useKerberos = true;
        	if(!principal.contains("/")) {
	           	System.err.println("server principal must like protocol/server_host type.");
	          	return;
	        }
	        int index = principal.indexOf('/');
	        int atIndex = principal.indexOf('@');
	        protocol = principal.substring(0, index);
	        serverName = principal.substring(index + 1, atIndex > 0 ? atIndex : principal.length());
        }
        if(prop.getProperty(TIMEOUT) != null) {
        	timeout = Integer.valueOf(prop.getProperty(TIMEOUT));
        }

        Scanner sc = new Scanner(System.in);
        try {
        	ImpalaThriftClientTest.testThriftClient(sc);
        } catch (Exception e) {
        	e.printStackTrace();
        }
        sc.close();
    }
    
    private static ImpalaService.Client getClient() throws Exception {
    	//open connection
        TTransport transport = new TSocket(host,port);
        if(useKerberos) {
        	TSaslClientTransport saslTransport = new TSaslClientTransport("GSSAPI", null, 
        		protocol, serverName, null, null, transport);
        	transport = saslTransport;
        } 
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        //connect to client
        ImpalaService.Client client = new ImpalaService.Client(protocol);
        client.PingImpalaService();
        return client;
    }
    
    protected static void testThriftClient(Scanner sc) throws Exception {
    	ImpalaService.Client client = null;
    	try {
    		client = getClient();
    	} catch (Exception e) {
    		e.printStackTrace();
    		return;
    	}
    	String line = null;
    	
    	System.out.println(">>>>>>>>Input Statement Line<<<<<<<<<");
    	while((line = sc.nextLine()) != null) {
    		if(line.trim().equalsIgnoreCase("quit")) {
    			System.out.println("Bye!");
    			break;
    		}
    		try {
    			executeAndOutput(client, line);
    		} catch(Exception e) {
    			System.err.println("Failed to execute sql : " + line);
    			e.printStackTrace();
    		}
        	System.out.println(">>>>>>>>Input Statement Line<<<<<<<<<");
    	}
    }
    	
    private static void executeAndOutput(ImpalaService.Client client, String statement) 
    		throws Exception {
        Query query = new Query();
        query.setQuery(statement); 
            
        QueryHandle handle = client.query(query);
		System.out.println("Submit query " + statement + ", Query Id : " + handle.getId());
	            
		QueryState queryState = null;
		long start = System.currentTimeMillis();
		while(true) {
	        queryState = client.get_state(handle);
	        if(queryState == QueryState.FINISHED){
	        	break;
	        }
	        if(queryState == QueryState.EXCEPTION){
	          	System.err.println("Query caused exception !");
	           	break;
	        }
	        if(System.currentTimeMillis() - start > timeout * 1000) {
	            client.Cancel(handle);
	        }
			
	        Thread.sleep(1000);
		}

        boolean done = false;
        while(queryState == QueryState.FINISHED && done == false) {
            Results results = client.fetch(handle,false,100);
            
            List<String> data = results.data;
               
            for(int i=0;i<data.size();i++) {
                System.out.println(data.get(i));
            }

            if(results.has_more==false) {
                done = true;
            }

        }
    }
}
package com.basecamp.rundeck.plugin;

import com.dtolabs.rundeck.core.logging.ExecutionFileStorageException;
import com.dtolabs.rundeck.core.logging.LogFileStorageException;
import com.dtolabs.rundeck.core.plugins.Plugin;
import com.dtolabs.rundeck.plugins.ServiceNameConstants;
import com.dtolabs.rundeck.plugins.descriptions.PluginDescription;
import com.dtolabs.rundeck.plugins.descriptions.PluginProperty;
import com.dtolabs.rundeck.plugins.logging.ExecutionFileStoragePlugin;
import com.dtolabs.utils.Streams;
/*import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.*;*/
import org.apache.commons.io.IOUtils;

import org.apache.hadoop.hbase.thrift.generated.AlreadyExists;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TCell;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.*;
import java.util.Date;
import java.util.Map;
import java.util.List; 
import java.util.ArrayList; 
import java.util.logging.Level;
import java.util.logging.Logger;
import java.nio.ByteBuffer;


/**
 * Example plugin which stores to HBase over the Thrift API
 */
@Plugin(service = ServiceNameConstants.ExecutionFileStorage, name = "hbase")
@PluginDescription(title = "Hbase Execution File Storage", description = "Store execution files in Hbase")
public class HbaseExecutionFileStoragePlugin implements ExecutionFileStoragePlugin {
    static final Logger log = Logger.getLogger(HbaseExecutionFileStoragePlugin.class.getName());
    private Map<String, ? extends Object> context;

    @PluginProperty(title = "Hostname", description = "Hbase Thrift Server Hostname", required=true)
      private String hostname;
    
    @PluginProperty(title = "Port", description = "Hbase Thrift Server Port", required=true)
      private int port;
    

    public HbaseExecutionFileStoragePlugin() {
    }

    public boolean store(String filetype, InputStream stream, long length, Date lastModified) throws IOException,
            ExecutionFileStorageException {

          ArrayList<Mutation> mutations = new ArrayList<Mutation>();
          mutations.add(new Mutation(false, column(filetype), ByteBuffer.wrap(IOUtils.toByteArray(stream)), true));
          try { 
          client.mutateRow(table(), row(), mutations, null);
          } catch (IOError e) {
            throw new ExecutionFileStorageException("IOError: " + e.getMessage());
          } catch (TException e) {
            throw new ExecutionFileStorageException("TException: " + e.getMessage());
          }
        return true;
    }

    public boolean retrieve(String filetype, OutputStream stream) throws IOException, ExecutionFileStorageException {
      List<TCell> result;
      try  {
       result = client.get(table(), row(), column(filetype), null);
      } catch (IOError e) {
        throw new ExecutionFileStorageException("IOError: " + e.getMessage());
      } catch (TException e) {
        throw new ExecutionFileStorageException("TException: " + e.getMessage());
      }
      if (result.size() == 0) {
        return false;
      } else {
        IOUtils.write(result.get(0).getValue(), stream);
        return true;
      }
    }

    private TTransport transport;
    private Hbase.Client client;

    public void initialize(Map<String, ? extends Object> context) {
        this.context = context;
        try {
          TTransport transport = new TSocket(hostname, port);
          transport.open();
          TProtocol protocol = new TBinaryProtocol(transport, true, true);
          client  = new Hbase.Client(protocol);
          } catch (TTransportException e)  {
            throw new RuntimeException("TTransportException while connecting" + e.getMessage() + e);
          }
    }

    public void close() {
       transport.close();
    }

    private ByteBuffer table() {
    return ByteBuffer.wrap("rundeck".getBytes()); 
    }

    private ByteBuffer column(String filetype) {
        return ByteBuffer.wrap(("logs:" + filetype).getBytes()); 
    }

    private ByteBuffer row() {
      return ByteBuffer.wrap(((String) context.get("execid")).getBytes());
    }

    public boolean isAvailable(String filetype) throws ExecutionFileStorageException {
      List<TCell> result;
      try  {
       result = client.get(table(), row(), column(filetype), null);
      } catch (IOError e) {
        throw new ExecutionFileStorageException("IOError: " + e.getMessage());
      } catch (TException e) {
        throw new ExecutionFileStorageException("TException: " + e.getMessage());
      }
        return result.size() > 0;
    }

}

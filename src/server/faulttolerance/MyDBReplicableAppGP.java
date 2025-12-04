package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ResultSet;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;

/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspace (because we are pretending different
 * replicas are like different keyspaces), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

    /**
     * Set this value to as small a value with which you can get tests to still
     * pass. The lower it is, the faster your implementation is. Grader* will
     * use this value provided it is no greater than its MAX_SLEEP limit.
     * Faster
     * is not necessarily better, so don't sweat speed. Focus on safety.
     */
    public static final int SLEEP = 1000;

    // --- Cassandra Constants and State ---
    private static final String CASSANDRA_HOST = "127.0.0.1";
    private static final String TABLE_NAME = "mytable";
    private static final String KEY_COLUMN = "id";
    private static final String VALUE_COLUMN = "value";

    private Session session;
    private String keyspaceName;
    private Cluster cluster;

    /**
     * All Gigapaxos apps must either support a no-args constructor or a
     * constructor taking a String[] as the only argument. Gigapaxos relies on
     * adherence to this policy in order to be able to reflectively construct
     * customer application instances.
     *
     * @param args Singleton array whose args[0] specifies the keyspace in the
     * backend data store to which this server must connect.
     * Optional args[1] and args[2]
     * @throws IOException
     */
    public MyDBReplicableAppGP(String[] args) throws IOException {
        if (args == null || args.length == 0 || args[0] == null) {
            throw new IllegalArgumentException("Keyspace name must be provided in args[0]");
        }
        this.keyspaceName = args[0];
        
        this.cluster = Cluster.builder()
                .addContactPoint(CASSANDRA_HOST)
                .build();
        
        try (Session tempSession = cluster.connect()) {
            String createKeyspace = String.format(
                    "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'};",
                    this.keyspaceName);
            tempSession.execute(createKeyspace);
        } catch (Exception e) {
            System.err.println("Error creating keyspace " + this.keyspaceName + ": " + e.getMessage());
            throw new IOException("Failed to initialize Cassandra keyspace.", e);
        }

        this.session = cluster.connect(this.keyspaceName);
        String createTable = String.format(
                "CREATE TABLE IF NOT EXISTS %s (%s text PRIMARY KEY, %s text);",
                TABLE_NAME, KEY_COLUMN, VALUE_COLUMN);
        this.session.execute(createTable);
        
        System.out.println("MyDBReplicableAppGP initialized for keyspace: " + this.keyspaceName);
    }

    /**
     * Refer documentation of {@link Replicable#execute(Request, boolean)} to
     * understand what the boolean flag means.
     * <p>
     * You can assume that all requests will be of type {@link
     * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
     *
     * @param request
     * @param b true if this request is a state-changing operation (isStateChange).
     * @return true if the execution was successful.
     */
    @Override
    public boolean execute(Request request, boolean b) {
        if (!b) {
            return true; 
        }

        String commandString = request.toString();
        
        String[] parts = commandString.split(":", 3);
        String cmd = parts[0];
        String key = parts.length > 1 ? parts[1] : null;
        String value = parts.length > 2 ? parts[2] : null;

        if (key == null) {
             System.err.println("Invalid command format: " + commandString);
             return false;
        }

        try {
            String cql;
            switch (cmd.toUpperCase()) {
                case "INSERT":
                case "UPDATE":
                    if (value == null) {
                        System.err.println("INSERT/UPDATE requires a value: " + commandString);
                        return false;
                    }
                    cql = String.format("INSERT INTO %s (%s, %s) VALUES ('%s', '%s')", 
                                        TABLE_NAME, KEY_COLUMN, VALUE_COLUMN, key, value);
                    session.execute(cql);
                    System.out.println("Executed: " + commandString);
                    break;
                case "DELETE":
                    cql = String.format("DELETE FROM %s WHERE %s = '%s'", 
                                        TABLE_NAME, KEY_COLUMN, key);
                    session.execute(cql);
                    System.out.println("Executed: " + commandString);
                    break;
                case "READ":
                    cql = String.format("SELECT %s FROM %s WHERE %s = '%s'", 
                                        VALUE_COLUMN, TABLE_NAME, KEY_COLUMN, key);
                    ResultSet rs = session.execute(cql);
                    Row row = rs.one();
                    System.out.println("Executed READ for key " + key + ": " + (row != null ? row.getString(VALUE_COLUMN) : "NOT_FOUND"));
                    break;
                default:
                    System.err.println("Unknown command: " + cmd);
                    return false;
            }
            return true;
        } catch (Exception e) {
            System.err.println("Failed to execute command " + commandString + ": " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    /**
     * Refer documentation of
     * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
     *
     * @param request
     * @return
     */
    @Override
    public boolean execute(Request request) {
        // We delegate to the two-argument execute for unified handling, treating all requests 
        // as potentially affecting state or requiring consensus for linearizability.
        return this.execute(request, true); 
    }

    /**
     * Refer documentation of {@link Replicable#checkpoint(String)}.
     *
     * @param s
     * @return A JSON string representing the entire table state.
     */
    @Override
    public String checkpoint(String s) {
        System.out.println("Starting checkpoint...");
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("[");
        
        String cql = String.format("SELECT %s, %s FROM %s", 
                                   KEY_COLUMN, VALUE_COLUMN, TABLE_NAME);
        
        ResultSet rs = session.execute(cql);
        boolean first = true;
        
        for (Row row : rs) {
            if (!first) {
                jsonBuilder.append(",");
            }
            String key = row.getString(KEY_COLUMN);
            String value = row.getString(VALUE_COLUMN);
            
            jsonBuilder.append(String.format("{\"%s\":\"%s\", \"%s\":\"%s\"}", 
                                             KEY_COLUMN, key, VALUE_COLUMN, value));
            first = false;
        }
        
        jsonBuilder.append("]");
        String checkpointData = jsonBuilder.toString();
        System.out.println("Checkpoint complete. Data size: " + checkpointData.length());
        return checkpointData;
    }

    /**
     * Refer documentation of {@link Replicable#restore(String, String)}
     *
     * @param id The application ID (keyspace name).
     * @param checkpoint The JSON string checkpoint data.
     * @return true if the state was restored successfully.
     */
    @Override
    public boolean restore(String id, String checkpoint) {
        System.out.println("Starting restore from checkpoint for keyspace: " + id);
        
        if (checkpoint == null || checkpoint.trim().isEmpty() || checkpoint.equals("[]")) {
            System.out.println("Empty checkpoint. Clearing table and finishing restore.");
            session.execute(String.format("TRUNCATE %s", TABLE_NAME));
            return true;
        }
        
        try {
            // 1. Clear the existing table state to ensure a clean restoration
            String truncateCql = String.format("TRUNCATE %s", TABLE_NAME);
            session.execute(truncateCql);
            System.out.println("Table truncated successfully.");

            // 2. Parse the checkpoint string and re-insert data.
            if (checkpoint.startsWith("[") && checkpoint.endsWith("]")) {
                String content = checkpoint.substring(1, checkpoint.length() - 1);
                
                String[] records = content.split("},\\{"); 
                
                for (String record : records) {
                    // Reconstruct the JSON object parts for robust parsing
                    String jsonRecord = (records.length > 1 && record != records[0]) ? "{" + record : record;
                    jsonRecord = (records.length > 1 && record != records[records.length - 1]) ? jsonRecord + "}" : jsonRecord;
                    
                    String key = extractValue(jsonRecord, KEY_COLUMN);
                    String value = extractValue(jsonRecord, VALUE_COLUMN);
                    
                    if (key != null && value != null) {
                        String insertCql = String.format("INSERT INTO %s (%s, %s) VALUES ('%s', '%s')", 
                                                         TABLE_NAME, KEY_COLUMN, VALUE_COLUMN, key, value);
                        session.execute(insertCql);
                    }
                }
            }
            
            System.out.println("Restore complete. Data re-inserted.");
            return true;
        } catch (Exception e) {
            System.err.println("Restore failed: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
    
    /**
     * Helper to manually extract value from simple '{"key":"value"}' JSON format
     * used during checkpoint/restore.
     */
    private String extractValue(String json, String property) {
        String search = "\"" + property + "\":\"";
        int start = json.indexOf(search);
        if (start == -1) return null;
        
        start += search.length();
        int end = json.indexOf("\"", start);
        if (end == -1) return null;
        
        return json.substring(start, end);
    }


    /**
     * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
     * .RequestPacket will be used by Grader, so you don't need to implement
     * this method.}
     *
     * @param s
     * @return
     * @throws RequestParseException
     */
    @Override
    public Request getRequest(String s) throws RequestParseException {
        return null;
    }

    /**
     * @return Return all integer packet types used by this application. For an
     * example of how to define your own IntegerPacketType enum, refer {@link
     * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
     * need to be implemented because the assignment Grader will only use
     * {@link
     * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
     */
    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<IntegerPacketType>();
    }
}
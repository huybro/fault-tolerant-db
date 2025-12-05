package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ResultSet;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
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

	private Session session;
	private Cluster cluster;
	private String keyspaceName;

	// Table structure from GraderCommonSetup: grade(id int, events list<int>)
	private static final String TABLE_NAME = "grade";

	/**
	 * All Gigapaxos apps must either support a no-args constructor or a
	 * constructor taking a String[] as the only argument. Gigapaxos relies on
	 * adherence to this policy in order to be able to reflectively construct
	 * customer application instances.
	 *
	 * @param args Singleton array whose args[0] specifies the keyspace in the
	 *             backend data store to which this server must connect.
	 *             Optional args[1] and args[2]
	 * @throws IOException
	 */
	public MyDBReplicableAppGP(String[] args) throws IOException {
		// Setup connection to the data store and keyspace
		if (args == null || args.length == 0 || args[0] == null) {
			throw new IllegalArgumentException("Keyspace name must be provided in args[0]");
		}
		this.keyspaceName = args[0];

		this.cluster = Cluster.builder()
				.addContactPoint("127.0.0.1")
				.build();

		// Create keyspace if not exists
		try (Session tempSession = cluster.connect()) {
			String createKeyspace = String.format(
					"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
					"{'class': 'SimpleStrategy', 'replication_factor': '3'};",
					this.keyspaceName);
			tempSession.execute(createKeyspace);
		} catch (Exception e) {
			System.err.println("Error creating keyspace: " + e.getMessage());
			throw new IOException("Failed to initialize Cassandra keyspace.", e);
		}

		// Connect to the keyspace - grade table is created by GraderCommonSetup
		this.session = cluster.connect(this.keyspaceName);
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
	 * @param b
	 * @return
	 */
	@Override
	public boolean execute(Request request, boolean b) {
		// Extract CQL from RequestPacket and execute on data store
		String cql = null;
		if (request instanceof RequestPacket) {
			cql = ((RequestPacket) request).requestValue;
		} else {
			cql = request.toString();
		}

		if (cql == null || cql.trim().isEmpty()) {
			return true;
		}

		try {
			session.execute(cql);
			return true;
		} catch (Exception e) {
			System.err.println("Failed to execute: " + cql + " Error: " + e.getMessage());
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
		// Delegate to the two-argument execute
		return this.execute(request, true);
	}

	/**
	 * Refer documentation of {@link Replicable#checkpoint(String)}.
	 *
	 * @param s
	 * @return
	 */
	@Override
	public String checkpoint(String s) {
		// Serialize entire table state to JSON
		StringBuilder json = new StringBuilder();
		json.append("{");

		try {
			String cql = "SELECT id, events FROM " + TABLE_NAME;
			ResultSet rs = session.execute(cql);
			boolean first = true;

			for (Row row : rs) {
				if (!first) json.append(",");
				int key = row.getInt("id");
				List<Integer> events = row.getList("events", Integer.class);
				json.append("\"").append(key).append("\":");
				json.append(events != null ? events.toString() : "[]");
				first = false;
			}
		} catch (Exception e) {
			System.err.println("Checkpoint error: " + e.getMessage());
		}

		json.append("}");
		return json.toString();
	}

	/**
	 * Refer documentation of {@link Replicable#restore(String, String)}
	 *
	 * @param s
	 * @param s1
	 * @return
	 */
	@Override
	public boolean restore(String s, String s1) {
		// Restore table state from JSON checkpoint
		if (s1 == null || s1.trim().isEmpty() || s1.equals("{}")) {
			try {
				session.execute("TRUNCATE " + TABLE_NAME);
			} catch (Exception e) { /* ignore */ }
			return true;
		}

		try {
			session.execute("TRUNCATE " + TABLE_NAME);

			// Parse JSON: {"key": [events], ...}
			if (s1.startsWith("{") && s1.endsWith("}")) {
				String content = s1.substring(1, s1.length() - 1).trim();
				if (content.isEmpty()) return true;

				int pos = 0;
				while (pos < content.length()) {
					int keyStart = content.indexOf('"', pos);
					if (keyStart == -1) break;
					int keyEnd = content.indexOf('"', keyStart + 1);
					if (keyEnd == -1) break;
					String key = content.substring(keyStart + 1, keyEnd);

					int arrayStart = content.indexOf('[', keyEnd);
					if (arrayStart == -1) break;
					int arrayEnd = content.indexOf(']', arrayStart);
					if (arrayEnd == -1) break;
					String arrayStr = content.substring(arrayStart, arrayEnd + 1);

					String insertCql = String.format(
							"INSERT INTO %s (id, events) VALUES (%s, %s)",
							TABLE_NAME, key, arrayStr);
					session.execute(insertCql);

					pos = arrayEnd + 1;
					while (pos < content.length() && 
						   (content.charAt(pos) == ',' || content.charAt(pos) == ' ')) {
						pos++;
					}
				}
			}
			return true;
		} catch (Exception e) {
			System.err.println("Restore failed: " + e.getMessage());
			return false;
		}
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
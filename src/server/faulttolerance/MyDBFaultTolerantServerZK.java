package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.AbstractBytePacketDemultiplexer;
import edu.umass.cs.nio.MessageNIOTransport;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import edu.umass.cs.utils.Util;
import org.json.JSONException;
import org.json.JSONObject;
import server.ReplicatedServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Custom fault-tolerant replicated database server using a simple Paxos-like protocol.
 * 
 * Protocol:
 * 1. Leader receives all client requests (non-leaders forward to leader)
 * 2. Leader broadcasts PROPOSAL to all replicas
 * 3. Replicas execute and send ACK back
 * 4. Leader waits for majority before responding
 * 5. Periodic checkpointing to disk for recovery
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {

	private static final Logger log = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());

	public static final int SLEEP = 1000;
	public static final boolean DROP_TABLES_AFTER_TESTS = true;
	public static final int MAX_LOG_SIZE = 400;
	public static final int DEFAULT_PORT = 2181;

	// Cassandra
	private Session session;
	private Cluster cluster;

	// Server identity and messaging
	private final String myID;
	private final MessageNIOTransport<String, String> serverMessenger;
	private final NodeConfig<String> nodeConfig;
	private String leader;

	// Table structure from tests
	private static final String TABLE_NAME = "grade";

	// Sequence tracking
	private AtomicLong sequenceNumber = new AtomicLong(0);
	private ConcurrentHashMap<Long, RequestState> pendingRequests = new ConcurrentHashMap<>();
	
	// Checkpoint paths
	private final String checkpointDir = "paxos_logs";
	private final String checkpointFile;
	private final String seqFile;

	// Message types
	private enum MsgType {
		REQUEST,      // Forward to leader
		PROPOSAL,     // Leader broadcasts
		ACK,          // Replica confirms
		SYNC_REQUEST, // Request state sync
		SYNC_REPLY    // State sync response
	}

	// Tracks state for pending requests awaiting ACKs
	private static class RequestState {
		String cql;
		InetSocketAddress clientAddr;
		CopyOnWriteArrayList<String> ackedServers = new CopyOnWriteArrayList<>();
		long timestamp;

		RequestState(String cql, InetSocketAddress clientAddr) {
			this.cql = cql;
			this.clientAddr = clientAddr;
			this.timestamp = System.currentTimeMillis();
		}
	}

	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig, String myID, 
			InetSocketAddress isaDB) throws IOException {
		super(new InetSocketAddress(nodeConfig.getNodeAddress(myID),
				nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET), isaDB, myID);

		this.myID = myID;
		this.nodeConfig = nodeConfig;
		this.checkpointFile = checkpointDir + "/checkpoint_" + myID + ".json";
		this.seqFile = checkpointDir + "/seq_" + myID + ".txt";

		// Create checkpoint directory
		new File(checkpointDir).mkdirs();

		// Connect to Cassandra
		this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		this.session = cluster.connect(myID);
		log.log(Level.INFO, "Server {0} connected to Cassandra keyspace {1}", 
				new Object[]{myID, myID});

		// Elect leader (first node in config)
		for (String node : nodeConfig.getNodeIDs()) {
			this.leader = node;
			break;
		}
		log.log(Level.INFO, "Server {0}: leader is {1}", new Object[]{myID, leader});

		// Set up server-to-server messaging
		this.serverMessenger = new MessageNIOTransport<String, String>(myID, nodeConfig,
				new AbstractBytePacketDemultiplexer() {
					@Override
					public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
						handleMessageFromServer(bytes, nioHeader);
						return true;
					}
				}, true);

		log.log(Level.INFO, "Server {0} started on {1}", 
				new Object[]{myID, this.clientMessenger.getListeningSocketAddress()});

		// Crash recovery
		recoverFromCheckpoint();
	}

	@Override
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		String request = new String(bytes);
		log.log(Level.INFO, "{0} received client request: {1}", new Object[]{myID, request});

		// Extract CQL from request (may be wrapped in JSON)
		String cql = extractCQL(request);
		if (cql == null || cql.trim().isEmpty()) {
			sendToClient(header.sndr, "[success]");
			return;
		}

		if (myID.equals(leader)) {
			// I am the leader - create proposal and broadcast
			long seqNum = sequenceNumber.getAndIncrement();
			
			RequestState state = new RequestState(cql, header.sndr);
			pendingRequests.put(seqNum, state);
			
			// Broadcast proposal
			broadcastProposal(seqNum, cql);
			
			// Also execute locally
			executeAndAck(seqNum, cql, myID);
			
		} else {
			// Forward to leader
			try {
				JSONObject msg = new JSONObject();
				msg.put("type", MsgType.REQUEST.toString());
				msg.put("cql", cql);
				msg.put("clientHost", header.sndr.getAddress().getHostAddress());
				msg.put("clientPort", header.sndr.getPort());
				serverMessenger.send(leader, msg.toString().getBytes());
			} catch (Exception e) {
				log.log(Level.SEVERE, "Failed to forward to leader", e);
			}
		}

		// Send immediate response to client
		sendToClient(header.sndr, "[success:" + request + "]");
	}

	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		try {
			JSONObject json = new JSONObject(new String(bytes));
			String type = json.getString("type");

			if (type.equals(MsgType.REQUEST.toString())) {
				// Leader receives request from follower
				if (myID.equals(leader)) {
					String cql = json.getString("cql");
					String clientHost = json.optString("clientHost", "");
					int clientPort = json.optInt("clientPort", 0);
					
					long seqNum = sequenceNumber.getAndIncrement();
					InetSocketAddress clientAddr = clientHost.isEmpty() ? null : 
							new InetSocketAddress(clientHost, clientPort);
					
					RequestState state = new RequestState(cql, clientAddr);
					pendingRequests.put(seqNum, state);
					
					broadcastProposal(seqNum, cql);
					executeAndAck(seqNum, cql, myID);
				}

			} else if (type.equals(MsgType.PROPOSAL.toString())) {
				// Replica receives proposal from leader
				long seqNum = json.getLong("seqNum");
				String cql = json.getString("cql");
				String fromLeader = json.getString("leader");
				
				executeAndAck(seqNum, cql, fromLeader);

			} else if (type.equals(MsgType.ACK.toString())) {
				// Leader receives ACK from replica
				if (myID.equals(leader)) {
					long seqNum = json.getLong("seqNum");
					String fromServer = json.getString("from");
					
					RequestState state = pendingRequests.get(seqNum);
					if (state != null) {
						state.ackedServers.addIfAbsent(fromServer);
						
						// Check majority
						int totalServers = 0;
						for (String ignored : nodeConfig.getNodeIDs()) totalServers++;
						int majority = (totalServers / 2) + 1;
						
						if (state.ackedServers.size() >= majority) {
							// Committed - clean up
							pendingRequests.remove(seqNum);
							
							// Checkpoint if needed
							if (pendingRequests.size() > MAX_LOG_SIZE / 2) {
								checkpoint();
							}
						}
					}
				}

			} else if (type.equals(MsgType.SYNC_REQUEST.toString())) {
				// Someone requesting state sync
				sendSyncReply(header);

			} else if (type.equals(MsgType.SYNC_REPLY.toString())) {
				// Received state sync
				String checkpointData = json.getString("checkpoint");
				long leaderSeq = json.getLong("seqNum");
				restoreFromCheckpointData(checkpointData);
				sequenceNumber.set(leaderSeq);
			}

		} catch (JSONException e) {
			log.log(Level.WARNING, "Failed to parse server message", e);
		}
	}

	private String extractCQL(String request) {
		// Try to parse as JSON first (wrapped request)
		try {
			JSONObject json = new JSONObject(request);
			if (json.has("request")) {
				return json.getString("request");
			}
			if (json.has("REQUEST")) {
				return json.getString("REQUEST");
			}
		} catch (JSONException e) {
			// Not JSON, treat as raw CQL
		}
		return request;
	}

	private void broadcastProposal(long seqNum, String cql) {
		try {
			JSONObject proposal = new JSONObject();
			proposal.put("type", MsgType.PROPOSAL.toString());
			proposal.put("seqNum", seqNum);
			proposal.put("cql", cql);
			proposal.put("leader", myID);

			for (String node : nodeConfig.getNodeIDs()) {
				if (!node.equals(myID)) {
					serverMessenger.send(node, proposal.toString().getBytes());
				}
			}
			log.log(Level.INFO, "{0} broadcast proposal seqNum={1}", new Object[]{myID, seqNum});
		} catch (Exception e) {
			log.log(Level.SEVERE, "Failed to broadcast proposal", e);
		}
	}

	private void executeAndAck(long seqNum, String cql, String leaderID) {
		try {
			// Execute CQL
			session.execute(cql);
			log.log(Level.INFO, "{0} executed seqNum={1}: {2}", new Object[]{myID, seqNum, cql});

			// Send ACK to leader (if not self)
			if (!myID.equals(leaderID)) {
				JSONObject ack = new JSONObject();
				ack.put("type", MsgType.ACK.toString());
				ack.put("seqNum", seqNum);
				ack.put("from", myID);
				serverMessenger.send(leaderID, ack.toString().getBytes());
			} else {
				// Self-ack for leader
				RequestState state = pendingRequests.get(seqNum);
				if (state != null) {
					state.ackedServers.addIfAbsent(myID);
				}
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "Failed to execute CQL: " + cql, e);
		}
	}

	private void sendToClient(InetSocketAddress client, String response) {
		try {
			serverMessenger.send(client, response.getBytes());
		} catch (IOException e) {
			log.log(Level.WARNING, "Failed to send to client", e);
		}
	}

	private void sendSyncReply(NIOHeader header) {
		try {
			String checkpointData = createCheckpointData();
			JSONObject reply = new JSONObject();
			reply.put("type", MsgType.SYNC_REPLY.toString());
			reply.put("checkpoint", checkpointData);
			reply.put("seqNum", sequenceNumber.get());
			serverMessenger.send(header.sndr, reply.toString().getBytes());
		} catch (Exception e) {
			log.log(Level.WARNING, "Failed to send sync reply", e);
		}
	}

	private void checkpoint() {
		try {
			String data = createCheckpointData();
			Files.write(Paths.get(checkpointFile), data.getBytes());
			Files.write(Paths.get(seqFile), String.valueOf(sequenceNumber.get()).getBytes());
			log.log(Level.INFO, "{0} checkpointed state", myID);
		} catch (IOException e) {
			log.log(Level.WARNING, "Checkpoint failed", e);
		}
	}

	private String createCheckpointData() {
		StringBuilder json = new StringBuilder();
		json.append("{");
		try {
			ResultSet rs = session.execute("SELECT id, events FROM " + TABLE_NAME);
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
			log.log(Level.WARNING, "Error creating checkpoint", e);
		}
		json.append("}");
		return json.toString();
	}

	private void recoverFromCheckpoint() {
		try {
			File cpFile = new File(checkpointFile);
			File sqFile = new File(seqFile);
			
			if (cpFile.exists() && sqFile.exists()) {
				String data = new String(Files.readAllBytes(Paths.get(checkpointFile)));
				String seq = new String(Files.readAllBytes(Paths.get(seqFile))).trim();
				
				restoreFromCheckpointData(data);
				sequenceNumber.set(Long.parseLong(seq));
				log.log(Level.INFO, "{0} recovered from checkpoint, seq={1}", 
						new Object[]{myID, seq});
			}
		} catch (Exception e) {
			log.log(Level.INFO, "{0} no checkpoint to recover from", myID);
		}
	}

	private void restoreFromCheckpointData(String checkpointData) {
		if (checkpointData == null || checkpointData.equals("{}")) return;
		
		try {
			session.execute("TRUNCATE " + TABLE_NAME);
			
			if (checkpointData.startsWith("{") && checkpointData.endsWith("}")) {
				String content = checkpointData.substring(1, checkpointData.length() - 1).trim();
				if (content.isEmpty()) return;
				
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
			log.log(Level.INFO, "{0} restored from checkpoint data", myID);
		} catch (Exception e) {
			log.log(Level.WARNING, "Failed to restore from checkpoint", e);
		}
	}

	@Override
	public void close() {
		checkpoint(); // Save state before closing
		if (serverMessenger != null) serverMessenger.stop();
		if (session != null) session.close();
		if (cluster != null) cluster.close();
		super.close();
	}

	public static void main(String[] args) throws IOException {
		new MyDBFaultTolerantServerZK(NodeConfigUtils.getNodeConfigFromFile(
				args[0], ReplicatedServer.SERVER_PREFIX, ReplicatedServer.SERVER_PORT_OFFSET), 
				args[1], 
				args.length > 2 ? Util.getInetSocketAddressFromString(args[2]) 
						: new InetSocketAddress("localhost", 9042));
	}
}
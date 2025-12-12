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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Custom fault-tolerant replicated database server using a simple Paxos-like protocol
 * with STRICT TOTAL ORDERING.
 * 
 * Key fix: Leader serializes ALL requests - waits for commit before processing next.
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
	private int totalServers = 0;

	// Table structure
	private static final String TABLE_NAME = "grade";

	// Sequence tracking - STRICT ordering
	private AtomicLong nextSeqToAssign = new AtomicLong(0);
	private AtomicLong nextSeqToExecute = new AtomicLong(0);
	
	// Pending proposals waiting for their turn to execute (for followers)
	private ConcurrentHashMap<Long, String> pendingProposals = new ConcurrentHashMap<>();
	
	// Leader's pending request awaiting ACKs
	private volatile long currentProposalSeq = -1;
	private volatile String currentProposalCql = null;
	private volatile int acksReceived = 0;
	private final Object proposalLock = new Object();
	
	// Request queue for serialization at leader
	private LinkedBlockingQueue<PendingRequest> requestQueue = new LinkedBlockingQueue<>();
	private volatile boolean processingRequest = false;
	
	// Checkpoint paths
	private final String checkpointDir = "paxos_logs";
	private final String checkpointFile;
	private final String seqFile;

	// Message types
	private enum MsgType {
		REQUEST,
		PROPOSAL,
		ACK,
		COMMIT
	}

	private static class PendingRequest {
		String cql;
		InetSocketAddress clientAddr;
		PendingRequest(String cql, InetSocketAddress addr) {
			this.cql = cql;
			this.clientAddr = addr;
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

		new File(checkpointDir).mkdirs();

		// Connect to Cassandra
		this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		this.session = cluster.connect(myID);

		// Count servers and elect leader (first node)
		for (String node : nodeConfig.getNodeIDs()) {
			if (leader == null) leader = node;
			totalServers++;
		}
		log.log(Level.INFO, "Server {0}: leader={1}, totalServers={2}", 
				new Object[]{myID, leader, totalServers});

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

		recoverFromCheckpoint();
		
		// Start request processor thread (leader only)
		if (myID.equals(leader)) {
			new Thread(this::processRequestQueue, "RequestProcessor-" + myID).start();
		}
	}

	@Override
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		String request = new String(bytes);
		String cql = extractCQL(request);
		
		if (cql == null || cql.trim().isEmpty()) {
			sendToClient(header.sndr, "[success]");
			return;
		}

		if (myID.equals(leader)) {
			// Queue request for ordered processing
			requestQueue.offer(new PendingRequest(cql, header.sndr));
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

		// Send immediate response
		sendToClient(header.sndr, "[success:" + request + "]");
	}

	/**
	 * Leader processes requests one at a time, waiting for majority ACKs before next.
	 */
	private void processRequestQueue() {
		while (true) {
			try {
				PendingRequest req = requestQueue.take();
				processOneRequest(req.cql);
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	private void processOneRequest(String cql) {
		synchronized (proposalLock) {
			long seqNum = nextSeqToAssign.getAndIncrement();
			currentProposalSeq = seqNum;
			currentProposalCql = cql;
			acksReceived = 0;
			
			// Broadcast PROPOSAL
			try {
				JSONObject proposal = new JSONObject();
				proposal.put("type", MsgType.PROPOSAL.toString());
				proposal.put("seqNum", seqNum);
				proposal.put("cql", cql);

				for (String node : nodeConfig.getNodeIDs()) {
					serverMessenger.send(node, proposal.toString().getBytes());
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, "Failed to broadcast proposal", e);
			}
			
			// Wait for majority ACKs (with timeout)
			int majority = (totalServers / 2) + 1;
			long deadline = System.currentTimeMillis() + 5000; // 5 sec timeout
			
			while (acksReceived < majority && System.currentTimeMillis() < deadline) {
				try {
					proposalLock.wait(100);
				} catch (InterruptedException e) {
					break;
				}
			}
			
			// Broadcast COMMIT so all replicas know this is committed
			if (acksReceived >= majority) {
				broadcastCommit(seqNum);
			}
			
			// Checkpoint if needed
			if (nextSeqToAssign.get() % (MAX_LOG_SIZE / 2) == 0) {
				checkpoint();
			}
		}
	}

	private void broadcastCommit(long seqNum) {
		try {
			JSONObject commit = new JSONObject();
			commit.put("type", MsgType.COMMIT.toString());
			commit.put("seqNum", seqNum);
			
			for (String node : nodeConfig.getNodeIDs()) {
				serverMessenger.send(node, commit.toString().getBytes());
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "Failed to broadcast commit", e);
		}
	}

	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		try {
			JSONObject json = new JSONObject(new String(bytes));
			String type = json.getString("type");

			if (type.equals(MsgType.REQUEST.toString())) {
				// Leader receives forwarded request
				if (myID.equals(leader)) {
					String cql = json.getString("cql");
					String clientHost = json.optString("clientHost", "");
					int clientPort = json.optInt("clientPort", 0);
					InetSocketAddress clientAddr = clientHost.isEmpty() ? null : 
							new InetSocketAddress(clientHost, clientPort);
					requestQueue.offer(new PendingRequest(cql, clientAddr));
				}

			} else if (type.equals(MsgType.PROPOSAL.toString())) {
				// All servers receive proposal and execute in order
				long seqNum = json.getLong("seqNum");
				String cql = json.getString("cql");
				
				// Store and try to execute in order
				pendingProposals.put(seqNum, cql);
				executeInOrder();
				
				// Send ACK to leader
				JSONObject ack = new JSONObject();
				ack.put("type", MsgType.ACK.toString());
				ack.put("seqNum", seqNum);
				ack.put("from", myID);
				serverMessenger.send(leader, ack.toString().getBytes());

			} else if (type.equals(MsgType.ACK.toString())) {
				// Leader receives ACK
				if (myID.equals(leader)) {
					long seqNum = json.getLong("seqNum");
					synchronized (proposalLock) {
						if (seqNum == currentProposalSeq) {
							acksReceived++;
							proposalLock.notifyAll();
						}
					}
				}

			} else if (type.equals(MsgType.COMMIT.toString())) {
				// Optional: handle commit confirmation
				long seqNum = json.getLong("seqNum");
				// Could use this for additional consistency guarantees
			}

		} catch (JSONException | IOException e) {
			log.log(Level.WARNING, "Failed to handle server message", e);
		}
	}

	/**
	 * Execute pending proposals in strict sequence order.
	 */
	private synchronized void executeInOrder() {
		while (true) {
			long next = nextSeqToExecute.get();
			String cql = pendingProposals.remove(next);
			if (cql == null) break;
			
			try {
				session.execute(cql);
				nextSeqToExecute.incrementAndGet();
			} catch (Exception e) {
				log.log(Level.WARNING, "Failed to execute: " + cql, e);
				nextSeqToExecute.incrementAndGet(); // Still advance to avoid stuck
			}
		}
	}

	private String extractCQL(String request) {
		try {
			JSONObject json = new JSONObject(request);
			if (json.has("request")) return json.getString("request");
			if (json.has("REQUEST")) return json.getString("REQUEST");
		} catch (JSONException e) {}
		return request;
	}

	private void sendToClient(InetSocketAddress client, String response) {
		try {
			serverMessenger.send(client, response.getBytes());
		} catch (IOException e) {
			log.log(Level.WARNING, "Failed to send to client", e);
		}
	}

	private void checkpoint() {
		try {
			String data = createCheckpointData();
			Files.write(Paths.get(checkpointFile), data.getBytes());
			Files.write(Paths.get(seqFile), 
					(nextSeqToAssign.get() + ":" + nextSeqToExecute.get()).getBytes());
			log.log(Level.INFO, "{0} checkpointed", myID);
		} catch (IOException e) {
			log.log(Level.WARNING, "Checkpoint failed", e);
		}
	}

	private String createCheckpointData() {
		StringBuilder json = new StringBuilder("{");
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
		} catch (Exception e) {}
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
				String[] parts = seq.split(":");
				nextSeqToAssign.set(Long.parseLong(parts[0]));
				nextSeqToExecute.set(Long.parseLong(parts.length > 1 ? parts[1] : parts[0]));
				log.log(Level.INFO, "{0} recovered: assign={1}, execute={2}", 
						new Object[]{myID, nextSeqToAssign.get(), nextSeqToExecute.get()});
			}
		} catch (Exception e) {
			log.log(Level.INFO, "{0} no checkpoint", myID);
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
					
					session.execute(String.format(
							"INSERT INTO %s (id, events) VALUES (%s, %s)",
							TABLE_NAME, key, arrayStr));
					
					pos = arrayEnd + 1;
					while (pos < content.length() && 
						   (content.charAt(pos) == ',' || content.charAt(pos) == ' ')) {
						pos++;
					}
				}
			}
		} catch (Exception e) {
			log.log(Level.WARNING, "Restore failed", e);
		}
	}

	@Override
	public void close() {
		checkpoint();
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
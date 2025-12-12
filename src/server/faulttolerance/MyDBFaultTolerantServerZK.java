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
 * Custom fault-tolerant replicated database server.
 * Simple leader-based protocol with total ordering.
 */
public class MyDBFaultTolerantServerZK extends server.MyDBSingleServer {

	private static final Logger log = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());

	public static final int SLEEP = 1000;
	public static final boolean DROP_TABLES_AFTER_TESTS = true;
	public static final int MAX_LOG_SIZE = 400;
	public static final int DEFAULT_PORT = 2181;

	private Session session;
	private Cluster cluster;
	private final String myID;
	private final MessageNIOTransport<String, String> serverMessenger;
	private final NodeConfig<String> nodeConfig;
	private String leader;
	private int totalServers = 0;
	private static final String TABLE_NAME = "grade";

	// Sequence tracking - key to total ordering
	private AtomicLong nextSeqToAssign = new AtomicLong(0);
	private AtomicLong nextSeqToExecute = new AtomicLong(0);
	
	// Buffer for out-of-order proposals
	private ConcurrentHashMap<Long, String> proposalBuffer = new ConcurrentHashMap<>();
	
	// Leader state
	private volatile long currentProposalSeq = -1;
	private volatile int acksReceived = 0;
	private final Object proposalLock = new Object();
	
	// Request queue for leader
	private LinkedBlockingQueue<PendingRequest> requestQueue = new LinkedBlockingQueue<>();
	private Thread processorThread;
	private volatile boolean running = true;
	
	// Checkpoint
	private final String checkpointDir = "paxos_logs";
	private final String checkpointFile;
	private final String seqFile;

	private enum MsgType {
		REQUEST, PROPOSAL, ACK
	}

	private static class PendingRequest {
		String cql;
		PendingRequest(String cql) {
			this.cql = cql;
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

		this.cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		this.session = cluster.connect(myID);

		for (String node : nodeConfig.getNodeIDs()) {
			if (leader == null) leader = node;
			totalServers++;
		}

		this.serverMessenger = new MessageNIOTransport<String, String>(myID, nodeConfig,
				new AbstractBytePacketDemultiplexer() {
					@Override
					public boolean handleMessage(byte[] bytes, NIOHeader nioHeader) {
						handleMessageFromServer(bytes, nioHeader);
						return true;
					}
				}, true);

		log.log(Level.INFO, "Server {0} started, leader={1}", new Object[]{myID, leader});

		// Recover from checkpoint
		recoverFromCheckpoint();
		
		// Start processor thread (leader only processes, but all servers can buffer)
		if (myID.equals(leader)) {
			processorThread = new Thread(this::processRequestQueue, "Processor-" + myID);
			processorThread.start();
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
			// Queue for ordered processing
			requestQueue.offer(new PendingRequest(cql));
		} else {
			// Forward to leader
			try {
				JSONObject msg = new JSONObject();
				msg.put("type", MsgType.REQUEST.toString());
				msg.put("cql", cql);
				serverMessenger.send(leader, msg.toString().getBytes());
			} catch (Exception e) {
				log.log(Level.WARNING, "Forward failed", e);
			}
		}

		sendToClient(header.sndr, "[success:" + request + "]");
	}

	private void processRequestQueue() {
		while (running) {
			try {
				PendingRequest req = requestQueue.poll(100, java.util.concurrent.TimeUnit.MILLISECONDS);
				if (req != null) {
					processOneRequest(req.cql);
				}
			} catch (InterruptedException e) {
				break;
			}
		}
	}

	private void processOneRequest(String cql) {
		synchronized (proposalLock) {
			long seqNum = nextSeqToAssign.getAndIncrement();
			currentProposalSeq = seqNum;
			acksReceived = 0;
			
			// Broadcast PROPOSAL to all (including self)
			try {
				JSONObject proposal = new JSONObject();
				proposal.put("type", MsgType.PROPOSAL.toString());
				proposal.put("seqNum", seqNum);
				proposal.put("cql", cql);

				byte[] data = proposal.toString().getBytes();
				for (String node : nodeConfig.getNodeIDs()) {
					try {
						serverMessenger.send(node, data);
					} catch (Exception e) {
						// Node may be down
					}
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, "Broadcast failed", e);
				return;
			}
			
			// Wait for majority ACKs
			int majority = (totalServers / 2) + 1;
			long deadline = System.currentTimeMillis() + 5000;
			
			while (acksReceived < majority && System.currentTimeMillis() < deadline) {
				try {
					proposalLock.wait(100);
				} catch (InterruptedException e) {
					break;
				}
			}
			
			// Checkpoint periodically
			if (seqNum > 0 && seqNum % (MAX_LOG_SIZE / 2) == 0) {
				checkpoint();
			}
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
					requestQueue.offer(new PendingRequest(cql));
				}

			} else if (type.equals(MsgType.PROPOSAL.toString())) {
				long seqNum = json.getLong("seqNum");
				String cql = json.getString("cql");
				
				// Buffer and execute in order
				proposalBuffer.put(seqNum, cql);
				executeBufferedProposals();
				
				// Keep sequence numbers in sync
				if (seqNum >= nextSeqToAssign.get()) {
					nextSeqToAssign.set(seqNum + 1);
				}
				
				// Send ACK
				try {
					JSONObject ack = new JSONObject();
					ack.put("type", MsgType.ACK.toString());
					ack.put("seqNum", seqNum);
					ack.put("from", myID);
					serverMessenger.send(leader, ack.toString().getBytes());
				} catch (Exception e) {}

			} else if (type.equals(MsgType.ACK.toString())) {
				if (myID.equals(leader)) {
					long seqNum = json.getLong("seqNum");
					synchronized (proposalLock) {
						if (seqNum == currentProposalSeq) {
							acksReceived++;
							proposalLock.notifyAll();
						}
					}
				}
			}

		} catch (JSONException e) {
			log.log(Level.WARNING, "Parse error", e);
		}
	}

	/**
	 * Execute proposals in strict sequence order from buffer.
	 */
	private synchronized void executeBufferedProposals() {
		while (true) {
			long next = nextSeqToExecute.get();
			String cql = proposalBuffer.remove(next);
			if (cql == null) break;
			
			try {
				session.execute(cql);
			} catch (Exception e) {
				log.log(Level.WARNING, "Execute failed: " + cql, e);
			}
			nextSeqToExecute.incrementAndGet();
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
		} catch (IOException e) {}
	}

	private void checkpoint() {
		try {
			String data = createCheckpointData();
			Files.write(Paths.get(checkpointFile), data.getBytes());
			Files.write(Paths.get(seqFile), 
					(nextSeqToAssign.get() + ":" + nextSeqToExecute.get()).getBytes());
		} catch (IOException e) {}
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
				log.log(Level.INFO, "{0} recovered seq={1}/{2}", 
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
		running = false;
		checkpoint();
		if (processorThread != null) processorThread.interrupt();
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
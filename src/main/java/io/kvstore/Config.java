package main.java.io.kvstore;

public class Config {
	public final static int[] PORT_COORDINATOR = {55555, 55556, 55557, 55558, 55559};
	
	public final static int[] PORT_STORE = {55560, 55561, 55562, 55563, 55564};
	
	public final static int REPLICATION_FACTOR = 5;
	
	public final static int WRITE_QUORUM = REPLICATION_FACTOR / 2 + 1;
	
	public final static int READ_QUORUM = REPLICATION_FACTOR - WRITE_QUORUM + 1;
	
	public final static String HASHING_ALGORITHM = "SHA-512";
}

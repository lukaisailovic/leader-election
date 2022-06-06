package domaci2;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class LeaderElection implements Watcher {
    String connectionString;
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    ZooKeeper zk;
    boolean isLeader = false;
    Vote myVote = null;
    String id;

    public LeaderElection(int id, String connectionString) {
        Random r = new Random();
        this.id = ((1 + r.nextInt(2)) * 10000 + r.nextInt(10000)) + "_" + id;
        this.connectionString = connectionString;
    }

    private byte[] getIdBytes() {
        return this.id.getBytes();
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getPath() == null) {
            return;
        }
        log("NEW EVENT " + event.getPath());
        if (event.getPath().equals("/leader") && !isLeader) {
            try {
                log("Nova vrednost /leader cvora: " + this.read("/leader", false));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (event.getPath().contains("/votes") && isLeader) {
            try {
                List<String> children = this.getVotes();
                log("Neko je glasao, trenutni broj glasova: " + children.size());
                if (children.size() == 3) {
                    this.processVotes(children);
                    this.zk.setData("/leader", "end".getBytes(), 0);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void start() throws IOException {
        this.zk = new ZooKeeper(this.connectionString, 500, this);
    }

    public void stop() throws InterruptedException {
        zk.close();
        System.out.println("Exiting...");
    }

    private String read(String path, boolean watch) throws KeeperException, InterruptedException {
        byte[] content = zk.getData(path, watch, null);
        return new String(content);
    }

    private List<String> getVotes() throws InterruptedException, KeeperException {
        return this.zk.getChildren("/votes", true);
    }

    private void vote() throws InterruptedException {
        Random r = new Random();
        Vote value = Vote.YES;
        if (r.nextBoolean()) {
            value = Vote.NO;
        }
        this.myVote = value;
        try {
            String path = zk.create("/votes/vote-", value.toString().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            log("Glasao sa " + this.read(path, false));
        } catch (KeeperException e) {

        }
    }

    private Vote processVotes(List<String> votingNodes) throws InterruptedException, KeeperException {

        Map<Vote,Integer> votes = new HashMap<>();
        votes.put(Vote.YES,0);
        votes.put(Vote.NO,0);
        for (String node :
                votingNodes) {
            Vote vote = Vote.valueOf(this.read("/votes/"+node, false));
            Integer current = votes.get(vote);
            votes.replace(vote,current+1);
        }
        System.out.println(votes);
        if (votes.get(Vote.NO) > votes.get(Vote.YES)){
            return Vote.NO;
        }
        return Vote.YES;
    }

    public void join() throws InterruptedException, KeeperException {
        try {
            zk.create("/leader", "start".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            log("Uspesno sam postao lider");
            this.isLeader = true;
            List<String> votes = this.getVotes();
            log("Ukupno je glasalo: " + votes.size());
            // cekati glasove
        } catch (KeeperException e) {
            String content = this.read("/leader", true);
            log("Leader vec postoji (vrednost=[" + content + "]), glasati");
            // glasati
            this.vote();
        }
    }

    private void log(String message) {
        Calendar cal = Calendar.getInstance();
        System.out.println("[" + this.id + "]" + " " + "[" + dateFormat.format(cal.getTime()) + "]" + " " + message);
    }
}

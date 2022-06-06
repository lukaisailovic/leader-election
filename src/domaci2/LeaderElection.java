package domaci2;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Random;

public class LeaderElection implements Watcher {
    String connectionString ;
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

    ZooKeeper zk;
    boolean isLeader = false;
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
        System.out.println("NEW EVENT " + event.getPath());
        if (event.getPath().equals("/leader")) {
            try {
                join();
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

    private void read() throws InterruptedException {
        try {
            byte[] content = zk.getData("/leader", true, null);
            System.out.println(this.id + ": " + new String(content));
        } catch (KeeperException.NoNodeException e) {
            // za slucaj da leader cvor nije napravljen ili je nestao
            join();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public void join() throws InterruptedException {
        try {
            zk.create("/leader", getIdBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            log("Uspesno sam postao lider");
            this.isLeader = true;
        } catch (KeeperException e) {
            log("Leader vec postoji, nije moguce kreirati cvor");
            this.read();
        }
    }

    private void log(String message){
        Calendar cal = Calendar.getInstance();
        System.out.println("["+ this.id + "]"+ " " + "["+ dateFormat.format(cal.getTime()) + "]"+ " " + message);
    }
}

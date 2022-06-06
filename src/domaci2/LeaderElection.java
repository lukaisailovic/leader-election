package domaci2;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Random;

public class LeaderElection implements Watcher {

    String connectionString = "localhost:2181";
    byte [] serverId = Integer.toHexString(new Random().nextInt()).getBytes();
    ZooKeeper zk;
    boolean isLeader = false;

    @Override
    public void process(WatchedEvent event) {
        System.out.println("NEW EVENT "+event.getPath());
        if (event.getPath().equals("/leader")){
            try {
                join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public void start() throws IOException {
        this.zk = new ZooKeeper(this.connectionString,500,this);
    }

    public void stop() throws InterruptedException{
        zk.close();
        System.out.println("Exiting...");
    }

    private void read() throws InterruptedException {
        try {
            byte[] content = zk.getData("/leader", true, null);
            System.out.println(serverId + ": " + new String(content));
        } catch (KeeperException e) {
            // za slucaj da leader cvor nije napravljen ili je nestao
            join();
        }
    }
    public void join() throws InterruptedException{
        try{
            zk.create("/leader",serverId, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);
            System.out.println("Uspesno sam postao leader");
            this.isLeader = true;
        }catch (KeeperException e){
            System.out.println("Leader vec postoji, nije moguce kreirati cvor");
            this.read();
        }
    }
}

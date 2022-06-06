package domaci2;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
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
        if (event == null){
            return;
        }
        log("NEW EVENT " + event.getPath());
        if (event.getPath().equals("/leader")) {
            try {
                join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (event.getPath().equals("/votes") && isLeader) {
            try {
                List<String> children = this.zk.getChildren("/votes",false);
                log("Neko je glasao, trenutni broj glasova: " + children.size());
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

    private String read(String path) throws KeeperException, InterruptedException {
        byte[] content = zk.getData(path, true, null);
        return new String(content);
    }

    private void vote() throws InterruptedException{
        Random r = new Random();
        String value = "yes";
        if (r.nextBoolean()){
            value = "no";
        }
        try {
            String path = zk.create("/votes/vote-",value.getBytes() , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            log("Glasao sa "+this.read(path));
        } catch (KeeperException e) {

        }
    }

    public void join() throws InterruptedException {
        try {
            zk.create("/leader", "start".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            log("Uspesno sam postao lider");
            this.isLeader = true;
            // cekati glasove
        } catch (KeeperException e) {
            log("Leader vec postoji, nije moguce kreirati cvor");
            // glasati
            this.vote();
        }
    }

    private void log(String message){
        Calendar cal = Calendar.getInstance();
        System.out.println("["+ this.id + "]"+ " " + "["+ dateFormat.format(cal.getTime()) + "]"+ " " + message);
    }
}

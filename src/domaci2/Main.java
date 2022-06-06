package domaci2;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


public class Main {
    static String connectionString = "localhost:2181";
    public static void main(String[] args) throws Exception {
        ch.qos.logback.classic.Logger root =
                (ch.qos.logback.classic.Logger)
                LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(ch.qos.logback.classic.Level.OFF);


        List<LeaderElection> leaderElections = new ArrayList<>();

        createEnvironment();

        for (int i = 0; i < 4; i++) {
            leaderElections.add(new LeaderElection(i,connectionString));
        }
        for (LeaderElection  le : leaderElections){
            le.start();
            le.join();
        }
        Thread.sleep(5000);


    }
    private static void createEnvironment() throws Exception {
        ZooKeeper zk = new ZooKeeper(connectionString, 500, watchedEvent -> {});
        try {
            byte[] content = zk.getData("/votes",false, null);
            System.out.println("ENV SETUP: Node /votes is available (content=["+new String(content)+"])");
        } catch (KeeperException.NoNodeException e){
            System.out.println("ENV SETUP: Node /votes not available, creating it...");
            zk.create("/votes","voting".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println("ENV SETUP: Node /votes has been created successfully");
        } catch (KeeperException e){
            e.printStackTrace();
        }
        zk.close();

    }
}

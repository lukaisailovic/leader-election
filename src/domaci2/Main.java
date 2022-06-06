package domaci2;

import org.slf4j.LoggerFactory;



public class Main {

    public static void main(String[] args) throws Exception {
        ch.qos.logback.classic.Logger root =
                (ch.qos.logback.classic.Logger)
                LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        root.setLevel(ch.qos.logback.classic.Level.OFF);



        LeaderElection le = new LeaderElection();
        le.start();
        le.join();

        Thread.sleep(7000);
        le.stop();
    }
}

package nrabbitmq.example;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.rabbitmq.client.Connection;

public class ConnectionKiller {
    private Connection con;

    public ConnectionKiller(Connection con) {
        this.con = con;
    }

    public void run() {
        sleep(10);
        System.out.println("コネクションキラーが目覚めた＼(^o^)／");

        while (true) {
            int sec = new Random().nextInt(5);

            sleep(sec);

            if (sec == 3) {
                try {
                    con.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                System.out.println("コネクションきっちゃった♪");
            }
        }
    }

    private static void sleep(int sec) {
        try {
            TimeUnit.SECONDS.sleep(sec);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

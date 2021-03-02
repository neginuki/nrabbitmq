package nrabbitmq.example;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/**
 * 特定のキュー（QUEUE_NAME）にメッセージをランダムに送信して、刈り取っていくだけ。
 * 
 * コネクションを切られたとき、どうなるかを見たいので、
 * 開始後数秒（ランダム）後にコネクションをキルしてみた。
 *  
 * 
 * Rabbit-MQ が起動してること前提 
 * 
 * 起動・・・docker-compose up -d
 * 停止・・・docker-compose down
 * 
 * http://localhost:15672/
 * 
 */
public class HelloRabbit {

    private static final String QUEUE_NAME = "oreore-queue";

    public static void main(String[] args) throws Exception {

        // consumer を登録
        AlohaConsumer consumer = new AlohaConsumer();
        consumer.addConsumer(QUEUE_NAME, (consumerTag, delivery) -> {
            String message = new String(delivery.getBody());

            System.out.println("*** " + message + " ***");
        });

        // consumer は最初にどんどん登録していくのがいいのかな？
        // こんな感じでまったく同じQUEUEに２個め登録すると順番で捌かれた
        // これを実際は exchange や topic 使って routingkey でグループ分けするのかな？
        //
        //        consumer.addConsumer(QUEUE_NAME, (consumerTag, delivery) -> {
        //            String message = new String(delivery.getBody());
        //
        //            System.out.println("@@@ " + message + " @@@");
        //        });

        // あとはメッセージが送信されてくるのをまって随時処理するだけ。

        // テストなので送信はぐるぐる無限ループしてN秒単位で送りまくる
        AlohaProducer producer = new AlohaProducer(QUEUE_NAME);

        newThread(() -> new ConnectionKiller(consumer.getConnection()).run());

        while (true) {
            int sec = new Random().nextInt(10);

            sleep(sec);

            producer.publish("そーしん (" + sec + "秒まった)");
        }

        // consumer.destroy(); // これを呼ぶと consumer 止まる（channel では止まらず、 con.close で止まる。 JDBC もそうだっけ？？）
    }

    public static class AlohaProducer {

        private String queueName;

        public AlohaProducer(String queueName) {
            this.queueName = queueName;
        }

        public void publish(String message) {
            // 実際は publish するときはコネクションは pooling したほうがよい。 java だと Spring AMQP しかない？           
            try (Connection con = MqConnectionFactory.getConnectionFactory().newConnection(); Channel channel = con.createChannel()) {
                channel.basicPublish("", queueName, null, message.getBytes());
            } catch (Exception e) {
                throw new RuntimeException("コネクションとれず", e);
            }

        }
    }

    public static class AlohaConsumer {
        private Connection con;
        private Channel channel;

        public AlohaConsumer() {
            try {
                ConnectionFactory factory = MqConnectionFactory.getConnectionFactory();
                System.out.println(factory.isAutomaticRecoveryEnabled()); // 元々の設定を確認

                factory.setAutomaticRecoveryEnabled(true); // これを有効にしたら自動リカバリするのん？

                con = factory.newConnection();
                channel = con.createChannel();

                con.addShutdownListener(cause -> {
                    System.out.println("シャットダウン検知しました。" + cause);
                });
            } catch (Exception e) {
                throw new RuntimeException("コネクションとれず", e);
            }
        }

        public void addConsumer(String queueName, DeliverCallback callback) {
            try {
                channel.queueDeclare(queueName, false, false, false, null);
                channel.basicConsume(queueName, true, callback, consumerTag -> {
                });
            } catch (Exception e) {
                throw new RuntimeException("ちゃんねるがつかえねー", e);
            }
        }

        public Connection getConnection() {
            return con;
        }

        public void destroy() {
            try {
                channel.close();
                con.close();
            } catch (IOException | TimeoutException e) {
                throw new RuntimeException("ちゃんねるがおわんねー", e);
            }
        }
    }

    public static void newThread(Runnable runnable) {
        ExecutorService es = Executors.newFixedThreadPool(1);
        es.submit(runnable);
    }

    private static void sleep(int sec) {
        try {
            TimeUnit.SECONDS.sleep(sec);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

package nrabbitmq.example;

import com.rabbitmq.client.ConnectionFactory;

public class MqConnectionFactory {

    public static ConnectionFactory getConnectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setPort(5672);
        //ユーザー指定する場合は明示する必要があるようだ（無いと guest になる）
        //factory.setUsername("guest");
        //factory.setPassword("guest");       

        return factory;
    }
}

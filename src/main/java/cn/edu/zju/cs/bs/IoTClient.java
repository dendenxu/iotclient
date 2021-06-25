package cn.edu.zju.cs.bs;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Vector;
import java.nio.file.Paths;
import java.nio.file.Path;

public class IoTClient {
    public static void main(String[] args) {
        int devices = 1;
        String mqttServer = "tcp://localhost:1883";
        String topic = "testapp";
        String lastwilltopic = "testapp/lastwill";
        String connecttopic = "testapp/connect";
        String disconnecttopic = "testapp/disconnect";
        String clientPrefix = "device";

        Path curDir = Paths.get(".").toAbsolutePath();
        System.out.printf("[CLIENT] Getting current dir: \"%s\"\n", curDir.toString());

        try {
            Properties properties = new Properties();
            FileInputStream in = new FileInputStream("iot.properties");
            // Snapshot of the iot.properties file
            // devices=5
            // server=tcp://localhost:1883
            // topic=testapp
            // prefix=device
            properties.load(in);
            devices = Integer.parseInt(properties.getProperty("devices"));
            mqttServer = properties.getProperty("server");
            topic = properties.getProperty("topic");
            lastwilltopic = properties.getProperty("lastwilltopic");
            connecttopic = properties.getProperty("connecttopic");
            disconnecttopic = properties.getProperty("disconnecttopic");
            clientPrefix = properties.getProperty("prefix");

            Vector<WorkerThread> threadVector = new Vector<WorkerThread>();
            for (int i = 0; i < devices; i++) {
                WorkerThread thread = new WorkerThread();
                thread.setDeviceId(i);
                thread.setMqttServer(mqttServer);
                thread.setTopic(topic);
                thread.setLastwilltopic(lastwilltopic);
                thread.setConnecttopic(connecttopic);
                thread.setDisconnecttopic(disconnecttopic);
                thread.setClientPrefix(clientPrefix);
                threadVector.add(thread);
                thread.start();
            }
            for (WorkerThread thread : threadVector) {
                thread.join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

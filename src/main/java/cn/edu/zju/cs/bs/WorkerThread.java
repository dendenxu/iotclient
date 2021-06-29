package cn.edu.zju.cs.bs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import com.alibaba.fastjson.JSONObject;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class WorkerThread extends Thread {
    private boolean threading = true;
    private int deviceId;
    private String mqttServer;
    private String topic;
    private String lastwilltopic;
    private String connecttopic;
    private String disconnecttopic;
    private String clientPrefix;
    private float sleepBound = 10; // upper bound for the thread to wait between sending next message, in seconds

    public void run() {
        try {
            Random rand = new Random();
            MemoryPersistence persistence = new MemoryPersistence();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
            String clientId;
            String content;
            int qos = 2;
            clientId = clientPrefix + String.format("%04d", deviceId);
            MqttClient mqttClient = new MqttClient(mqttServer, clientId, persistence);
            MqttConnectOptions connOpts = new MqttConnectOptions();
            // With a non persistent connection the broker doesn’t store any subscription
            // information or undelivered messages for the client.
            connOpts.setCleanSession(true);
            connOpts.setMaxInflight(1024);
            IoTMessage msg = new IoTMessage();
            msg.setClientId(clientId);
            msg.setInfo("Unexpected disconnect from the broker");
            connOpts.setWill(lastwilltopic, JSONObject.toJSONString(msg).getBytes(), qos, true);
            while (threading) {
                System.out.println("Connecting to broker: " + mqttServer);
                mqttClient.connect(connOpts);
                System.out.println("Connected " + clientId);
                boolean firstmessage = true;
                boolean lastmessage = false;
                boolean running = true;
                while (running || lastmessage) {
                    // 随机等待1秒
                    float interval = rand.nextFloat();
                    Thread.sleep((long) Math.round(interval * 1000 * sleepBound));

                    Date now = new Date();
                    int value = rand.nextInt(100);
                    IoTMessage payload = new IoTMessage();
                    payload.setClientId(clientId);
                    payload.setInfo("Device Data " + sdf.format(now));
                    payload.setValue(value);
                    // 超过80告警
                    payload.setAlert(value > 80 ? 1 : 0);
                    rand.nextFloat();
                    // 根据杭州经纬度随机生成设备位置信息
                    payload.setLng(119.9 + rand.nextFloat() * 0.6);
                    payload.setLat(30.1 + rand.nextFloat() * 0.4);
                    payload.setTimestamp(now.getTime());
                    content = JSONObject.toJSONString(payload);
                    String curTopic = lastmessage ? disconnecttopic : (firstmessage ? connecttopic : topic);
                    System.out.println("Publishing message: " + content + " to topic: " + curTopic);
                    MqttMessage message = new MqttMessage(content.getBytes());
                    message.setQos(qos);
                    mqttClient.publish(curTopic, message);
                    System.out.println("Message published");
                    firstmessage = false;
                    if (lastmessage) {
                        running = false;
                        lastmessage = false;
                    } else {
                        // 0.1 to disconnect
                        float RR = rand.nextFloat();
                        if (RR < 0.1) {
                            lastmessage = true;
                        }
                    }
                }
                mqttClient.disconnect();
                System.out.println("Disconnected " + clientId);
            }
            mqttClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

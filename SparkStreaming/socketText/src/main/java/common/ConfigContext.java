package common;

import java.io.Serializable;

/**
 * Created by lijingxiao on 2018/10/24.
 */
public class ConfigContext implements Serializable {
    private String groupId;
    private String topic;
    private String brokerList;
    private String zkQuorum;

    private static ConfigContext configContext;

    public static ConfigContext getConfigContext() {
        return configContext;
    }

    public static void setConfigContext(ConfigContext configContext) {
        ConfigContext.configContext = configContext;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }

    public String getZkQuorum() {
        return zkQuorum;
    }

    public void setZkQuorum(String zkQuorum) {
        this.zkQuorum = zkQuorum;
    }

    /**
     *         //指定消费的 topic 名字
     String topic = "wwcc";
     //指定kafka的broker地址(sparkStream的Task直连到kafka的分区上，用更加底层的API消费，效率更高)
     String brokerList = "node-4:9092,node-5:9092,node-6:9092";
     //指定zk的地址，后期更新消费的偏移量时使用(以后可以使用Redis、MySQL来记录偏移量)
     String zkQuorum = "node-1:2181,node-2:2181,node-3:2181";
     //指定组名
     String group = "g001";

     HashMap<String, String> kafkaParam = new HashMap<>();
     kafkaParam.put("group.id", group);
     kafkaParam.put("metadata.broker.list", brokerList);
     kafkaParam.put("auto.offset.reset", "largest");
     */
}

package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import net.yychildren.middleware.mwpreliminary.spout.MetaClientConfig;
import net.yychildren.middleware.mwpreliminary.spout.MetaSpout;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

// import com.alibaba.middleware.race.RaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.alibaba.middleware.race.RaceConfig;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);


    public static void main(String[] args) throws Exception {

        Map conf = config();
        int spout_Parallelism_hint = 1;
        int split_Parallelism_hint = 2;
        int count_Parallelism_hint = 2;

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("taobao_spout", new MetaSpout(RaceConfig.MqTaobaoTradeTopic), spout_Parallelism_hint);
        builder.setSpout("tmall_spout", new MetaSpout(RaceConfig.MqTmallTradeTopic), spout_Parallelism_hint);
        builder.setSpout("pay_spout", new MetaSpout(RaceConfig.MqPayTopic), spout_Parallelism_hint);
        
        
        // builder.setBolt("split", new SplitSentence(), split_Parallelism_hint).shuffleGrouping("spout");
        // builder.setBolt("count", new WordCount(), count_Parallelism_hint).fieldsGrouping("split", new Fields("word"));

        try {
            String topologyName = RaceConfig.JstormTopologyName;
            StormSubmitter.submitTopology(topologyName , conf, builder.createTopology());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    private static Map config(){
    	Yaml yaml = new Yaml();
    	Map conf = null;
		InputStream stream = ClassLoader.getSystemResourceAsStream("topology.yaml");
		conf = (Map) yaml.load(stream);
		if (conf == null || conf.isEmpty() == true) {
			throw new RuntimeException("Failed to read config file");
		}
    	return conf;
    }
}
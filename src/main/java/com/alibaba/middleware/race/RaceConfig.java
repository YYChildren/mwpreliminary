package com.alibaba.middleware.race;

import java.io.Serializable;

@SuppressWarnings("serial")
public class RaceConfig implements Serializable {
	
	public static String Teamcode = "43676k21xf";

    //这些是写tair key的前缀
	public static String prex_taobao = "platformTaobao_" + Teamcode + "_";
	public static String prex_tmall = "platformTmall_" + Teamcode + "_";
    public static String prex_ratio = "ratio_" + Teamcode + "_";

    
    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    // jstorm
    public static String JstormTopologyName = "43676k21xf";
    public static String MetaConsumerGroup = "43676k21xf";
    
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    

    // tair
    public static String TairConfigServer = "10.101.72.127:5198";
    public static String TairSalveConfigServer = "10.101.72.128:5198";
    public static String TairGroup = "group_tianchi";
    public static Integer TairNamespace = 13361;
}

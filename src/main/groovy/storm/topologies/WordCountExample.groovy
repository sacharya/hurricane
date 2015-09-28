package storm.topologies

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.StormSubmitter
import backtype.storm.generated.StormTopology
import backtype.storm.topology.TopologyBuilder
import storm.bolts.PrinterBolt
import storm.bolts.SplitSentenceBolt
import storm.bolts.WordCountBolt
import storm.kafka.ZkHosts;
import storm.kafka.KafkaSpout
import storm.kafka.SpoutConfig
import storm.kafka.BrokerHosts
import storm.kafka.StringScheme

import backtype.storm.spout.SchemeAsMultiScheme;

/**
 * The simplest example of getting Storm running
 *
 * Taken from the Storm Starter: https://github.com/nathanmarz/storm-starter
 */

public class WordCountExample {
    static main( args ) {
        println "Entering WordCountExample "
        KafkaSpout objectKafkaSpout = getKafkaSpout("object_events");
        KafkaSpout syncKafkaSpout = getKafkaSpout("sync_events");

        StormTopology topology = new TopologyBuilder().with {
            //setSpout( 'spout', new RandomSentenceSpout(), 5  )
            setSpout( 'objectSpout', objectKafkaSpout, 5  )
            setSpout( 'syncSpout', syncKafkaSpout, 5  )
            setBolt(  'split', new SplitSentenceBolt(),   8  ).shuffleGrouping( 'objectSpout').shuffleGrouping('syncSpout')
            setBolt(  'count', new WordCountBolt(),       12 ).shuffleGrouping( 'split')
            setBolt(  'print', new PrinterBolt(), 15).shuffleGrouping( 'count')
            createTopology()
        }

        Config conf = new Config()
        conf.debug = true

        if( args ) {
            conf.numWorkers = 3
            StormSubmitter.submitTopology( args[ 0 ], conf, topology )
        }
        else {
            conf.maxTaskParallelism = 3

            LocalCluster cluster = new LocalCluster()
            print "Submitting"
            cluster.submitTopology( 'word-count', conf, topology )
            print "Submitted"
            Thread.sleep( 10000 )

            cluster.shutdown()
        }
    }

    private static KafkaSpout getKafkaSpout(String topic) {
        String brokerZkStr = "0.0.0.0:2181";
        BrokerHosts hosts = new ZkHosts(brokerZkStr);
        SpoutConfig kafkaConf = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString())
        kafkaConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf)
        println kafkaSpout
        return kafkaSpout
    }
}








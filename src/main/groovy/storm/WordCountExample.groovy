package storm

import backtype.storm.Config
import backtype.storm.LocalCluster
import backtype.storm.StormSubmitter
import backtype.storm.generated.StormTopology
import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.TopologyBuilder
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values
import storm.kafka.ZkHosts;
import storm.kafka.KafkaSpout
import storm.kafka.SpoutConfig
import storm.kafka.BrokerHosts

import java.util.UUID

/**
 * The simplest example of getting Storm running
 *
 * Taken from the Storm Starter: https://github.com/nathanmarz/storm-starter
 */

public class WordCountExample {
    static main( args ) {
        println "Entering WordCountExample "
        String brokerZkStr = "0.0.0.0:2181";
        String topic = "test";
        BrokerHosts hosts = new ZkHosts(brokerZkStr);

        SpoutConfig kafkaConf = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString())
        println kafkaConf
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConf)
        println kafkaSpout
        StormTopology topology = new TopologyBuilder().with {
            //setSpout( 'spout', new RandomSentenceSpout(), 5  )
            setSpout( 'spout', kafkaSpout, 5  )
            setBolt(  'split', new SplitSentenceBolt(),   8  ).shuffleGrouping( 'spout')
            setBolt(  'count', new WordCountBolt(),       12 ).fieldsGrouping( 'split', new Fields( 'word' ) )
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
            cluster.submitTopology( 'word-count', conf, topology )
        
            Thread.sleep( 10000 )

            cluster.shutdown()
        }
    }
}

class SplitSentenceBolt extends BaseBasicBolt {
    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declare( new Fields( 'word' ) )
    }

    @Override
    public void execute( Tuple tuple, BasicOutputCollector collector ) {
        println(SplitSentenceBolt)
        String line = tuple.getString( 0 )
        line.split().each { word ->
            collector.emit( new Values( word ) )
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null
    }
}

class WordCountBolt extends BaseBasicBolt {
    // Can't use withDefault { 0 } as it is then not Serializable
    Map<String, Integer> counts = [:]

    @Override
    public void execute( Tuple tuple, BasicOutputCollector collector ) {
        println(WordCountBolt)
        String word = tuple.getString( 0 )
        Integer count = counts[ word ] ?: 0
        counts[ word ] = ++count
        collector.emit( new Values( word, count ) )
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare( new Fields( 'word', 'count' ) )
    }
}

class PrinterBolt extends BaseBasicBolt {

    @Override
    public void execute( Tuple tuple, BasicOutputCollector collector ) {
        try {
            println(PrinterBolt)
            BufferedWriter output;
            output = new BufferedWriter(new FileWriter("/tmp/test.txt", true));
            output.newLine();
            output.append(tuple.toString());
            output.close();
	 } catch (IOException e) {
             e.printStackTrace();
	 } 
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare( new Fields( 'word', 'count' ) )
    }
}

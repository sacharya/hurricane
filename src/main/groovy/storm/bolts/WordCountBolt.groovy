package storm.bolts

import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values

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

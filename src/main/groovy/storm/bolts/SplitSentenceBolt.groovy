package storm.bolts

import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple
import backtype.storm.tuple.Values

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

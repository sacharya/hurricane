package storm.spouts

import backtype.storm.spout.SpoutOutputCollector
import backtype.storm.task.TopologyContext
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseRichSpout
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Values
import backtype.storm.utils.Utils

class RandomSentenceSpout extends BaseRichSpout {
    SpoutOutputCollector collector
    Random rand
    List sentences = [ 'the cow jumped over the moon',
                       'an apple a day keeps the doctor away',
                       'four score and seven years ago',
                       'snow white and the seven dwarfs',
                       'i am at two with nature']

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector
        this.rand = new Random()
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100)
        collector.emit( new Values( sentences[ rand.nextInt( sentences.size() ) ] ) )
    }
    @Override
    public void ack( Object id ) { }

    @Override
    public void fail( Object id ) { }

    @Override
    public void declareOutputFields( OutputFieldsDeclarer declarer ) {
        declarer.declare( new Fields( 'word' ) )
    }
}

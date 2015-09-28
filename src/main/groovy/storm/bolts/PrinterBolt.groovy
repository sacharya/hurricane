package storm.bolts

import backtype.storm.topology.BasicOutputCollector
import backtype.storm.topology.OutputFieldsDeclarer
import backtype.storm.topology.base.BaseBasicBolt
import backtype.storm.tuple.Fields
import backtype.storm.tuple.Tuple

class PrinterBolt extends BaseBasicBolt {

    @Override
    public void execute( Tuple tuple, BasicOutputCollector collector ) {
        try {
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

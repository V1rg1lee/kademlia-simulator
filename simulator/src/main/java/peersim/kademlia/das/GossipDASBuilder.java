package peersim.kademlia.das;

import java.util.Arrays;
import peersim.config.Configuration;
import peersim.kademlia.Message;
import peersim.kademlia.gossipsub.GossipCommonConfig;
import peersim.kademlia.gossipsub.GossipSubProtocol;

public class GossipDASBuilder extends GossipDAS {

  protected boolean started;

  public GossipDASBuilder(String prefix) {
    super(prefix);
    started = false;
    bw = Configuration.getInt(prefix + "." + PAR_BW, KademliaCommonConfigDas.BUILDER_UPLOAD_RATE);
  }

  @Override
  public Object clone() {
    GossipDASBuilder dolly = new GossipDASBuilder(GossipDASBuilder.prefix);
    return dolly;
  }

  protected void handleInitNewBlock(Message m, int myPid) {
    currentBlock = (Block) m.body;
    logger.fine("Builder Init block");

    if (!started) {
      started = true;

      if (GossipCommonConfig.singleTopicEnabled) {
        String topic = GossipCommonConfig.singleTopicName;
        gossipsub.Join(topic);
        GossipSubProtocol.registerBootstrapPeer(topic, gossipsub.getGossipNode().getId());
      } else {
        for (int j = 1; j <= KademliaCommonConfigDas.BLOCK_DIM_SIZE; j++) {
          String topic = "Row" + j;
          gossipsub.Join(topic);
          GossipSubProtocol.registerBootstrapPeer(topic, gossipsub.getGossipNode().getId());
          topic = "Column" + j;
          gossipsub.Join(topic);
          GossipSubProtocol.registerBootstrapPeer(topic, gossipsub.getGossipNode().getId());
        }
      }
    } else {
      for (int i = 1; i <= KademliaCommonConfigDas.BLOCK_DIM_SIZE; i++) {
        Sample[] samples = currentBlock.getSamplesByRow(i);
        String topic =
            GossipCommonConfig.singleTopicEnabled ? GossipCommonConfig.singleTopicName : "Row" + i;
        Message msg =
            Message.makePublishMessage(topic, Arrays.copyOfRange(samples, 0, samples.length / 2));
        msg.src = this.gossipsub.node;
        gossipsub.Publish(msg, myPid);

        samples = currentBlock.getSamplesByColumn(i);
        topic =
            GossipCommonConfig.singleTopicEnabled
                ? GossipCommonConfig.singleTopicName
                : "Column" + i;
        msg = Message.makePublishMessage(topic, Arrays.copyOfRange(samples, 0, samples.length / 2));
        msg.src = this.gossipsub.node;
        gossipsub.Publish(msg, myPid);
      }
    }
  }

  @Override
  protected void handleGetSample(Message m, int myPid) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'handleGetSample'");
  }

  @Override
  protected void handleGetSampleResponse(Message m, int myPid) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'handleGetSampleResponse'");
  }

  @Override
  public void messageReceived(Message m) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'messageReceived'");
  }
}

package peersim.kademlia;

import peersim.core.CommonState;
import peersim.core.Control;

/**
 * A control that forces the KademliaObserver to write out logs at the end of the simulation. This
 * is a workaround for the issue where KademliaObserver.execute() condition is never satisfied.
 *
 * <p>With PeerSim schedulers, a control configured with a {@code step} also runs at time {@code 0}.
 * This writer must therefore stay inert during the main simulation phase and only fire when the
 * experiment is effectively over.
 */
public class FinalLogWriter implements Control {
  public FinalLogWriter(String prefix) {
    // Constructor required by PeerSim configuration framework
  }

  public boolean execute() {
    if (CommonState.getPhase() != CommonState.POST_SIMULATION
        && CommonState.getTime() + 1 < CommonState.getEndTime()) {
      return false;
    }
    // Force writing out logs
    KademliaObserver.writeOut();
    return false;
  }
}

package phrasecount;

import com.google.common.base.Optional;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.observer.Observer.Context;
import io.fluo.recipes.export.ExportQueue;
import io.fluo.recipes.map.UpdateObserver;

public class PcmUpdateObserver extends UpdateObserver<String, PhraseCounts>{

  private ExportQueue<String, PhraseCounts> pcEq;

  @Override
  public void init(String mapId, Context observerContext) throws Exception {
    pcEq = ExportQueue.getInstance("aeq", observerContext.getAppConfiguration());
  }

  @Override
  public void updatingValue(TransactionBase tx, String key, Optional<PhraseCounts> oldValue,
      PhraseCounts newValue) {
    pcEq.add(tx, key, newValue);
  }
}

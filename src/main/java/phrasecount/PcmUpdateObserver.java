package phrasecount;

import java.util.Iterator;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import io.fluo.api.client.TransactionBase;
import io.fluo.api.observer.Observer.Context;
import io.fluo.recipes.export.Export;
import io.fluo.recipes.export.ExportQueue;
import io.fluo.recipes.map.Update;
import io.fluo.recipes.map.UpdateObserver;

public class PcmUpdateObserver extends UpdateObserver<String, PhraseCounts>{

  private ExportQueue<String, PhraseCounts> pcEq;

  @Override
  public void init(String mapId, Context observerContext) throws Exception {
    pcEq = ExportQueue.getInstance("aeq", observerContext.getAppConfiguration());
  }

  @Override
  public void updatingValues(TransactionBase tx, Iterable<Update<String, PhraseCounts>> updates) {
    Iterator<Export<String, PhraseCounts>> exports = Iterators.transform(updates.iterator(), new Function<Update<String, PhraseCounts>, Export<String, PhraseCounts>>() {
      @Override
      public Export<String, PhraseCounts> apply(Update<String, PhraseCounts> update) {
        return new Export<String, PhraseCounts>(update.getKey(), update.getNewValue().get());
      }});

    pcEq.addAll(tx, exports);
  }
}

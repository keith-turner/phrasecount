package phrasecount;

import java.util.Iterator;

import io.fluo.api.client.TransactionBase;
import io.fluo.recipes.map.CollisionFreeMap;
import io.fluo.recipes.serialization.KryoSimplerSerializer;

public class PhraseCountMap extends CollisionFreeMap<String, PhraseCounts, PhraseCounts>{

  protected PhraseCountMap() {
    super("pc:", String.class, PhraseCounts.class, PhraseCounts.class, new KryoSimplerSerializer(), new PhraseCounts(0l, 0l));
  }

  @Override
  protected PhraseCounts combine(String key, PhraseCounts currentValue, Iterator<PhraseCounts> updates) {
    PhraseCounts sum = new PhraseCounts(0l, 0l);
    while(updates.hasNext()) {
      sum.add(updates.next());
    }

    sum.add(currentValue);

    return sum;
  }

  @Override
  protected void updatingValue(TransactionBase tx, String key, PhraseCounts oldValue, PhraseCounts currentValue) {
    new PhraseExporter2().getExportQueue(null).add(tx, key, currentValue);
  }
}

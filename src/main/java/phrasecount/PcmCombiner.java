package phrasecount;

import java.util.Iterator;

import com.google.common.base.Optional;

import io.fluo.recipes.map.Combiner;

public class PcmCombiner implements Combiner<String, PhraseCounts, PhraseCounts> {

  @Override
  public PhraseCounts combine(String key, Optional<PhraseCounts> currentValue,
      Iterator<PhraseCounts> updates) {
    PhraseCounts sum = currentValue.or(new PhraseCounts(0, 0));
    while(updates.hasNext()) {
      sum = sum.add(updates.next());
    }

    return sum;
  }

}

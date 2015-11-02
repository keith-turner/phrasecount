package phrasecount;

import java.util.Iterator;

import com.google.common.base.Optional;
import io.fluo.recipes.map.CollisionFreeMap;
import io.fluo.recipes.map.Combiner;
import phrasecount.pojos.Counts;

/**
 * A combiner for the {@link CollisionFreeMap} that stores phrase counts. The
 * {@link CollisionFreeMap} calls this combiner when it lazily updates the counts for a phrase.
 */
public class PcmCombiner implements Combiner<String, Counts, Counts> {

  @Override
  public Optional<Counts> combine(String key, Optional<Counts> currentValue, Iterator<Counts> updates) {
    Counts sum = currentValue.or(new Counts(0, 0));
    while (updates.hasNext()) {
      sum = sum.add(updates.next());
    }

    return Optional.of(sum);
  }
}

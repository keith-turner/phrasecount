package phrasecount.query;

import java.util.Collection;
import java.util.Collections;

import io.fluo.recipes.accumulo.export.AccumuloExport;
import org.apache.accumulo.core.data.Mutation;
import phrasecount.pojos.Counts;

/**
 * This class is queued up on the export queue.
 *
 */
public class PhraseCountsExport extends Counts implements AccumuloExport<String>{

  public PhraseCountsExport(){
    super();
  }

  public PhraseCountsExport(Counts counts){
    super(counts);
  }

  @Override
  public Collection<Mutation> toMutations(String phrase, long seq) {
    return Collections.singletonList(PhraseCountTable.createMutation(phrase, seq, this));
  }

}

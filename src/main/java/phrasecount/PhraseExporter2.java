package phrasecount;

import org.apache.accumulo.core.data.Mutation;

import io.fluo.recipes.accumulo.export.AccumuloExporter;
import io.fluo.recipes.serialization.KryoSimplerSerializer;

public class PhraseExporter2 extends AccumuloExporter<String, PhraseCounts>{

  protected PhraseExporter2() {
    super("pe:", String.class, PhraseCounts.class, new KryoSimplerSerializer());
  }

  @Override
  protected Mutation convert(String key, long seq, PhraseCounts value) {
    // TODO Auto-generated method stub
    return null;
  }

}

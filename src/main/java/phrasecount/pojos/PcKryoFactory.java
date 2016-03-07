package phrasecount.pojos;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.pool.KryoFactory;
import phrasecount.query.PhraseCountsExport;

public class PcKryoFactory implements KryoFactory {
  @Override
  public Kryo create() {
    Kryo kryo = new Kryo();
    kryo.register(Counts.class, 9);
    kryo.register(PhraseCountsExport.class, 10);
    return kryo;
  }
}

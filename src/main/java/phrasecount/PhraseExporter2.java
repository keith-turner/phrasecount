package phrasecount;

import org.apache.accumulo.core.data.Mutation;

import io.fluo.recipes.accumulo.export.AccumuloExporter;

public class PhraseExporter2 extends AccumuloExporter<String, PhraseCounts>{
  @Override
  protected Mutation convert(String phrase, long seq, PhraseCounts pc) {

    Mutation mutation = new Mutation(phrase);

    // use the sequence number for the Accumulo timestamp, this will cause older updates to fall behind newer ones
    if (pc.total == 0)
      mutation.putDelete("stat", "sum", seq);
    else
      mutation.put("stat", "sum", seq, pc.total + "");

    if (pc.documents == 0)
      mutation.putDelete("stat", "docCount", seq);
    else
      mutation.put("stat", "docCount", seq, pc.documents + "");

    return mutation;
  }

}

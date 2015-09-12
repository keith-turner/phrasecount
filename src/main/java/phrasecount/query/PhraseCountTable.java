package phrasecount.query;

import java.util.Iterator;
import java.util.Map.Entry;

import com.google.common.collect.Iterators;
import io.fluo.api.config.FluoConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import phrasecount.pojos.Counts;
import phrasecount.pojos.PhraseAndCounts;

/**
 * All of the code for dealing with the Accumulo table that Fluo is exporting to
 *
 */
public class PhraseCountTable implements Iterable<PhraseAndCounts> {
  public static Mutation createMutation(String phrase, long seq, Counts pc) {
    Mutation mutation = new Mutation(phrase);

    // use the sequence number for the Accumulo timestamp, this will cause older updates to fall
    // behind newer ones
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

  private Connector conn;
  private String table;

  public PhraseCountTable(FluoConfiguration fluoConfig, String table) throws Exception {
    ZooKeeperInstance zki =
        new ZooKeeperInstance(fluoConfig.getAccumuloInstance(), fluoConfig.getAccumuloZookeepers());
    this.conn = zki.getConnector(fluoConfig.getAccumuloUser(),
        new PasswordToken(fluoConfig.getAccumuloPassword()));
    this.table = table;
  }

  public PhraseCountTable(Connector conn, String table) {
    this.conn = conn;
    this.table = table;
  }


  public Counts getPhraseCounts(String phrase) throws Exception {
    Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);
    scanner.setRange(new Range(phrase));

    int sum = 0;
    int docCount = 0;

    for (Entry<Key, Value> entry : scanner) {
      String cq = entry.getKey().getColumnQualifierData().toString();
      if (cq.equals("sum")) {
        sum = Integer.valueOf(entry.getValue().toString());
      }

      if (cq.equals("docCount")) {
        docCount = Integer.valueOf(entry.getValue().toString());
      }
    }

    return new Counts(docCount, sum);
  }

  @Override
  public Iterator<PhraseAndCounts> iterator() {
    try {
      Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);
      scanner.fetchColumn(new Text("stat"), new Text("sum"));
      scanner.fetchColumn(new Text("stat"), new Text("docCount"));

      Iterator<PhraseAndCounts> accumuloIter =
          Iterators.transform(new RowIterator(scanner), new RowTransform());
      return accumuloIter;
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}

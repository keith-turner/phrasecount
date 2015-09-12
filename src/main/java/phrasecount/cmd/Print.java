package phrasecount.cmd;

import java.io.File;
import java.util.Map.Entry;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import phrasecount.Constants;
import phrasecount.pojos.PhraseAndCounts;
import phrasecount.query.PhraseCountTable;

public class Print {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err
          .println("Usage : " + Print.class.getName() + " <fluo props file> <export table name>");
      System.exit(-1);
    }

    FluoConfiguration fluoConfig = new FluoConfiguration(new File(args[0]));

    PhraseCountTable pcTable = new PhraseCountTable(fluoConfig, args[1]);
    for (PhraseAndCounts phraseCount : pcTable) {
      System.out.printf("%7d %7d '%s'\n", phraseCount.documents, phraseCount.total,
          phraseCount.phrase);
    }

    try (FluoClient fluoClient = FluoFactory.newClient(fluoConfig);
        Snapshot snap = fluoClient.newSnapshot()) {

      // TODO could precompute this using observers
      int uriCount = count(snap, "uri:", Constants.DOC_HASH_COL);
      int documentCount = count(snap, "doc:", Constants.DOC_REF_COUNT_COL);
      int numIndexedDocs = count(snap, "doc:", Constants.INDEX_STATUS_COL);

      System.out.println();
      System.out.printf("# uris                : %,d\n", uriCount);
      System.out.printf("# unique documents    : %,d\n", documentCount);
      System.out.printf("# processed documents : %,d\n", numIndexedDocs);
      System.out.println();
    }

    // TODO figure what threads are hanging around
    System.exit(0);
  }

  private static int count(Snapshot snap, String prefix, Column col) throws Exception {
    ScannerConfiguration scanConfig = new ScannerConfiguration();
    scanConfig.setSpan(Span.prefix(prefix));
    scanConfig.fetchColumn(col.getFamily(), col.getQualifier());

    int count = 0;

    RowIterator riter = snap.get(scanConfig);
    while (riter.hasNext()) {
      @SuppressWarnings("unused")
      Entry<Bytes, ColumnIterator> rowEntry = riter.next();
      count++;
    }

    return count;
  }
}

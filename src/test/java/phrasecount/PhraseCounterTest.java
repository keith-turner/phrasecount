package phrasecount;

import java.util.Random;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.types.TypedSnapshot;
import io.fluo.recipes.test.AccumuloExportITBase;
import org.junit.Assert;
import org.junit.Test;
import phrasecount.pojos.Counts;
import phrasecount.pojos.Document;
import phrasecount.query.PhraseCountTable;

import static phrasecount.Constants.DOC_CONTENT_COL;
import static phrasecount.Constants.DOC_REF_COUNT_COL;
import static phrasecount.Constants.TYPEL;

// TODO make this an integration test

public class PhraseCounterTest extends AccumuloExportITBase {

  private PhraseCountTable pcTable;

  @Override
  protected void preFluoInitHook() throws Exception {

    getFluoConfiguration().setApplicationName("phrasecount");
    getFluoConfiguration().setWorkerThreads(5);

    // create the export/query table
    String queryTable = "pcq" + tableCounter.getAndIncrement();

    getAccumuloConnector().tableOperations().create(queryTable);
    pcTable = new PhraseCountTable(getAccumuloConnector(), queryTable);

    // configure phrase count observers
    Application.configure(getFluoConfiguration(), new Application.Options(13, 13, getMiniAccumuloCluster().getInstanceName(),
        getMiniAccumuloCluster().getZooKeepers(), ACCUMULO_USER, ACCUMULO_PASSWORD, queryTable));
  }

  private void loadDocument(FluoClient fluoClient, String uri, String content) {
    try (LoaderExecutor le = fluoClient.newLoaderExecutor()) {
      Document doc = new Document(uri, content);
      le.execute(new DocumentLoader(doc));
    }
    getMiniFluo().waitForObservers();
  }

  @Test
  public void test1() throws Exception {
    try (FluoClient fluoClient = FluoFactory.newClient(getFluoConfiguration())) {

      loadDocument(fluoClient, "/foo1", "This is only a test.  Do not panic. This is only a test.");

      Assert.assertEquals(new Counts(1, 2), pcTable.getPhraseCounts("is only a test"));
      Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("test do not panic"));

      // add new document w/ different content and overlapping phrase.. should change some counts
      loadDocument(fluoClient, "/foo2", "This is only a test");

      Assert.assertEquals(new Counts(2, 3), pcTable.getPhraseCounts("is only a test"));
      Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("test do not panic"));

      // add new document w/ same content, should not change any counts
      loadDocument(fluoClient, "/foo3", "This is only a test");

      Assert.assertEquals(new Counts(2, 3), pcTable.getPhraseCounts("is only a test"));
      Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("test do not panic"));

      // change the content of /foo1, should change counts
      loadDocument(fluoClient, "/foo1", "The test is over, for now.");

      Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("the test is over"));
      Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("is only a test"));
      Assert.assertEquals(new Counts(0, 0), pcTable.getPhraseCounts("test do not panic"));

      // change content of foo2, should not change anything
      loadDocument(fluoClient, "/foo2", "The test is over, for now.");

      Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("the test is over"));
      Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("is only a test"));
      Assert.assertEquals(new Counts(0, 0), pcTable.getPhraseCounts("test do not panic"));

      String oldHash = new Document("/foo3", "This is only a test").getHash();
      try(TypedSnapshot tsnap = TYPEL.wrap(fluoClient.newSnapshot())){
        Assert.assertNotNull(tsnap.get().row("doc:" + oldHash).col(DOC_CONTENT_COL).toString());
        Assert.assertEquals(1, tsnap.get().row("doc:" + oldHash).col(DOC_REF_COUNT_COL).toInteger(0));
      }
      // dereference document that foo3 was referencing
      loadDocument(fluoClient, "/foo3", "The test is over, for now.");

      Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("the test is over"));
      Assert.assertEquals(new Counts(0, 0), pcTable.getPhraseCounts("is only a test"));
      Assert.assertEquals(new Counts(0, 0), pcTable.getPhraseCounts("test do not panic"));

      try(TypedSnapshot tsnap = TYPEL.wrap(fluoClient.newSnapshot())){
        Assert.assertNull(tsnap.get().row("doc:" + oldHash).col(DOC_CONTENT_COL).toString());
        Assert.assertNull(tsnap.get().row("doc:" + oldHash).col(DOC_REF_COUNT_COL).toInteger());
      }
    }

  }

  @Test
  public void testHighCardinality() throws Exception {
    try (FluoClient fluoClient = FluoFactory.newClient(getFluoConfiguration())) {

      Random rand = new Random();

      loadDocsWithRandomWords(fluoClient, rand, "This is only a test", 0, 100);

      Assert.assertEquals(new Counts(100, 100), pcTable.getPhraseCounts("this is only a"));
      Assert.assertEquals(new Counts(100, 100), pcTable.getPhraseCounts("is only a test"));

      loadDocsWithRandomWords(fluoClient, rand, "This is not a test", 0, 2);

      Assert.assertEquals(new Counts(2, 2), pcTable.getPhraseCounts("this is not a"));
      Assert.assertEquals(new Counts(2, 2), pcTable.getPhraseCounts("is not a test"));
      Assert.assertEquals(new Counts(98, 98), pcTable.getPhraseCounts("this is only a"));
      Assert.assertEquals(new Counts(98, 98), pcTable.getPhraseCounts("is only a test"));

      loadDocsWithRandomWords(fluoClient, rand, "This is not a test", 2, 100);

      Assert.assertEquals(new Counts(100, 100), pcTable.getPhraseCounts("this is not a"));
      Assert.assertEquals(new Counts(100, 100), pcTable.getPhraseCounts("is not a test"));
      Assert.assertEquals(new Counts(0, 0), pcTable.getPhraseCounts("this is only a"));
      Assert.assertEquals(new Counts(0, 0), pcTable.getPhraseCounts("is only a test"));

      loadDocsWithRandomWords(fluoClient, rand, "This is only a test", 0, 50);

      Assert.assertEquals(new Counts(50, 50), pcTable.getPhraseCounts("this is not a"));
      Assert.assertEquals(new Counts(50, 50), pcTable.getPhraseCounts("is not a test"));
      Assert.assertEquals(new Counts(50, 50), pcTable.getPhraseCounts("this is only a"));
      Assert.assertEquals(new Counts(50, 50), pcTable.getPhraseCounts("is only a test"));

    }
  }

  void loadDocsWithRandomWords(FluoClient fluoClient, Random rand, String phrase, int start,
      int end) {

    try (LoaderExecutor le = fluoClient.newLoaderExecutor()) {
      // load many documents that share the same phrase
      for (int i = start; i < end; i++) {
        String uri = "/foo" + i;
        StringBuilder content = new StringBuilder(phrase);
        // add a bunch of random words
        for (int j = 0; j < 20; j++) {
          content.append(' ');
          content.append(Integer.toString(rand.nextInt(10000), 36));
        }

        Document doc = new Document(uri, content.toString());
        le.execute(new DocumentLoader(doc));
      }
    }
    getMiniFluo().waitForObservers();
  }
}


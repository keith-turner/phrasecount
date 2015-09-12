package phrasecount;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import io.fluo.api.client.FluoAdmin.InitOpts;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.mini.MiniFluo;
import io.fluo.api.types.TypedSnapshot;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import phrasecount.pojos.Counts;
import phrasecount.pojos.Document;
import phrasecount.query.PhraseCountTable;

import static phrasecount.Constants.DOC_CONTENT_COL;
import static phrasecount.Constants.DOC_REF_COUNT_COL;
import static phrasecount.Constants.TYPEL;

// TODO make this an integration test

public class PhraseCounterTest {
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  private static FluoConfiguration props;
  private static MiniFluo miniFluo;
  private static final PasswordToken password = new PasswordToken("secret");
  private static AtomicInteger tableCounter = new AtomicInteger(1);
  private PhraseCountTable pcTable;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"),
        new String(password.getPassword()));
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }

  @Before
  public void setUpFluo() throws Exception {

    // configure Fluo to use mini instance. Could avoid all of this code and let MiniFluo create a
    // MiniAccumulo instance. However we need access to the MiniAccumulo instance inorder to create
    // the export/query table.
    props = new FluoConfiguration();
    props.setMiniStartAccumulo(false);
    props.setApplicationName("phrasecount");
    props.setAccumuloInstance(cluster.getInstanceName());
    props.setAccumuloUser("root");
    props.setAccumuloPassword("secret");
    props.setInstanceZookeepers(cluster.getZooKeepers() + "/fluo");
    props.setAccumuloZookeepers(cluster.getZooKeepers());
    props.setAccumuloTable("data" + tableCounter.getAndIncrement());
    props.setWorkerThreads(5);

    // create the export/query table
    String queryTable = "pcq" + tableCounter.getAndIncrement();
    Connector conn = cluster.getConnector("root", "secret");
    conn.tableOperations().create(queryTable);
    pcTable = new PhraseCountTable(conn, queryTable);

    // configure phrase count observers
    Application.configure(props, new Application.Options(13, 13, cluster.getInstanceName(),
        cluster.getZooKeepers(), "root", "secret", queryTable));

    FluoFactory.newAdmin(props)
        .initialize(new InitOpts().setClearTable(true).setClearZookeeper(true));

    miniFluo = FluoFactory.newMiniFluo(props);
  }

  @After
  public void tearDownFluo() throws Exception {
    miniFluo.close();
  }

  private void loadDocument(LoaderExecutor le, String uri, String content) {
    Document doc = new Document(uri, content);
    le.execute(new DocumentLoader(doc));
    miniFluo.waitForObservers();
  }

  @Test
  public void test1() throws Exception {

    FluoConfiguration lep = new FluoConfiguration(props);
    lep.setLoaderThreads(0);
    lep.setLoaderQueueSize(0);

    FluoClient fluoClient = FluoFactory.newClient(lep);

    LoaderExecutor le = fluoClient.newLoaderExecutor();

    loadDocument(le, "/foo1", "This is only a test.  Do not panic. This is only a test.");

    Assert.assertEquals(new Counts(1, 2), pcTable.getPhraseCounts("is only a test"));
    Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("test do not panic"));

    // add new document w/ different content and overlapping phrase.. should change some counts
    loadDocument(le, "/foo2", "This is only a test");

    Assert.assertEquals(new Counts(2, 3), pcTable.getPhraseCounts("is only a test"));
    Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("test do not panic"));

    // add new document w/ same content, should not change any counts
    loadDocument(le, "/foo3", "This is only a test");

    Assert.assertEquals(new Counts(2, 3), pcTable.getPhraseCounts("is only a test"));
    Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("test do not panic"));

    // change the content of /foo1, should change counts
    loadDocument(le, "/foo1", "The test is over, for now.");

    Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("the test is over"));
    Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("is only a test"));
    Assert.assertEquals(new Counts(0, 0), pcTable.getPhraseCounts("test do not panic"));

    // change content of foo2, should not change anything
    loadDocument(le, "/foo2", "The test is over, for now.");

    Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("the test is over"));
    Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("is only a test"));
    Assert.assertEquals(new Counts(0, 0), pcTable.getPhraseCounts("test do not panic"));

    String oldHash = new Document("/foo3", "This is only a test").getHash();
    TypedSnapshot tsnap = TYPEL.wrap(fluoClient.newSnapshot());
    Assert.assertNotNull(tsnap.get().row("doc:" + oldHash).col(DOC_CONTENT_COL).toString());
    Assert.assertEquals(1, tsnap.get().row("doc:" + oldHash).col(DOC_REF_COUNT_COL).toInteger(0));

    // dereference document that foo3 was referencing
    loadDocument(le, "/foo3", "The test is over, for now.");

    Assert.assertEquals(new Counts(1, 1), pcTable.getPhraseCounts("the test is over"));
    Assert.assertEquals(new Counts(0, 0), pcTable.getPhraseCounts("is only a test"));
    Assert.assertEquals(new Counts(0, 0), pcTable.getPhraseCounts("test do not panic"));

    tsnap = TYPEL.wrap(fluoClient.newSnapshot());
    Assert.assertNull(tsnap.get().row("doc:" + oldHash).col(DOC_CONTENT_COL).toString());
    Assert.assertNull(tsnap.get().row("doc:" + oldHash).col(DOC_REF_COUNT_COL).toInteger());

    le.close();
    fluoClient.close();

  }

  @Test
  public void testHighCardinality() throws Exception {
    FluoConfiguration lep = new FluoConfiguration(props);
    lep.setLoaderThreads(0);
    lep.setLoaderQueueSize(0);

    FluoClient fluoClient = FluoFactory.newClient(lep);

    LoaderExecutor le = fluoClient.newLoaderExecutor();

    Random rand = new Random();

    loadDocsWithRandomWords(le, rand, "This is only a test", 0, 100);

    Assert.assertEquals(new Counts(100, 100), pcTable.getPhraseCounts("this is only a"));
    Assert.assertEquals(new Counts(100, 100), pcTable.getPhraseCounts("is only a test"));

    loadDocsWithRandomWords(le, rand, "This is not a test", 0, 2);

    Assert.assertEquals(new Counts(2, 2), pcTable.getPhraseCounts("this is not a"));
    Assert.assertEquals(new Counts(2, 2), pcTable.getPhraseCounts("is not a test"));
    Assert.assertEquals(new Counts(98, 98), pcTable.getPhraseCounts("this is only a"));
    Assert.assertEquals(new Counts(98, 98), pcTable.getPhraseCounts("is only a test"));

    loadDocsWithRandomWords(le, rand, "This is not a test", 2, 100);

    Assert.assertEquals(new Counts(100, 100), pcTable.getPhraseCounts("this is not a"));
    Assert.assertEquals(new Counts(100, 100), pcTable.getPhraseCounts("is not a test"));
    Assert.assertEquals(new Counts(0, 0), pcTable.getPhraseCounts("this is only a"));
    Assert.assertEquals(new Counts(0, 0), pcTable.getPhraseCounts("is only a test"));

    loadDocsWithRandomWords(le, rand, "This is only a test", 0, 50);

    Assert.assertEquals(new Counts(50, 50), pcTable.getPhraseCounts("this is not a"));
    Assert.assertEquals(new Counts(50, 50), pcTable.getPhraseCounts("is not a test"));
    Assert.assertEquals(new Counts(50, 50), pcTable.getPhraseCounts("this is only a"));
    Assert.assertEquals(new Counts(50, 50), pcTable.getPhraseCounts("is only a test"));

    le.close();
    fluoClient.close();
  }

  void loadDocsWithRandomWords(LoaderExecutor le, Random rand, String phrase, int start, int end) {
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

    miniFluo.waitForObservers();
  }
}


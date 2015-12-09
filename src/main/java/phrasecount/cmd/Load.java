package phrasecount.cmd;

import java.io.File;

import javax.inject.Inject;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.config.FluoConfiguration;
import phrasecount.DocumentLoader;
import phrasecount.pojos.Document;

public class Load {


  //the fluo exec command will inject this
  @Inject private static FluoConfiguration fluoConfiguration;

  public static void main(String[] args) throws Exception {

    if (args.length != 1) {
      System.err.println("Usage : " + Load.class.getName() + " <txt file dir>");
      System.exit(-1);
    }

    FluoConfiguration leprops = new FluoConfiguration(fluoConfiguration);
    leprops.setLoaderThreads(20);
    leprops.setLoaderQueueSize(40);

    try (FluoClient fluoClient = FluoFactory.newClient(leprops);
        LoaderExecutor le = fluoClient.newLoaderExecutor()) {
      File[] files = new File(args[1]).listFiles();

      for (File txtFile : files) {
        if (txtFile.getName().endsWith(".txt")) {
          String uri = txtFile.toURI().toString();
          String content = Files.toString(txtFile, Charsets.UTF_8);

          System.out.println("Processing : " + txtFile.toURI());
          le.execute(new DocumentLoader(new Document(uri, content)));
        } else {
          System.out.println("Ignoring : " + txtFile.toURI());
        }
      }
    }

    // TODO figure what threads are hanging around
    System.exit(0);
  }

}

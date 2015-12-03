package phrasecount.cmd;

import java.io.File;

import io.fluo.api.config.FluoConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.configuration.PropertiesConfiguration;
import phrasecount.Application;
import phrasecount.Application.Options;

public class Setup {

  public static void main(String[] args) throws Exception {
    FluoConfiguration config = new FluoConfiguration(new File(args[0]));

    String exportTable = "pce";

    Connector conn =
        new ZooKeeperInstance(config.getAccumuloInstance(), config.getAccumuloZookeepers())
            .getConnector("root", new PasswordToken("secret"));
    try {
      conn.tableOperations().delete(exportTable);
    } catch (TableNotFoundException e) {
    }

    conn.tableOperations().create(exportTable);


    Options opts = new Options(103, 103, config.getAccumuloInstance(), config.getAccumuloZookeepers(),
        config.getAccumuloUser(), config.getAccumuloPassword(), exportTable);

    FluoConfiguration observerConfig = new FluoConfiguration();
    Application.configure(observerConfig, opts);

    PropertiesConfiguration propsConfig = new PropertiesConfiguration();
    propsConfig.setDelimiterParsingDisabled(true);
    propsConfig.copy(observerConfig);
    propsConfig.save(System.out);

    //TODO do after init
    //TableOperations.optimizeTable(config);
  }
}

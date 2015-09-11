package org.fixtrading.silverflash.cuke;

import org.fixtrading.silverflash.auth.Directory;
import org.fixtrading.silverflash.auth.SimpleDirectory;
import org.fixtrading.silverflash.fixp.Engine;
import org.fixtrading.silverflash.fixp.auth.SimpleAuthenticator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;

@RunWith(Cucumber.class)
@CucumberOptions(format = {"pretty", "html:target/cucumber"},
    features = "src/test/resources/features")
public class RunAllTests {

  private static Engine clientEngine;
  private static Engine serverEngine;
  private static SimpleDirectory directory = new SimpleDirectory();

  public static Engine getClientEngine() {
    return clientEngine;
  }

  public static Engine getServerEngine() {
    return serverEngine;
  }

  public static Directory getDirectory() {
    return directory;
  }

  @BeforeClass
  public static void initTestEnvironment() throws Exception {
    System.out.println("Initializing test environment");
    clientEngine = Engine.builder().build();
    clientEngine.getReactor().setTrace(true, "client");
    clientEngine.open();

    serverEngine =
        Engine.builder().withAuthenticator(new SimpleAuthenticator().withDirectory(directory))
            .build();
    serverEngine.getReactor().setTrace(true, "server");
    serverEngine.open();
  }

  @AfterClass
  public static void detroyTestEnvironment() {
    System.out.println("Tearing down test environment");
    clientEngine.close();
    serverEngine.close();
  }
}

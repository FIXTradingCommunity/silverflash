/**
 *    Copyright 2015 FIX Protocol Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package io.fixprotocol.silverflash.cuke;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;
import io.fixprotocol.silverflash.auth.Directory;
import io.fixprotocol.silverflash.auth.SimpleDirectory;
import io.fixprotocol.silverflash.fixp.Engine;
import io.fixprotocol.silverflash.fixp.auth.SimpleAuthenticator;

@RunWith(Cucumber.class)
@CucumberOptions(plugin = {"pretty", "html:target/cucumber"},
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
    //clientEngine.getReactor().setTrace(true, "client");
    clientEngine.open();

    serverEngine =
        Engine.builder().withAuthenticator(new SimpleAuthenticator().withDirectory(directory))
            .build();
    //serverEngine.getReactor().setTrace(true, "server");
    serverEngine.open();
  }

  @AfterClass
  public static void detroyTestEnvironment() {
    System.out.println("Tearing down test environment");
    clientEngine.close();
    serverEngine.close();
  }
}

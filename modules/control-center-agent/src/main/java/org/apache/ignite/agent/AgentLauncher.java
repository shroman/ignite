/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent;

import com.beust.jcommander.JCommander;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.ignite.agent.handlers.RestExecutor;
import org.apache.ignite.agent.testdrive.AgentMetadataTestDrive;
import org.apache.ignite.agent.testdrive.AgentSqlTestDrive;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import static org.apache.ignite.agent.AgentConfiguration.DFLT_SERVER_PORT;

/**
 * Control Center Agent launcher.
 */
public class AgentLauncher {
    /** Static initializer. */
    static {
        AgentLoggingConfigurator.configure();
    }

    /** */
    private static final Logger log = Logger.getLogger(AgentLauncher.class.getName());

    /** */
    private static final int RECONNECT_INTERVAL = 3000;

    /**
     * @param args Args.
     */
    @SuppressWarnings("BusyWait")
    public static void main(String[] args) throws Exception {
        log.log(Level.INFO, "Starting Apache Ignite Control Center Agent...");

        AgentConfiguration cfg = new AgentConfiguration();

        AgentConfiguration cmdCfg = new AgentConfiguration();

        JCommander jCommander = new JCommander(cmdCfg, args);

        String osName = System.getProperty("os.name").toLowerCase();

        jCommander.setProgramName("ignite-web-agent." + (osName.contains("win") ? "bat" : "sh"));

        if (cmdCfg.help()) {
            jCommander.usage();

            return;
        }

        if (cmdCfg.testDriveSql() && cmdCfg.nodeUri() != null)
            log.log(Level.WARNING,
                "URI for connect to Ignite REST server will be ignored because --test-drive-sql option was specified.");


        if (cmdCfg.configPath() != null)
            cfg.load(new File(cmdCfg.configPath()).toURI().toURL());
        else {
            try {
                cfg.load(new File("./default.properties").toURI().toURL());
            }
            catch (IOException ignore) {
                // No-op.
            }
        }

        cfg.merge(cmdCfg);

        if (cfg.token() == null) {
            System.out.print("Token: ");

            cfg.token(new String(System.console().readPassword()));
        }

        if (cfg.testDriveMetadata())
            AgentMetadataTestDrive.testDrive();

        if (cfg.testDriveSql())
            AgentSqlTestDrive.testDrive(cfg);

        RestExecutor restExecutor = new RestExecutor(cfg);

        restExecutor.start();

        try {
            SslContextFactory sslCtxFactory = new SslContextFactory();

            // TODO IGNITE-843 Fix issue with trust all: if (Boolean.TRUE.equals(Boolean.getBoolean("trust.all")))
            sslCtxFactory.setTrustAll(true);

            WebSocketClient client = new WebSocketClient(sslCtxFactory);

            client.setMaxIdleTimeout(Long.MAX_VALUE);

            client.start();

            try {
                while (!Thread.interrupted()) {
                    AgentSocket agentSock = new AgentSocket(cfg, restExecutor);

                    log.log(Level.INFO, "Connecting to: " + cfg.serverUri());

                    URI uri = URI.create(cfg.serverUri());

                    if (uri.getPort() == -1)
                        uri = URI.create(cfg.serverUri() + ":" + DFLT_SERVER_PORT);

                    client.connect(agentSock, uri);

                    agentSock.waitForClose();

                    Thread.sleep(RECONNECT_INTERVAL);
                }
            }
            finally {
                client.stop();
            }
        }
        finally {
            restExecutor.stop();
        }
    }
}

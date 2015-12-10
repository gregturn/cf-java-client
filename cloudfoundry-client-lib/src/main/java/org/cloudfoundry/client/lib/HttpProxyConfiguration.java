/*
 * Copyright 2009-2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.cloudfoundry.client.lib;


/**
 * Class that encapsulates http proxy information
 *
 * @author Thomas Risberg
 */
public class HttpProxyConfiguration {

    private boolean authRequired;

    private String password;

    private String proxyHost;

    private int proxyPort;

    private String username;

    public HttpProxyConfiguration(String proxyHost, int proxyPort) {
        this(proxyHost, proxyPort, false, null, null);
    }

    public HttpProxyConfiguration(String proxyHost, int proxyPort,
                                  boolean authRequired, String username, String password) {
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.authRequired = authRequired;
        this.username = username;
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    public String getUsername() {
        return username;
    }

    public boolean isAuthRequired() {
        return authRequired;
    }
}
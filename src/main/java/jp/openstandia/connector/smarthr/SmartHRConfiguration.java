/*
 *  Copyright Nomura Research Institute, Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package jp.openstandia.connector.smarthr;

import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;

public class SmartHRConfiguration extends AbstractConfiguration {

    private String endpointURL;
    private GuardedString apiAccessToken;
    private String httpProxyHost;
    private Integer httpProxyPort = 3128;
    private String httpProxyUser;
    private GuardedString httpProxyPassword;
    private Integer defaultQueryPageSize = 50;
    private Integer connectionTimeoutInSeconds = 10;
    private Integer readTimeoutInSeconds = 10;
    private Integer writeTimeoutInSeconds = 10;

    @ConfigurationProperty(
            order = 1,
            displayMessageKey = "SmartHR API URL",
            helpMessageKey = "SmartHR API URL which is connected from this connector." +
                    " e.g. https://<your-tenant-id>.smarthr.jp or https://<your-sandbox-tenant-id>.daruma.space",
            required = true,
            confidential = false)
    public String getEndpointURL() {
        if (endpointURL != null && !endpointURL.endsWith("/")) {
            return endpointURL + "/";
        }
        return endpointURL;
    }

    public void setEndpointURL(String endpointURL) {
        this.endpointURL = endpointURL;
    }

    @ConfigurationProperty(
            order = 2,
            displayMessageKey = "SmartHR API Access Token",
            helpMessageKey = "Access token for the API authentication.",
            required = true,
            confidential = true)
    public GuardedString getApiAccessToken() {
        return apiAccessToken;
    }

    public void setApiAccessToken(GuardedString apiAccessToken) {
        this.apiAccessToken = apiAccessToken;
    }

    @ConfigurationProperty(
            order = 3,
            displayMessageKey = "HTTP Proxy Host",
            helpMessageKey = "Hostname for the HTTP Proxy.",
            required = false,
            confidential = false)
    public String getHttpProxyHost() {
        return httpProxyHost;
    }

    public void setHttpProxyHost(String httpProxyHost) {
        this.httpProxyHost = httpProxyHost;
    }

    @ConfigurationProperty(
            order = 4,
            displayMessageKey = "HTTP Proxy Port",
            helpMessageKey = "Port for the HTTP Proxy. (Default: 3128)",
            required = false,
            confidential = false)
    public int getHttpProxyPort() {
        return httpProxyPort;
    }

    public void setHttpProxyPort(int httpProxyPort) {
        this.httpProxyPort = httpProxyPort;
    }

    @ConfigurationProperty(
            order = 5,
            displayMessageKey = "HTTP Proxy User",
            helpMessageKey = "Username for the HTTP Proxy Authentication.",
            required = false,
            confidential = false)
    public String getHttpProxyUser() {
        return httpProxyUser;
    }

    public void setHttpProxyUser(String httpProxyUser) {
        this.httpProxyUser = httpProxyUser;
    }

    @ConfigurationProperty(
            order = 6,
            displayMessageKey = "HTTP Proxy Password",
            helpMessageKey = "Password for the HTTP Proxy Authentication.",
            required = false,
            confidential = true)
    public GuardedString getHttpProxyPassword() {
        return httpProxyPassword;
    }

    public void setHttpProxyPassword(GuardedString httpProxyPassword) {
        this.httpProxyPassword = httpProxyPassword;
    }

    @ConfigurationProperty(
            order = 7,
            displayMessageKey = "Default Query Page Size",
            helpMessageKey = "Number of results to return per page. (Default: 50)",
            required = false,
            confidential = false)
    public int getDefaultQueryPageSize() {
        return defaultQueryPageSize;
    }

    public void setDefaultQueryPageSize(int defaultQueryPageSize) {
        this.defaultQueryPageSize = defaultQueryPageSize;
    }

    @ConfigurationProperty(
            order = 8,
            displayMessageKey = "Connection Timeout (in seconds)",
            helpMessageKey = "Connection timeout when connecting to SmartHR. (Default: 10)",
            required = false,
            confidential = false)
    public int getConnectionTimeoutInSeconds() {
        return connectionTimeoutInSeconds;
    }

    public void setConnectionTimeoutInSeconds(int connectionTimeoutInSeconds) {
        this.connectionTimeoutInSeconds = connectionTimeoutInSeconds;
    }

    @ConfigurationProperty(
            order = 9,
            displayMessageKey = "Read Timeout (in seconds)",
            helpMessageKey = "Read timeout when fetching data from SmartHR. (Default: 10)",
            required = false,
            confidential = false)
    public int getReadTimeoutInSeconds() {
        return readTimeoutInSeconds;
    }

    public void setReadTimeoutInSeconds(int writeTimeoutInSeconds) {
        this.writeTimeoutInSeconds = writeTimeoutInSeconds;
    }

    @ConfigurationProperty(
            order = 10,
            displayMessageKey = "Write Timeout (in seconds)",
            helpMessageKey = "Write timeout when fetching data from SmartHR. (Default: 10)",
            required = false,
            confidential = false)
    public int getWriteTimeoutInSeconds() {
        return writeTimeoutInSeconds;
    }

    public void setWriteTimeoutInSeconds(int writeTimeoutInSeconds) {
        this.writeTimeoutInSeconds = writeTimeoutInSeconds;
    }

    @Override
    public void validate() {
        if (endpointURL == null) {
            throw new ConfigurationException("SmartHR Endpoint URL is required");
        }
        if (apiAccessToken == null) {
            throw new ConfigurationException("SmartHR API Password is required");
        }
    }
}

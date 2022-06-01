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

    private String smartHREndpointURL;
    private GuardedString apiAccessToken;
    private String httpProxyHost;
    private Integer httpProxyPort;
    private String httpProxyUser;
    private GuardedString httpProxyPassword;
    private Integer defaultQueryPageSize;

    @ConfigurationProperty(
            order = 1,
            displayMessageKey = "SmartHR API URL",
            helpMessageKey = "SmartHR API URL which is connected from this connector." +
                    " e.g. https://<your-tenant-id>.smarthr.jp/api or https://<your-sandbox-tenant-id>.daruma.space/api",
            required = true,
            confidential = false)
    public String getSmartHREndpointURL() {
        if (smartHREndpointURL != null && !smartHREndpointURL.endsWith("/")) {
            return smartHREndpointURL + "/";
        }
        return smartHREndpointURL;
    }

    public void setSmartHREndpointURL(String smartHREndpointURL) {
        this.smartHREndpointURL = smartHREndpointURL;
    }

    @ConfigurationProperty(
            order = 2,
            displayMessageKey = "SmartHR API Access Token",
            helpMessageKey = "Access token for the API authentication.",
            required = true,
            confidential = true)
    public GuardedString getAPIAccessToken() {
        return apiAccessToken;
    }

    public void setAPIAccessToken(GuardedString apiAccessToken) {
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
        if (httpProxyPort == null) {
            return 3128; // Default
        }
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
            helpMessageKey = "Number of results to return per page. Default: 50",
            required = false,
            confidential = false)
    public int getDefaultQueryPageSize() {
        if (defaultQueryPageSize == null) {
            return 50; // Default
        }
        return defaultQueryPageSize;
    }

    public void setDefaultQueryPageSize(int defaultQueryPageSize) {
        this.defaultQueryPageSize = defaultQueryPageSize;
    }

    @Override
    public void validate() {
        if (smartHREndpointURL == null) {
            throw new ConfigurationException("SmartHR Endpoint URL is required");
        }
        if (apiAccessToken == null) {
            throw new ConfigurationException("SmartHR API Password is required");
        }
    }
}

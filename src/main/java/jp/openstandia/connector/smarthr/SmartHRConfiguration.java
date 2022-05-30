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

    private String smarthrURL;
    private String smarthrDataSource;
    private String apiUsername;
    private GuardedString apiPassword;
    private String httpProxyHost;
    private int httpProxyPort;
    private String httpProxyUser;
    private GuardedString httpProxyPassword;
    private boolean suppressInvitationMessageEnabled = true;

    @ConfigurationProperty(
            order = 1,
            displayMessageKey = "SmartHR API URL",
            helpMessageKey = "SmartHR API URL which is connected from this connector. e.g. https://smarthr.example.com/smarthr/api",
            required = true,
            confidential = false)
    public String getSmartHRURL() {
        if (smarthrURL != null && !smarthrURL.endsWith("/")) {
            return smarthrURL + "/";
        }
        return smarthrURL;
    }

    public void setSmartHRURL(String smarthrURL) {
        this.smarthrURL = smarthrURL;
    }

    @ConfigurationProperty(
            order = 2,
            displayMessageKey = "SmartHR Data source",
            helpMessageKey = "SmartHR Data source which is connected from this connector. e.g. postgresql-shared",
            required = true,
            confidential = false)
    public String getSmartHRDataSource() {
        return smarthrDataSource;
    }

    public void setSmartHRDataSource(String smarthrDataSource) {
        this.smarthrDataSource = smarthrDataSource;
    }

    @ConfigurationProperty(
            order = 3,
            displayMessageKey = "SmartHR API Username",
            helpMessageKey = "Username for the API authentication.",
            required = true,
            confidential = false)
    public String getApiUsername() {
        return apiUsername;
    }

    public void setApiUsername(String apiUsername) {
        this.apiUsername = apiUsername;
    }

    @ConfigurationProperty(
            order = 4,
            displayMessageKey = "SmartHR API Password",
            helpMessageKey = "Password for the API authentication.",
            required = true,
            confidential = false)
    public GuardedString getApiPassword() {
        return apiPassword;
    }

    public void setApiPassword(GuardedString apiPassword) {
        this.apiPassword = apiPassword;
    }

    @ConfigurationProperty(
            order = 5,
            displayMessageKey = "HTTP Proxy Host",
            helpMessageKey = "Hostname for the HTTP Proxy",
            required = false,
            confidential = false)
    public String getHttpProxyHost() {
        return httpProxyHost;
    }

    public void setHttpProxyHost(String httpProxyHost) {
        this.httpProxyHost = httpProxyHost;
    }

    @ConfigurationProperty(
            order = 6,
            displayMessageKey = "HTTP Proxy Port",
            helpMessageKey = "Port for the HTTP Proxy",
            required = false,
            confidential = false)
    public int getHttpProxyPort() {
        return httpProxyPort;
    }

    public void setHttpProxyPort(int httpProxyPort) {
        this.httpProxyPort = httpProxyPort;
    }

    @ConfigurationProperty(
            order = 7,
            displayMessageKey = "HTTP Proxy User",
            helpMessageKey = "Username for the HTTP Proxy Authentication",
            required = false,
            confidential = false)
    public String getHttpProxyUser() {
        return httpProxyUser;
    }

    public void setHttpProxyUser(String httpProxyUser) {
        this.httpProxyUser = httpProxyUser;
    }

    @ConfigurationProperty(
            order = 8,
            displayMessageKey = "HTTP Proxy Password",
            helpMessageKey = "Password for the HTTP Proxy Authentication",
            required = false,
            confidential = true)
    public GuardedString getHttpProxyPassword() {
        return httpProxyPassword;
    }

    public void setHttpProxyPassword(GuardedString httpProxyPassword) {
        this.httpProxyPassword = httpProxyPassword;
    }

    @Override
    public void validate() {
        if (smarthrURL == null) {
            throw new ConfigurationException("SmartHR URL is required");
        }
        if (smarthrDataSource == null) {
            throw new ConfigurationException("SmartHR Data source is required");
        }
        if (apiUsername == null) {
            throw new ConfigurationException("SmartHR API Username is required");
        }
        if (apiPassword == null) {
            throw new ConfigurationException("SmartHR API Password is required");
        }
    }
}

/**
 * Copyright © 2016-2017 The Thingsboard Authors
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
 */

package org.thingsboard.install.tools;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.DefaultApplicationArguments;
import org.springframework.boot.context.config.RandomValuePropertySource;
import org.springframework.boot.env.PropertySourcesLoader;
import org.springframework.core.env.*;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.context.support.StandardServletEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ThingsboardEnvironment {

    public static final String THINGSBOARD_CONFIG_FILE_NAME = "thingsboard";
    public static final String CONFIG_NAME_PROPERTY = "spring.config.name";

    private static final String DEFAULT_SEARCH_LOCATIONS = "classpath:/,classpath:/config/,file:./,file:./config/";
    private static final String CONFIG_LOCATION_PROPERTY = "spring.config.location";
    private static final String DEFAULT_NAMES = "application";
    private static final String DEFAULT_PROPERTIES = "defaultProperties";
    private static final String APPLICATION_CONFIGURATION_PROPERTY_SOURCE_NAME = "applicationConfigurationProperties";

    private static final String INSTALL_OPTION = "install";
    private static final String UPGRADE_OPTION = "upgrade";
    private static final String FROM_VERSION_CODE = "fromVersion";

    private static final String DATA_DIR_PROPERTY = "thingsboard.data.dir";

    private static final String JSON_DIR = "json";
    private static final String SYSTEM_DIR = "system";

    private boolean isInstall = false;
    private boolean isUpgrade = false;
    private String upgradeFromVersion;

    private String dataDir;
    private String jsonDir;
    private String systemJsonDir;

    private ConfigurableEnvironment environment;
    private ApplicationArguments applicationArguments;

    private PropertySourcesLoader propertiesLoader;
    private ResourceLoader resourceLoader;

    public ThingsboardEnvironment(String... args) throws IOException {
        this.applicationArguments = new DefaultApplicationArguments(
                args);

        processArguments();

        this.environment = new StandardServletEnvironment();
        this.resourceLoader = new DefaultResourceLoader();
        this.propertiesLoader = new PropertySourcesLoader();
        prepareEnvironment();

        this.dataDir = getProperty(DATA_DIR_PROPERTY);
        if (this.dataDir == null) {
            throw new RuntimeException("'" + DATA_DIR_PROPERTY + "' property should specified!");
        }
        if (!Files.isDirectory(Paths.get(this.dataDir))) {
            throw new RuntimeException("'" + DATA_DIR_PROPERTY + "' property value is not valid directory!");
        }
    }

    private void processArguments() {
        this.isInstall = applicationArguments.getNonOptionArgs().contains(INSTALL_OPTION);
        this.isUpgrade = applicationArguments.getNonOptionArgs().contains(UPGRADE_OPTION);
        if (this.isInstall == this.isUpgrade) {
            throw new RuntimeException("Invalid options specified! Valid options:     install | upgrade");
        }
        if (this.isUpgrade) {
            List<String> values = applicationArguments.getOptionValues(FROM_VERSION_CODE);
            if (values == null || values.size() != 1) {
                throw new RuntimeException("Invalid upgrade option values specified! Valid upgrade option values:     --fromVersion=<... ThingsBoard version, ex. 1.2.3 ...>");
            } else {
                this.upgradeFromVersion = values.get(0);
            }
        }
    }

    public boolean isInstall() {
        return isInstall;
    }

    public boolean isUpgrade() {
        return isUpgrade;
    }

    public String getUpgradeFromVersion() {
        return this.upgradeFromVersion;
    }

    public String getProperty(String key) {
        return this.environment.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        return this.environment.getProperty(key, defaultValue);
    }

    public <T> T getProperty(String key, Class<T> targetType) {
        return this.environment.getProperty(key, targetType);
    }

    public <T> T getProperty(String key, Class<T> targetType, T defaultValue) {
        return this.environment.getProperty(key, targetType, defaultValue);
    }

    private void prepareEnvironment() throws IOException {
        String[] sourceArgs = this.applicationArguments.getSourceArgs();
        MutablePropertySources sources = this.environment.getPropertySources();
        if (sourceArgs.length > 0) {
            String name = CommandLinePropertySource.COMMAND_LINE_PROPERTY_SOURCE_NAME;
            if (sources.contains(name)) {
                PropertySource<?> source = sources.get(name);
                CompositePropertySource composite = new CompositePropertySource(name);
                composite.addPropertySource(new SimpleCommandLinePropertySource(
                        name + "-" + sourceArgs.hashCode(), sourceArgs));
                composite.addPropertySource(source);
                sources.replace(name, composite);
            }
            else {
                sources.addFirst(new SimpleCommandLinePropertySource(sourceArgs));
            }
        }
        RandomValuePropertySource.addToEnvironment(environment);


        for (String location : getSearchLocations()) {
            if (!location.endsWith("/")) {
                load(location, null);
            }
            else {
                for (String name : getSearchNames()) {
                    load(location, name);
                }
            }
        }

        addConfigurationProperties(this.propertiesLoader.getPropertySources());
    }

    private void load(String location, String name)
            throws IOException {
        String group = "profile=";
        if (!StringUtils.hasText(name)) {
            loadIntoGroup(group, location);
        }
        else {
            for (String ext : this.propertiesLoader.getAllFileExtensions()) {
                loadIntoGroup(group, location + name + "." + ext);
            }
        }
    }

    private PropertySource<?> loadIntoGroup(String identifier, String location) throws IOException {
        Resource resource = this.resourceLoader.getResource(location);
        PropertySource<?> propertySource = null;
        if (resource != null && resource.exists()) {
            String name = "applicationConfig: [" + location + "]";
            String group = "applicationConfig: [" + identifier + "]";
            propertySource = this.propertiesLoader.load(resource, group, name, null);
        }
        return propertySource;
    }

    private void addConfigurationProperties(MutablePropertySources sources) {
        List<PropertySource<?>> reorderedSources = new ArrayList<PropertySource<?>>();
        for (PropertySource<?> item : sources) {
            reorderedSources.add(item);
        }
        addConfigurationProperties(
                new ConfigurationPropertySources(reorderedSources));
    }

    private void addConfigurationProperties(
            ConfigurationPropertySources configurationSources) {
        MutablePropertySources existingSources = this.environment
                .getPropertySources();
        if (existingSources.contains(DEFAULT_PROPERTIES)) {
            existingSources.addBefore(DEFAULT_PROPERTIES, configurationSources);
        }
        else {
            existingSources.addLast(configurationSources);
        }
    }

    private Set<String> getSearchNames() {
        if (this.environment.containsProperty(CONFIG_NAME_PROPERTY)) {
            return asResolvedSet(this.environment.getProperty(CONFIG_NAME_PROPERTY));
        }
        return asResolvedSet(DEFAULT_NAMES);
    }


    private Set<String> getSearchLocations() {
        Set<String> locations = new LinkedHashSet<String>();
        if (this.environment.containsProperty(CONFIG_LOCATION_PROPERTY)) {
            for (String path : asResolvedSet(
                    this.environment.getProperty(CONFIG_LOCATION_PROPERTY))) {
                if (!path.contains("$")) {
                    path = StringUtils.cleanPath(path);
                    if (!ResourceUtils.isUrl(path)) {
                        path = ResourceUtils.FILE_URL_PREFIX + path;
                    }
                }
                locations.add(path);
            }
        }
        locations.addAll(
                asResolvedSet(DEFAULT_SEARCH_LOCATIONS));
        return locations;
    }

    private Set<String> asResolvedSet(String value) {
        List<String> list = Arrays.asList(StringUtils.trimArrayElements(
                StringUtils.commaDelimitedListToStringArray(value)));
        Collections.reverse(list);
        return new LinkedHashSet<String>(list);
    }

    static class ConfigurationPropertySources
            extends EnumerablePropertySource<Collection<PropertySource<?>>> {

        private final Collection<PropertySource<?>> sources;

        private final String[] names;

        ConfigurationPropertySources(Collection<PropertySource<?>> sources) {
            super(APPLICATION_CONFIGURATION_PROPERTY_SOURCE_NAME, sources);
            this.sources = sources;
            List<String> names = new ArrayList<String>();
            for (PropertySource<?> source : sources) {
                if (source instanceof EnumerablePropertySource) {
                    names.addAll(Arrays.asList(
                            ((EnumerablePropertySource<?>) source).getPropertyNames()));
                }
            }
            this.names = names.toArray(new String[names.size()]);
        }

        @Override
        public Object getProperty(String name) {
            for (PropertySource<?> propertySource : this.sources) {
                Object value = propertySource.getProperty(name);
                if (value != null) {
                    return value;
                }
            }
            return null;
        }

        @Override
        public String[] getPropertyNames() {
            return this.names;
        }

    }
}

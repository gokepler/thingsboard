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

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

@Slf4j
public class InstallTool {

    private static final String SPRING_CONFIG_NAME_KEY = "--" + ThingsboardEnvironment.CONFIG_NAME_PROPERTY;
    private static final String DEFAULT_SPRING_CONFIG_PARAM = SPRING_CONFIG_NAME_KEY + "=" + ThingsboardEnvironment.THINGSBOARD_CONFIG_FILE_NAME;

    public static void main(String[] args) {
        log.info("Started ThingsBoard install tool");
        try {
            new InstallTool().run(updateArguments(args));
        } catch (Exception e) {
            log.error("Error occurred while running Install Tool!", e);
        }
    }

    private static String[] updateArguments(String[] args) {
        if (Arrays.stream(args).noneMatch(arg -> arg.startsWith(SPRING_CONFIG_NAME_KEY))) {
            String[] modifiedArgs = new String[args.length + 1];
            System.arraycopy(args, 0, modifiedArgs, 0, args.length);
            modifiedArgs[args.length] = DEFAULT_SPRING_CONFIG_PARAM;
            return modifiedArgs;
        }
        return args;
    }

    InstallTool() {}

    void run(String[] args) throws Exception {
        ThingsboardEnvironment environment = new ThingsboardEnvironment(args);

        //log.info("cassandra.cluster_name = [{}]", environment.getProperty("cassandra.cluster_name"));
        //log.info("cassandra.ssl = [{}]", environment.getProperty("cassandra.ssl", boolean.class));

        if (environment.isInstall()) {
            install(environment);
        } else if (environment.isUpgrade()) {
            upgrade(environment);
        }

    }

    void install(ThingsboardEnvironment environment) throws Exception {
        log.info("Going to install ThingsBoard System Data ...");
    }

    void upgrade(ThingsboardEnvironment environment) throws Exception {
        log.info("Going to upgrade ThingsBoard from version {} ...", environment.getUpgradeFromVersion());
    }

}

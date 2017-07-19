/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ambari.server.actionmanager;

import static org.apache.ambari.server.agent.ExecutionCommand.KeyNames.HOOKS_FOLDER;
import static org.apache.ambari.server.agent.ExecutionCommand.KeyNames.SERVICE_PACKAGE_FOLDER;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.ambari.server.AmbariException;
import org.apache.ambari.server.ClusterNotFoundException;
import org.apache.ambari.server.RoleCommand;
import org.apache.ambari.server.agent.AgentCommand.AgentCommandType;
import org.apache.ambari.server.agent.ExecutionCommand;
import org.apache.ambari.server.agent.ExecutionCommand.KeyNames;
import org.apache.ambari.server.api.services.AmbariMetaInfo;
import org.apache.ambari.server.orm.dao.HostRoleCommandDAO;
import org.apache.ambari.server.orm.entities.ClusterVersionEntity;
import org.apache.ambari.server.state.Cluster;
import org.apache.ambari.server.state.Clusters;
import org.apache.ambari.server.state.ConfigHelper;
import org.apache.ambari.server.state.DesiredConfig;
import org.apache.ambari.server.state.ServiceInfo;
import org.apache.ambari.server.state.StackId;
import org.apache.ambari.server.state.StackInfo;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;

public class ExecutionCommandWrapper {

  private final static Logger LOG = LoggerFactory.getLogger(ExecutionCommandWrapper.class);
  String jsonExecutionCommand = null;
  ExecutionCommand executionCommand = null;

  @Inject
  Clusters clusters;

  @Inject
  HostRoleCommandDAO hostRoleCommandDAO;

  @Inject
  ConfigHelper configHelper;

  @Inject
  private Gson gson;

  /**
   * Used for injecting hooks and common-services into the command.
   */
  @Inject
  private AmbariMetaInfo ambariMetaInfo;

  @AssistedInject
  public ExecutionCommandWrapper(@Assisted String jsonExecutionCommand) {
    this.jsonExecutionCommand = jsonExecutionCommand;
  }

  @AssistedInject
  public ExecutionCommandWrapper(@Assisted ExecutionCommand executionCommand) {
    this.executionCommand = executionCommand;
  }

  /**
   * Gets the execution command by either de-serializing the backing JSON
   * command or returning the encapsulated instance which has already been
   * de-serialized.
   * <p/>
   * If the {@link ExecutionCommand} has configuration tags which need to be
   * refreshed, then this method will lookup the appropriate configuration tags
   * before building the final configurations to set ont he command. Therefore,
   * the {@link ExecutionCommand} is allowed to have no configuration tags as
   * long as it has been instructed to set updated ones at execution time.
   *
   * @return
   * @see ExecutionCommand#setForceRefreshConfigTagsBeforeExecution(Set)
   */
  public ExecutionCommand getExecutionCommand() {
    if (executionCommand != null) {
      return executionCommand;
    }

    if( null == jsonExecutionCommand ){
      throw new RuntimeException(
          "Invalid ExecutionCommandWrapper, both object and string representations are null");
    }

    try {
      executionCommand = gson.fromJson(jsonExecutionCommand, ExecutionCommand.class);

      // sanity; if no configurations, just initialize to prevent NPEs
      if (null == executionCommand.getConfigurations()) {
        executionCommand.setConfigurations(new TreeMap<String, Map<String, String>>());
      }

      Map<String, Map<String, String>> configurations = executionCommand.getConfigurations();

      // For a configuration type, both tag and an actual configuration can be stored
      // Configurations from the tag is always expanded and then over-written by the actual
      // global:version1:{a1:A1,b1:B1,d1:D1} + global:{a1:A2,c1:C1,DELETED_d1:x} ==>
      // global:{a1:A2,b1:B1,c1:C1}
      Long clusterId = hostRoleCommandDAO.findByPK(
          executionCommand.getTaskId()).getStage().getClusterId();

      Cluster cluster = clusters.getClusterById(clusterId);

      // Execution commands may have config-tags already set during their creation.
      // However, these tags become stale at runtime when other
      // ExecutionCommands run and change the desired configs (like
      // ConfigureAction). Hence an ExecutionCommand can specify which
      // config-types should be refreshed at runtime. Specifying <code>*</code>
      // will result in all config-type tags to be refreshed to the latest
      // cluster desired-configs. Additionally, there may be no configuration
      // tags set but refresh might be set to *. In this case, they should still
      // be refreshed with the latest.
      boolean refreshConfigTagsBeforeExecution = executionCommand.getForceRefreshConfigTagsBeforeExecution();
      if (refreshConfigTagsBeforeExecution) {
        Map<String, DesiredConfig> desiredConfigs = cluster.getDesiredConfigs();

        Map<String, Map<String, String>> configurationTags = configHelper.getEffectiveDesiredTags(
            cluster, executionCommand.getHostname(), desiredConfigs);

        LOG.debug(
            "While scheduling task {} on cluster {}, configurations are being refreshed using desired configurations of {}",
            executionCommand.getTaskId(), cluster.getClusterName(), desiredConfigs);

        // then clear out any existing configurations so that all of the new
        // configurations are forcefully applied
        configurations.clear();
        executionCommand.setConfigurationTags(configurationTags);
      }

      // now that the tags have been updated (if necessary), fetch the
      // configurations
      Map<String, Map<String, String>> configurationTags = executionCommand.getConfigurationTags();
      if (null != configurationTags && !configurationTags.isEmpty()) {
        Map<String, Map<String, String>> configProperties = configHelper
            .getEffectiveConfigProperties(cluster, configurationTags);

        // Apply the configurations saved with the Execution Cmd on top of
        // derived configs - This will take care of all the hacks
        for (Map.Entry<String, Map<String, String>> entry : configProperties.entrySet()) {
          String type = entry.getKey();
          Map<String, String> allLevelMergedConfig = entry.getValue();

          if (configurations.containsKey(type)) {
            Map<String, String> mergedConfig = configHelper.getMergedConfig(allLevelMergedConfig,
                configurations.get(type));

            configurations.get(type).clear();
            configurations.get(type).putAll(mergedConfig);

          } else {
            configurations.put(type, new HashMap<String, String>());
            configurations.get(type).putAll(allLevelMergedConfig);
          }
        }

        Map<String, Map<String, Map<String, String>>> configAttributes = configHelper.getEffectiveConfigAttributes(
            cluster, executionCommand.getConfigurationTags());

        for (Map.Entry<String, Map<String, Map<String, String>>> attributesOccurrence : configAttributes.entrySet()) {
          String type = attributesOccurrence.getKey();
          Map<String, Map<String, String>> attributes = attributesOccurrence.getValue();

          if (executionCommand.getConfigurationAttributes() != null) {
            if (!executionCommand.getConfigurationAttributes().containsKey(type)) {
              executionCommand.getConfigurationAttributes().put(type,
                  new TreeMap<String, Map<String, String>>());
            }
            configHelper.cloneAttributesMap(attributes,
                executionCommand.getConfigurationAttributes().get(type));
            }
        }
      }

      Map<String,String> commandParams = executionCommand.getCommandParams();

      // set the version for the command if it's not already set
      if (!commandParams.containsKey(KeyNames.VERSION)) {
        // the cluster's effective version should be used for this command
        ClusterVersionEntity effectiveClusterVersion = cluster.getEffectiveClusterVersion();

        // in the event that the effective version is NULL (meaning that most
        // likely the cluster is still being provisioned), then send down the
        // version if this is not an install command
        if (null == effectiveClusterVersion
            && executionCommand.getRoleCommand() != RoleCommand.INSTALL) {
          Collection<ClusterVersionEntity> clusterVersions = cluster.getAllClusterVersions();
          if (clusterVersions.size() == 1) {
            effectiveClusterVersion = clusterVersions.iterator().next();
          }
        }

        if (null != effectiveClusterVersion) {
          commandParams.put(KeyNames.VERSION,
              effectiveClusterVersion.getRepositoryVersion().getVersion());
        }
      }

      // add the stack and common-services folders to the command, but only if
      // they don't exist - they may have been put on here with specific
      // values ahead of time
      StackId stackId = cluster.getDesiredStackVersion();
      StackInfo stackInfo = ambariMetaInfo.getStack(stackId.getStackName(),
          stackId.getStackVersion());

      if (!commandParams.containsKey(HOOKS_FOLDER)) {
        commandParams.put(HOOKS_FOLDER, stackInfo.getStackHooksFolder());
      }

      if (!commandParams.containsKey(SERVICE_PACKAGE_FOLDER)) {
        String serviceName = executionCommand.getServiceName();
        if (!StringUtils.isEmpty(serviceName)) {
          ServiceInfo serviceInfo = ambariMetaInfo.getService(stackId.getStackName(),
              stackId.getStackVersion(), serviceName);

          commandParams.put(SERVICE_PACKAGE_FOLDER, serviceInfo.getServicePackageFolder());
        }
      }
    } catch (ClusterNotFoundException cnfe) {
      // it's possible that there are commands without clusters; in such cases,
      // just return the de-serialized command and don't try to read configs
      LOG.warn(
          "Unable to lookup the cluster by ID; assuming that there is no cluster and therefore no configs for this execution command: {}",
          cnfe.getMessage());

      return executionCommand;
    } catch (AmbariException e) {
      throw new RuntimeException(e);
    }

    return executionCommand;
  }

  /**
   * Gets the type of command by deserializing the JSON and invoking
   * {@link ExecutionCommand#getCommandType()}.
   *
   * @return
   */
  public AgentCommandType getCommandType() {
    if (executionCommand != null) {
      return executionCommand.getCommandType();
    }

    if (null == jsonExecutionCommand) {
      throw new RuntimeException(
          "Invalid ExecutionCommandWrapper, both object and string" + " representations are null");
    }

    return gson.fromJson(jsonExecutionCommand,
        ExecutionCommand.class).getCommandType();
  }

  public String getJson() {
    if (jsonExecutionCommand != null) {
      return jsonExecutionCommand;
    } else if (executionCommand != null) {
      jsonExecutionCommand = gson.toJson(executionCommand);
      return jsonExecutionCommand;
    } else {
      throw new RuntimeException(
          "Invalid ExecutionCommandWrapper, both object and string"
              + " representations are null");
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ExecutionCommandWrapper wrapper = (ExecutionCommandWrapper) o;

    if (executionCommand != null && wrapper.executionCommand != null) {
      return executionCommand.equals(wrapper.executionCommand);
    } else {
      return getJson().equals(wrapper.getJson());
    }
  }

  @Override
  public int hashCode() {
    if (executionCommand != null) {
      return executionCommand.hashCode();
    } else if (jsonExecutionCommand != null) {
      return jsonExecutionCommand.hashCode();
    }
    throw new RuntimeException("Invalid Wrapper object");
  }

  void invalidateJson() {
    if (executionCommand == null) {
      throw new RuntimeException("Invalid Wrapper object");
    }
    jsonExecutionCommand = null;
  }
}

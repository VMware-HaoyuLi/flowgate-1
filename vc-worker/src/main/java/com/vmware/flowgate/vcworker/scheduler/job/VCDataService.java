/**
 * Copyright 2019 VMware, Inc.
 * SPDX-License-Identifier: BSD-2-Clause
*/
package com.vmware.flowgate.vcworker.scheduler.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vmware.cis.tagging.CategoryModel;
import com.vmware.cis.tagging.CategoryModel.Cardinality;
import com.vmware.cis.tagging.TagModel;
import com.vmware.flowgate.client.WormholeAPIClient;
import com.vmware.flowgate.common.FlowgateConstant;
import com.vmware.flowgate.common.model.Asset;
import com.vmware.flowgate.common.model.AssetIPMapping;
import com.vmware.flowgate.common.model.IntegrationStatus;
import com.vmware.flowgate.common.model.SDDCSoftwareConfig;
import com.vmware.flowgate.common.model.ServerMapping;
import com.vmware.flowgate.common.model.redis.message.AsyncService;
import com.vmware.flowgate.common.model.redis.message.EventMessage;
import com.vmware.flowgate.common.model.redis.message.EventType;
import com.vmware.flowgate.common.model.redis.message.EventUser;
import com.vmware.flowgate.common.model.redis.message.MessagePublisher;
import com.vmware.flowgate.common.model.redis.message.impl.EventMessageImpl;
import com.vmware.flowgate.common.model.redis.message.impl.EventMessageUtil;
import com.vmware.flowgate.common.utils.IPAddressUtil;
import com.vmware.flowgate.vcworker.client.HostTagClient;
import com.vmware.flowgate.vcworker.client.VsphereClient;
import com.vmware.flowgate.vcworker.config.ServiceKeyConfig;
import com.vmware.flowgate.vcworker.model.EsxiMetadata;
import com.vmware.flowgate.vcworker.model.HostInfo;
import com.vmware.flowgate.vcworker.model.HostNic;
import com.vmware.flowgate.vcworker.model.VCConstants;
import com.vmware.vim.binding.vim.AboutInfo;
import com.vmware.vim.binding.vim.ClusterComputeResource;
import com.vmware.vim.binding.vim.HostSystem;
import com.vmware.vim.binding.vim.cluster.ConfigInfoEx;
import com.vmware.vim.binding.vim.cluster.DpmHostConfigInfo;
import com.vmware.vim.binding.vim.fault.InvalidLogin;
import com.vmware.vim.binding.vim.host.Capability;
import com.vmware.vim.binding.vim.host.NetworkInfo;
import com.vmware.vim.binding.vim.host.PhysicalNic;
import com.vmware.vim.binding.vim.host.RuntimeInfo;
import com.vmware.vim.binding.vim.host.Summary;
import com.vmware.vim.binding.vim.host.Summary.HardwareSummary;
import com.vmware.vim.binding.vim.host.Summary.QuickStats;
import com.vmware.vim.binding.vim.host.ConnectInfo.DatastoreInfo;
import com.vmware.vim.binding.vmodl.ManagedObjectReference;
import com.vmware.vim.vmomi.client.exception.ConnectionException;

@Service
public class VCDataService implements AsyncService {

   private static final Logger logger = LoggerFactory.getLogger(VCDataService.class);
   @Autowired
   private WormholeAPIClient restClient;

   @Autowired
   private MessagePublisher publisher;

   @Autowired
   private StringRedisTemplate template;

   @Autowired
   private ServiceKeyConfig serviceKeyConfig;

   private ObjectMapper mapper = new ObjectMapper();

   @Override
   @Async("asyncServiceExecutor")
   public void executeAsync(EventMessage message) {
      // when receive message, will do the related jobs
      // sync customer attribute.
      // update the value.
      if (message.getType() != EventType.VCenter) {
         logger.warn("Drop none vcenter message " + message.getType());
         return;
      }
      // TO, this should be comment out since it may contain vc password.
      logger.info("message received");
      Set<EventUser> users = message.getTarget().getUsers();

      for (EventUser command : users) {
         logger.info(command.getId());
         switch (command.getId()) {
         case EventMessageUtil.VCENTER_SyncData:
            // it will sync all the data depend on the type in the vcjoblist.
            String messageString = null;
            while ((messageString =
                  template.opsForList().rightPop(EventMessageUtil.vcJobList)) != null) {
               EventMessage payloadMessage = null;
               try {
                  payloadMessage = mapper.readValue(messageString, EventMessageImpl.class);
               } catch (IOException e) {
                  logger.error("Cannot process message", e);
               }
               if (payloadMessage == null) {
                  continue;
               }
               SDDCSoftwareConfig vcInfo = null;
               try {
                  vcInfo = mapper.readValue(payloadMessage.getContent(), SDDCSoftwareConfig.class);
               } catch (IOException e) {
                  logger.error("Cannot process message", e);
               }
               if (null == vcInfo) {
                  continue;
               }

               for (EventUser payloadCommand : payloadMessage.getTarget().getUsers()) {
                  switch (payloadCommand.getId()) {
                  case EventMessageUtil.VCENTER_SyncCustomerAttrs:
                     syncCustomAttributes(vcInfo);
                     logger.info("Finish sync customer attributes for " + vcInfo.getName());
                     break;
                  case EventMessageUtil.VCENTER_SyncCustomerAttrsData:
                     syncCustomerAttrsData(vcInfo);
                     logger.info("Finish sync data for " + vcInfo.getName());
                     break;
                  case EventMessageUtil.VCENTER_QueryHostMetaData:
                     queryHostMetaData(vcInfo);
                     logger.info("Finish query host metadata for " + vcInfo.getName());
                     break;
                  default:
                     break;
                  }
               }
            }
            break;
         case EventMessageUtil.VCENTER_SyncCustomerAttrs:
            logger.warn(
                  "VCENTER_SyncCustomerAttrs command is depreacted. use VCENTER_SyncData instead");
            break;
         case EventMessageUtil.VCENTER_SyncCustomerAttrsData:
            logger.warn(
                  "VCENTER_SyncCustomerAttrsData command is depreacted. use VCENTER_SyncData instead");
            break;
         default:
            logger.warn("Not supported command");
            break;
         }
      }
   }

   public void updateIntegrationStatus(SDDCSoftwareConfig config) {
      restClient.setServiceKey(serviceKeyConfig.getServiceKey());
      restClient.updateSDDC(config);
   }

   public void checkAndUpdateIntegrationStatus(SDDCSoftwareConfig vc, String message) {
      IntegrationStatus integrationStatus = vc.getIntegrationStatus();
      if (integrationStatus == null) {
         integrationStatus = new IntegrationStatus();
      }
      int timesOftry = integrationStatus.getRetryCounter();
      timesOftry++;
      if (timesOftry < FlowgateConstant.MAXNUMBEROFRETRIES) {
         integrationStatus.setRetryCounter(timesOftry);
      } else {
         integrationStatus.setStatus(IntegrationStatus.Status.ERROR);
         integrationStatus.setDetail(message);
         integrationStatus.setRetryCounter(FlowgateConstant.DEFAULTNUMBEROFRETRIES);
         logger.error("Failed to sync data to VC");
      }
      vc.setIntegrationStatus(integrationStatus);
      updateIntegrationStatus(vc);
   }

   public VsphereClient connectVsphere(SDDCSoftwareConfig vc) throws Exception {
      return VsphereClient.connect(String.format(VCConstants.SDKURL, vc.getServerURL()),
            vc.getUserName(), vc.getPassword(), !vc.isVerifyCert());
   }

   public HashMap<String, ServerMapping> getVaildServerMapping(SDDCSoftwareConfig vc) {

      HashMap<String, ServerMapping> mobIdDictionary = new HashMap<String, ServerMapping>();
      ServerMapping[] mappings = null;
      try {
         restClient.setServiceKey(serviceKeyConfig.getServiceKey());
         mappings = restClient.getServerMappingsByVC(vc.getId()).getBody();
      } catch (HttpClientErrorException clientError) {
         if (clientError.getRawStatusCode() != HttpStatus.NOT_FOUND.value()) {
            return null;
         }
      }

      for (ServerMapping mapping : mappings) {
         if (mapping.getAsset() != null) {
            mobIdDictionary.put(mapping.getVcMobID(), mapping);
         }
      }
      return mobIdDictionary;

   }

   public void queryHostMetaData(SDDCSoftwareConfig vc) {

      HashMap<String, ServerMapping> serverMappingMap = getVaildServerMapping(vc);
      if (serverMappingMap == null) {
         logger.info("serverMapping is invaild");
         return;
      }

      try (VsphereClient vsphereClient = connectVsphere(vc);) {

         Collection<HostSystem> hosts = vsphereClient.getAllHost();
         if (hosts == null || hosts.isEmpty()) {
            logger.error("AssetId is null");
            return;
         }
         Collection<ClusterComputeResource> clusters = vsphereClient.getAllClusterComputeResource();
         HashMap<String, ClusterComputeResource> clusterMap =
               new HashMap<String, ClusterComputeResource>();
         for (ClusterComputeResource cluster : clusters) {
            clusterMap.put(cluster._getRef().getValue(), cluster);
         }

         for (HostSystem host : hosts) {

            String mobId = host._getRef().getValue();
            if (serverMappingMap.containsKey(mobId)) {
               ServerMapping serverMapping = serverMappingMap.get(mobId);
               String assetId = serverMapping.getAsset();

               Asset hostMappingAsset = restClient.getAssetByID(assetId).getBody();
               if (hostMappingAsset == null) {
                  logger.error("AssetId is null");
                  return;
               }
               HashMap<String, String> hostJustification =
                     hostMappingAsset.getJustificationfields();
               String oldHostInfoString = hostJustification.get(FlowgateConstant.HOST_METADATA);
               HostInfo oldHostInfo = null;
               try {
                  oldHostInfo = mapper.readValue(oldHostInfoString, HostInfo.class);
               } catch (IOException e) {
                  logger.error("Cannot process message", e);
                  return;
               }

               boolean hostNeedUpdate = false;
               boolean clusterNeedUpdate = false;
               HostInfo hostInfo = new HostInfo();
               hostNeedUpdate = feedHostMetaData(oldHostInfo, host, hostInfo);
               if (clusters != null && !clusters.isEmpty()) {
                  EsxiMetadata esxiMetadata = new EsxiMetadata();
                  clusterNeedUpdate =
                        feedClusterMetaData(clusterMap, oldHostInfo, host, esxiMetadata, hostInfo);
               }

               if (hostNeedUpdate || clusterNeedUpdate) {

                  try {

                     String vcHostObjStr = mapper.writeValueAsString(hostInfo);
                     hostJustification.put(FlowgateConstant.HOST_METADATA, vcHostObjStr);
                     hostMappingAsset.setJustificationfields(hostJustification);
                  } catch (JsonProcessingException e) {
                     logger.error("Format host info map error", e);
                     return;
                  }
                  restClient.saveAssets(hostMappingAsset);
               } else {
                  logger.info("No update required");
                  return;
               }
            }
         }
      } catch (ConnectionException e1) {
         checkAndUpdateIntegrationStatus(vc, e1.getMessage());
         return;
      } catch (ExecutionException e2) {
         if (e2.getCause() instanceof InvalidLogin) {
            logger.error("Failed to push data to " + vc.getServerURL(), e2);
            checkAndUpdateIntegrationStatus(vc, "Invalid username or password.");
            return;
         }
      } catch (Exception e) {
         logger.error("Failed to sync the host metadata to VC ", e);
         return;
      }
   }

   public boolean feedClusterMetaData(HashMap<String, ClusterComputeResource> clusterMap,
         HostInfo oldHostInfo, HostSystem host, EsxiMetadata esxiMetadata, HostInfo hostInfo) {

      ManagedObjectReference hostParent = host.getParent();
      if (hostParent == null) {
         return false;
      }
      String clusterName = hostParent.getValue();
      ClusterComputeResource cluster = clusterMap.get(clusterName);
      boolean needUpdate = false;
      EsxiMetadata oldEsxiMetadata = oldHostInfo.getEsxiMetadata();
      if (oldEsxiMetadata == null) {
         needUpdate = true;
      }
      String hostMobId = host._getRef().getValue();

      ConfigInfoEx ci = (ConfigInfoEx) cluster.getConfigurationEx();

      needUpdate =
            needUpdate == true || (cluster.getName().equals(oldEsxiMetadata.getClusterName()))
                  ? needUpdate
                  : true;
      esxiMetadata.setClusterName(cluster.getName());

      needUpdate = needUpdate == true
            || (ci.getDpmConfigInfo().getEnabled().equals(oldEsxiMetadata.isClusterDPMenabled()))
                  ? needUpdate
                  : true;
      esxiMetadata.setClusterDPMenabled(ci.getDpmConfigInfo().getEnabled());

      needUpdate = needUpdate == true || (ci.getDrsConfig().getDefaultVmBehavior().toString()
            .equals(oldEsxiMetadata.getClusterDRSBehavior())) ? needUpdate : true;
      esxiMetadata.setClusterDRSBehavior(ci.getDrsConfig().getDefaultVmBehavior().toString());

      needUpdate =
            needUpdate == true || (cluster.getSummary().getNumEffectiveHosts() == oldEsxiMetadata
                  .getClusterEffectiveHostsNum()) ? needUpdate : true;
      esxiMetadata.setClusterEffectiveHostsNum(cluster.getSummary().getNumEffectiveHosts());

      needUpdate = needUpdate == true
            || (cluster.getSummary().getNumHosts() == oldEsxiMetadata.getClusterHostsNum())
                  ? needUpdate
                  : true;
      esxiMetadata.setClusterHostsNum(cluster.getSummary().getNumHosts());

      needUpdate = needUpdate == true
            || (cluster.getSummary().getTotalCpu() == oldEsxiMetadata.getClusterTotalCpu())
                  ? needUpdate
                  : true;
      esxiMetadata.setClusterTotalCpu(cluster.getSummary().getTotalCpu());

      needUpdate = needUpdate == true
            || (cluster.getSummary().getNumCpuCores() == oldEsxiMetadata.getClusterTotalCpuCores())
                  ? needUpdate
                  : true;
      esxiMetadata.setClusterTotalCpuCores(cluster.getSummary().getNumCpuCores());

      needUpdate = needUpdate == true || (cluster.getSummary().getNumCpuThreads() == oldEsxiMetadata
            .getClusterTotalCpuThreads()) ? needUpdate : true;
      esxiMetadata.setClusterTotalCpuThreads(cluster.getSummary().getNumCpuThreads());

      needUpdate = needUpdate == true
            || (cluster.getSummary().getTotalMemory() == oldEsxiMetadata.getClusterTotalMemory())
                  ? needUpdate
                  : true;
      esxiMetadata.setClusterTotalMemory(cluster.getSummary().getTotalMemory());

      needUpdate = needUpdate == true
            || (ci.getVsanConfigInfo().getEnabled().equals(oldEsxiMetadata.isHostVSANenabled()))
                  ? needUpdate
                  : true;
      esxiMetadata.setClusterVSANenabled(ci.getVsanConfigInfo().getEnabled());

      DpmHostConfigInfo[] dhcis = ci.getDpmHostConfig();
      if (dhcis != null && dhcis.length > 0) {
         for (DpmHostConfigInfo dhci : dhcis) {
            if (hostMobId.equals(dhci.getKey().getValue())) {

               esxiMetadata.setHostDPMenabled(dhci.getEnabled());
            }
         }
      }
      needUpdate = needUpdate == true
            || (cluster._getRef().getValue().equals(oldEsxiMetadata.getClusterMobid())) ? needUpdate
                  : true;
      esxiMetadata.setClusterMobid(cluster._getRef().getValue());

      String clusterInstance =
            cluster.getParent().getValue() == null ? "" : cluster.getParent().getValue();
      needUpdate = needUpdate == true || (clusterInstance.equals(oldEsxiMetadata.getInstanceId()))
            ? needUpdate
            : true;
      esxiMetadata.setInstanceId(clusterInstance);

      needUpdate = needUpdate == true || (host.getName().equals(oldEsxiMetadata.getHostName()))
            ? needUpdate
            : true;
      esxiMetadata.setHostName(host.getName());

      needUpdate = needUpdate == true || (host.getConfig().getVsanHostConfig().getEnabled()
            .equals(oldEsxiMetadata.isHostVSANenabled())) ? needUpdate : true;
      esxiMetadata.setHostVSANenabled(host.getConfig().getVsanHostConfig().getEnabled());

      needUpdate = needUpdate == true || (host.getCapability().getVsanSupported()
            .equals(oldEsxiMetadata.isHostVsanSupported())) ? needUpdate : true;
      esxiMetadata.setHostVsanSupported(host.getCapability().getVsanSupported());

      esxiMetadata.setHostMobid(hostMobId);
      hostInfo.setEsxiMetadata(esxiMetadata);


      return needUpdate;
   }

   public boolean feedHostMetaData(HostInfo oldHostInfo, HostSystem host, HostInfo hostInfo) {

      boolean needUpdate = false;
      Capability capability = host.getCapability();
      if (capability != null) {

         needUpdate = needUpdate || (capability.isMaintenanceModeSupported() != oldHostInfo
               .isMaintenanceModeSupported());
         hostInfo.setMaintenanceModeSupported(capability.isMaintenanceModeSupported());

         needUpdate =
               needUpdate || (capability.isRebootSupported() != oldHostInfo.isRebootSupported());
         hostInfo.setRebootSupported(capability.isRebootSupported());

         Integer maxRunningVMs =
               capability.getMaxRunningVMs() == null ? 0 : capability.getMaxRunningVMs();
         needUpdate = needUpdate || (!maxRunningVMs.equals(oldHostInfo.getMaxRunningVms()));
         hostInfo.setMaxRunningVms(maxRunningVMs);

         Integer maxSupportedVcpus =
               capability.getMaxSupportedVcpus() == null ? 0 : capability.getMaxSupportedVcpus();
         needUpdate = needUpdate || (!maxSupportedVcpus.equals(oldHostInfo.getMaxSupportedVcpus()));
         hostInfo.setMaxSupportedVcpus(maxSupportedVcpus);

         Integer maxRegisteredVMs =
               capability.getMaxRegisteredVMs() == null ? 0 : capability.getMaxRegisteredVMs();
         needUpdate = needUpdate || (!maxRegisteredVMs.equals(oldHostInfo.getMaxRegisteredVMs()));
         hostInfo.setMaxRegisteredVMs(maxRegisteredVMs);
      }

      RuntimeInfo runtimeInfo = host.getRuntime();
      if (runtimeInfo != null) {

         needUpdate = needUpdate || (!runtimeInfo.getConnectionState().toString()
               .equals(oldHostInfo.getConnectionState()));
         hostInfo.setConnectionState(runtimeInfo.getConnectionState().toString());

         needUpdate = needUpdate
               || (!runtimeInfo.getPowerState().toString().equals(oldHostInfo.getPowerState()));
         hostInfo.setPowerState(runtimeInfo.getPowerState().toString());

         needUpdate = needUpdate
               || (runtimeInfo.getBootTime().getTimeInMillis() != oldHostInfo.getBootTime());
         hostInfo.setBootTime(runtimeInfo.getBootTime().getTimeInMillis());
      }

      Summary summary = host.getSummary();
      if (summary != null) {

         needUpdate = needUpdate || (summary.isRebootRequired() != oldHostInfo.isRebootRequired());
         hostInfo.setRebootRequired(summary.isRebootRequired());

         QuickStats quickStats = summary.getQuickStats();
         if (quickStats != null) {

            Integer uptime = quickStats.getUptime() == null ? 0 : quickStats.getUptime();
            hostInfo.setUptime(uptime);
         }

         AboutInfo aboutInfo = summary.getConfig().getProduct();
         if (aboutInfo != null) {
            String build = aboutInfo.getBuild() == null ? "" : aboutInfo.getBuild();
            needUpdate = needUpdate || (!build.equals(oldHostInfo.getHypervisorBuildVersion()));
            hostInfo.setHypervisorBuildVersion(build);

            String fullName = aboutInfo.getFullName() == null ? "" : aboutInfo.getFullName();
            needUpdate = needUpdate || (!fullName.equals(oldHostInfo.getHypervisorFullName()));
            hostInfo.setHypervisorFullName(fullName);

            String licenseProductName = aboutInfo.getLicenseProductName() == null ? ""
                  : aboutInfo.getLicenseProductName();
            needUpdate = needUpdate
                  || (!licenseProductName.equals(oldHostInfo.getHypervisorLicenseProductName()));
            hostInfo.setHypervisorLicenseProductName(licenseProductName);

            String licenseProductVersion = aboutInfo.getLicenseProductVersion() == null ? ""
                  : aboutInfo.getLicenseProductVersion();
            needUpdate = needUpdate || (!licenseProductVersion
                  .equals(oldHostInfo.getHypervisorLicenseProductVersion()));
            hostInfo.setHypervisorLicenseProductVersion(licenseProductVersion);

            String version = aboutInfo.getVersion() == null ? "" : aboutInfo.getVersion();
            needUpdate = needUpdate || (!version.equals(oldHostInfo.getHypervisorVersion()));
            hostInfo.setHypervisorVersion(version);
         }

         HardwareSummary hardwareSummary = summary.getHardware();
         if (hardwareSummary != null) {

            String hostModel = hardwareSummary.getModel() == null ? "" : hardwareSummary.getModel();
            needUpdate = needUpdate || (!hostModel.equals(oldHostInfo.getHostModel()));
            hostInfo.setHostModel(hostModel);

            String vendor = hardwareSummary.getVendor() == null ? "" : hardwareSummary.getVendor();
            needUpdate = needUpdate || (!vendor.equals(oldHostInfo.getHypervisorVendor()));
            hostInfo.setHypervisorVendor(vendor);

            needUpdate = needUpdate
                  || (hardwareSummary.getNumCpuCores() != oldHostInfo.getCpuTotalCores());
            hostInfo.setCpuTotalCores(hardwareSummary.getNumCpuCores());

            needUpdate = needUpdate
                  || (hardwareSummary.getNumCpuPkgs() != oldHostInfo.getCpuTotalPackages());
            hostInfo.setCpuTotalPackages(hardwareSummary.getNumCpuPkgs());

            needUpdate = needUpdate
                  || (hardwareSummary.getNumCpuThreads() != oldHostInfo.getCpuTotalThreads());
            hostInfo.setCpuTotalThreads(hardwareSummary.getNumCpuThreads());

            needUpdate =
                  needUpdate || (hardwareSummary.getCpuMhz() != oldHostInfo.getSingleCoreCpuMhz());
            hostInfo.setSingleCoreCpuMhz(hardwareSummary.getCpuMhz());

            needUpdate = needUpdate
                  || (hardwareSummary.getMemorySize() != oldHostInfo.getMemoryCapacity());
            hostInfo.setMemoryCapacity(hardwareSummary.getMemorySize());
         }
      }

      NetworkInfo networkInfo = host.getConfig().getNetwork();
      if (networkInfo != null) {
         PhysicalNic[] physicalNics = networkInfo.getPnic();
         int nicsNum = physicalNics.length;
         if (nicsNum > 0) {
            List<HostNic> hostNics = new ArrayList<HostNic>();

            for (int i = 0; i < nicsNum; i++) {

               HostNic hostNic = new HostNic();

               hostNic.setMacAddress(physicalNics[i].getMac());
               hostNic.setDriver(physicalNics[i].getDriver());
               hostNic.setDuplex(physicalNics[i].getLinkSpeed() == null ? false
                     : physicalNics[i].getLinkSpeed().isDuplex());
               hostNic.setLinkSpeedMb(physicalNics[i].getLinkSpeed() == null ? 0
                     : physicalNics[i].getLinkSpeed().getSpeedMb());
               hostNic.setName(physicalNics[i].getDevice());
               hostNics.add(hostNic);
            }

            List<HostNic> oldNics = oldHostInfo.getHostNics();
            boolean nicNeedUpdate = false;
            if (oldNics.size() == hostNics.size()) {
               for (HostNic oldNic : oldNics) {
                  for (HostNic nic : hostNics) {
                     nicNeedUpdate =
                           oldNic.getDriver().equals(nic.getDriver()) ? nicNeedUpdate : true;
                     nicNeedUpdate =
                           oldNic.getLinkSpeedMb() == nic.getLinkSpeedMb() ? nicNeedUpdate : true;
                     nicNeedUpdate =
                           oldNic.getMacAddress().equals(nic.getMacAddress()) ? nicNeedUpdate
                                 : true;
                     nicNeedUpdate = oldNic.getName().equals(nic.getName()) ? nicNeedUpdate : true;
                  }
               }
            } else {
               nicNeedUpdate = true;
            }
            needUpdate = needUpdate || nicNeedUpdate;
            hostInfo.setHostNics(hostNics);
         }
      }

      long diskCapacity = 0;
      DatastoreInfo[] datastores = host.queryConnectionInfo().getDatastore();
      if (datastores != null && datastores.length > 0) {
         for (DatastoreInfo datastore : datastores) {
            diskCapacity += datastore.getSummary().getCapacity();
         }
         needUpdate = needUpdate || (diskCapacity != oldHostInfo.getDiskCapacity());
         hostInfo.setDiskCapacity(diskCapacity);
      }

      return needUpdate;
   }


   private void syncCustomAttributes(SDDCSoftwareConfig vc) {
      // TODO need to allow only update 1 vcenter instead of all the vcenter.

      try (VsphereClient vsphereClient =
            VsphereClient.connect(String.format(VCConstants.SDKURL, vc.getServerURL()),
                  vc.getUserName(), vc.getPassword(), !vc.isVerifyCert());) {
         for (String key : VCConstants.hostCustomAttrMapping.values()) {
            vsphereClient.createCustomAttribute(key, VCConstants.HOSTSYSTEM);
         }
         // Add the PDU information;
         vsphereClient.createCustomAttribute(VCConstants.ASSET_PDUs, VCConstants.HOSTSYSTEM);
         // Add host switch information;
         vsphereClient.createCustomAttribute(VCConstants.ASSET_SWITCHs, VCConstants.HOSTSYSTEM);
      } catch (ConnectionException e1) {
         checkAndUpdateIntegrationStatus(vc, e1.getMessage());
         return;
      } catch (ExecutionException e2) {
         if (e2.getCause() instanceof InvalidLogin) {
            logger.error("Failed to push data to " + vc.getServerURL(), e2);
            IntegrationStatus integrationStatus = vc.getIntegrationStatus();
            if (integrationStatus == null) {
               integrationStatus = new IntegrationStatus();
            }
            integrationStatus.setStatus(IntegrationStatus.Status.ERROR);
            integrationStatus.setDetail("Invalid username or password.");
            integrationStatus.setRetryCounter(FlowgateConstant.DEFAULTNUMBEROFRETRIES);
            updateIntegrationStatus(vc);
            return;
         }
      } catch (Exception e) {
         logger.error("Failed to sync the host metadata to VC ", e);
         return;
      }
      try (HostTagClient client = new HostTagClient(vc.getServerURL(), vc.getUserName(),
            vc.getPassword(), !vc.isVerifyCert());) {
         client.initConnection();
         TagModel tag = client.getTagByName(VCConstants.locationAntiAffinityTagName);
         String categoryID;
         if (tag == null) {
            CategoryModel category = client.getTagCategoryByName(VCConstants.categoryName);
            if (category == null) {
               categoryID = client.createTagCategory(VCConstants.categoryName,
                     VCConstants.categoryDescription, Cardinality.MULTIPLE);
            } else {
               categoryID = category.getId();
            }
            client.createTag(VCConstants.locationAntiAffinityTagName,
                  VCConstants.locationAntiAffinityTagDescription, categoryID);
         }
      } catch (Exception e) {
         logger.error("Faild to check the predefined tag information", e);
      }
   }

   private void syncCustomerAttrsData(SDDCSoftwareConfig vcInfo) {
      restClient.setServiceKey(serviceKeyConfig.getServiceKey());

      try (VsphereClient vsphereClient =
            VsphereClient.connect(String.format(VCConstants.SDKURL, vcInfo.getServerURL()),
                  vcInfo.getUserName(), vcInfo.getPassword(), !vcInfo.isVerifyCert());) {
         ServerMapping[] mappings = null;
         try {
            mappings = restClient.getServerMappingsByVC(vcInfo.getId()).getBody();
         } catch (HttpClientErrorException clientError) {
            if (clientError.getRawStatusCode() != HttpStatus.NOT_FOUND.value()) {
               throw clientError;
            }
            mappings = new ServerMapping[0];
         }
         HashMap<String, ServerMapping> mobIdDictionary = new HashMap<String, ServerMapping>();
         for (ServerMapping mapping : mappings) {
            mobIdDictionary.put(mapping.getVcMobID(), mapping);
         }
         List<ServerMapping> validMapping = new ArrayList<ServerMapping>();
         Collection<HostSystem> hosts = vsphereClient.getAllHost();
         Map<String, HostSystem> hostDictionary = new HashMap<String, HostSystem>();
         for (HostSystem host : hosts) {
            String mobId = host._getRef().getValue();
            String hostName = host.getName();
            if (mobIdDictionary.containsKey(mobId)) {
               ServerMapping serverMapping = mobIdDictionary.get(mobId);
               if (!serverMapping.getVcHostName().equals(hostName)) {
                  // need to update the hostname.
                  serverMapping.setVcHostName(hostName);
                  restClient.saveServerMapping(serverMapping);
               }
               if (serverMapping.getAsset() != null) {
                  validMapping.add(serverMapping);
               } else {
                  // check the hostNameIP mapping
                  String ipaddress = IPAddressUtil.getIPAddress(hostName);
                  if (null != ipaddress) {
                     AssetIPMapping[] ipMappings =
                           restClient.getHostnameIPMappingByIP(ipaddress).getBody();
                     if (null != ipMappings && ipMappings.length > 0) {
                        // update the mapping
                        String assetName = ipMappings[0].getAssetname();
                        Asset asset = restClient.getAssetByName(assetName).getBody();
                        if (asset != null) {
                           serverMapping.setAsset(asset.getId());
                           restClient.saveServerMapping(serverMapping);
                           validMapping.add(serverMapping);
                        }
                     } else {// seems we don't have the ip hostname mapping. Notify infoblox to check the ip
                        logger.info("Notify infoblox to check ip: " + ipaddress);
                        publisher.publish(null, ipaddress);
                     }
                  }
               }
               hostDictionary.put(mobId, host);
            } else {
               ServerMapping newMapping = new ServerMapping();
               newMapping.setVcHostName(hostName);
               newMapping.setVcID(vcInfo.getId());
               newMapping.setVcMobID(mobId);
               newMapping.setVcInstanceUUID(vsphereClient.getVCUUID());
               String ipaddress = IPAddressUtil.getIPAddress(hostName);
               logger.info(String.format("hostName %s, ipaddress: %s", hostName, ipaddress));
               // publish message to queue.
               if (null != ipaddress) {
                  logger.info("Notify infoblox");
                  publisher.publish(null, hostName);
                  AssetIPMapping[] ipMappings =
                        restClient.getHostnameIPMappingByIP(ipaddress).getBody();
                  if (null != ipMappings && ipMappings.length > 0) {
                     // update the mapping
                     String assetName = ipMappings[0].getAssetname();
                     Asset asset = restClient.getAssetByName(assetName).getBody();
                     if (asset != null) {
                        newMapping.setAsset(asset.getId());
                     }
                  }
               }
               restClient.saveServerMapping(newMapping);
            }
         }
         // feed meta data to VC.
         Asset[] assets = restClient.getAssetsByVCID(vcInfo.getId()).getBody();
         Map<String, Asset> assetDictionary = new HashMap<String, Asset>();
         for (Asset asset : assets) {
            assetDictionary.put(asset.getId(), asset);
         }

         feedData(assetDictionary, validMapping, hostDictionary);
         validClusterHostsLocationAntiaffinity(vcInfo, assetDictionary, validMapping);
      } catch (ConnectionException e1) {
         checkAndUpdateIntegrationStatus(vcInfo, e1.getMessage());
         return;
      } catch (ExecutionException e2) {
         if (e2.getCause() instanceof InvalidLogin) {
            logger.error("Failed to push data to " + vcInfo.getServerURL(), e2);
            IntegrationStatus integrationStatus = vcInfo.getIntegrationStatus();
            if (integrationStatus == null) {
               integrationStatus = new IntegrationStatus();
            }
            integrationStatus.setStatus(IntegrationStatus.Status.ERROR);
            integrationStatus.setDetail("Invalid username or password.");
            integrationStatus.setRetryCounter(FlowgateConstant.DEFAULTNUMBEROFRETRIES);
            updateIntegrationStatus(vcInfo);
            return;
         }
      } catch (Exception e) {
         logger.error("Failed to push data to " + vcInfo.getServerURL(), e);
      }
   }

   private void feedData(Map<String, Asset> assetDictionary, List<ServerMapping> validMapping,
         Map<String, HostSystem> hostDictionary) {
      restClient.setServiceKey(serviceKeyConfig.getServiceKey());
      for (ServerMapping validServer : validMapping) {
         HostSystem host = hostDictionary.get(validServer.getVcMobID());
         Asset asset = assetDictionary.get(validServer.getAsset());
         BeanWrapper wrapper = PropertyAccessorFactory.forBeanPropertyAccess(asset);
         for (String key : VCConstants.hostCustomAttrMapping.keySet()) {
            host.setCustomValue(VCConstants.hostCustomAttrMapping.get(key),
                  String.valueOf(wrapper.getPropertyValue(key)));
         }
         Map<String, String> idNamePortMapping = getPDUSwitchIDNamePortMapping(asset);
         if (asset.getPdus() != null) {
            // check pdu and port
            List<String> pduNameList = new ArrayList<String>();
            for (String pduid : asset.getPdus()) {
               if (idNamePortMapping.containsKey(pduid)) {
                  pduNameList.add(idNamePortMapping.get(pduid));
               } else {
                  Asset pduAsset = restClient.getAssetByID(pduid).getBody();
                  if (pduAsset != null) {
                     pduNameList.add(pduAsset.getAssetName());
                  }
               }
            }
            host.setCustomValue(VCConstants.ASSET_PDUs, String.join(",", pduNameList));
         }
         if (asset.getSwitches() != null) {
            List<String> switchNameList = new ArrayList<String>();
            for (String switchID : asset.getSwitches()) {
               if (idNamePortMapping.containsKey(switchID)) {
                  switchNameList.add(idNamePortMapping.get(switchID));
               } else {
                  Asset switchAsset = restClient.getAssetByID(switchID).getBody();
                  if (switchAsset != null) {
                     switchNameList.add(switchAsset.getAssetName());
                  }
               }
            }
            host.setCustomValue(VCConstants.ASSET_SWITCHs, String.join(",", switchNameList));
         }
      }
   }

   public Map<String, String> getPDUSwitchIDNamePortMapping(Asset asset) {
      Map<String, String> result = new HashMap<String, String>();
      Map<String, String> enhanceFields = asset.getJustificationfields();
      if (null != enhanceFields) {
         String allPduPortString = enhanceFields.get(FlowgateConstant.PDU_PORT_FOR_SERVER);
         List<String> devicePorts = new ArrayList<String>();
         if (!StringUtils.isEmpty(allPduPortString)) {
            devicePorts = Arrays.asList(allPduPortString.split(FlowgateConstant.SPILIT_FLAG));
         }

         String allSwitchPortString = enhanceFields.get(FlowgateConstant.NETWORK_PORT_FOR_SERVER);
         if (!StringUtils.isEmpty(allSwitchPortString)) {
            devicePorts
                  .addAll(Arrays.asList(allSwitchPortString.split(FlowgateConstant.SPILIT_FLAG)));
         }

         for (String devicePortString : devicePorts) {
            // startport_FIELDSPLIT_endDeviceName_FIELDSPLIT_endport_FIELDSPLIT_endDeviceAssetID
            // item[0] start port
            // item[1] device name
            // item[2] end port
            // itme[3] assetid
            String items[] = devicePortString.split(FlowgateConstant.SEPARATOR);
            result.put(items[3], items[1] + ":" + items[2]);
         }
      }
      return result;
   }

   private void validClusterHostsLocationAntiaffinity(SDDCSoftwareConfig vcInfo,
         Map<String, Asset> assetDictionary, List<ServerMapping> validMapping) {

      Map<String, Set<Asset>> assetsByCluster = new HashMap<String, Set<Asset>>();
      for (ServerMapping validServer : validMapping) {
         Asset asset = assetDictionary.get(validServer.getAsset());
         if (null != validServer.getVcClusterMobID()) {
            if (!assetsByCluster.containsKey(validServer.getVcClusterMobID())) {
               assetsByCluster.put(validServer.getVcClusterMobID(), new HashSet<Asset>());
            }
            assetsByCluster.get(validServer.getVcClusterMobID()).add(asset);
         }
      }
      Set<Asset> needTagHost = new HashSet<>();
      for (String clusterMob : assetsByCluster.keySet()) {
         if (assetsByCluster.get(clusterMob).size() > 1) {
            String location = "%s-%s-%s-%s-%s-%s-%d";
            Map<String, Set<Asset>> assetsByLocation = new HashMap<String, Set<Asset>>();
            for (Asset asset : assetsByCluster.get(clusterMob)) {
               String assetLocation = String.format(location, asset.getRegion(), asset.getCountry(),
                     asset.getCity(), asset.getBuilding(), asset.getFloor(),
                     asset.getCabinetAssetNumber(), asset.getCabinetUnitPosition());
               if (!assetsByLocation.containsKey(assetLocation)) {
                  assetsByLocation.put(assetLocation, new HashSet<Asset>());
               }
               assetsByLocation.get(assetLocation).add(asset);
            }
            for (String local : assetsByLocation.keySet()) {
               if (assetsByLocation.get(local).size() > 1) {
                  // now we need to tag the hosts
                  needTagHost.addAll(assetsByLocation.get(local));
               }
            }
         }
      }

      if (!needTagHost.isEmpty()) {
         Map<String, ServerMapping> assetIDMapping = new HashMap<String, ServerMapping>();
         for (ServerMapping mapping : validMapping) {
            assetIDMapping.put(mapping.getAsset(), mapping);
         }
         try (HostTagClient client = new HostTagClient(vcInfo.getServerURL(), vcInfo.getUserName(),
               vcInfo.getPassword(), !vcInfo.isVerifyCert());) {
            client.initConnection();
            TagModel locationTag = client.getTagByName(VCConstants.locationAntiAffinityTagName);
            for (Asset a : needTagHost) {
               client.attachTagToHost(locationTag.getId(),
                     assetIDMapping.get(a.getId()).getVcMobID());
            }
         } catch (Exception e) {
            logger.warn("Failed to tag the host, will try to tag it in next run.", e);
         }
      }
   }

}

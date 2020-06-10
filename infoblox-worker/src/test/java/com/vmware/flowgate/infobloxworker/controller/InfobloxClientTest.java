/**
 * Copyright 2019 VMware, Inc.
 * SPDX-License-Identifier: BSD-2-Clause
*/
package com.vmware.flowgate.infobloxworker.controller;

import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.client.ClientProtocolException;

import com.vmware.flowgate.infobloxworker.model.Infoblox;
import com.vmware.flowgate.infobloxworker.model.JsonResultForQueryHostNames;
import com.vmware.flowgate.infobloxworker.service.InfobloxClient;
import com.vmware.flowgate.client.WormholeAPIClient;
import com.vmware.flowgate.common.model.FacilitySoftwareConfig;
import com.vmware.flowgate.common.model.FacilitySoftwareConfig.SoftwareType;

import ch.qos.logback.classic.Logger;
import junit.framework.TestCase;

public class InfobloxClientTest {

    @Mock
    private JsonResultForQueryHostNames jsonHostNameResult = new JsonResultForQueryHostNames();


}

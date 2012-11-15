package org.apache.hadoop.yarn.server.resourcemanager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.AMResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.records.HeartbeatResponse;
import org.apache.hadoop.yarn.server.api.records.NodeAction;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.MemoryRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.NdbRMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.ApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.StoreFactory;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before;

/**
 *
 * @author aknahs
 */
public class TestNdbRmRestart {

    private static final Log LOG = LogFactory.getLog(TestNdbRmRestart.class);

    @Before
    public void setUp() throws Exception {
        NdbRMStateStore ndbStore = new NdbRMStateStore();
        ndbStore.clearData();
    }

    @Test
    public void testNdbRmRestart() throws Exception {
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.DEBUG);
        NdbRMStateStore ndbStore = new NdbRMStateStore();
        RMState rmState = ndbStore.loadState();
        Map<ApplicationId, ApplicationState> rmAppState =
                rmState.getApplicationState();

        YarnConfiguration conf = new YarnConfiguration();

        MockRM rm1 = new MockRM(conf, true, ndbStore) {
            @Override
            protected void doSecureLogin() throws IOException {
                // Skip the login.
            }
        };

        // start like normal because state is empty
        rm1.start();
        MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
        MockNM nm2 = new MockNM("h2:5678", 15120, rm1.getResourceTrackerService());
        nm1.registerNode();
        nm2.registerNode();

        // app that gets launched
        RMApp app1 = rm1.submitApp(200);

        //Load data from ndb
        //rmState = ndbStore.loadState();
        rmAppState = rmState.getApplicationState();
        //end load data

        // assert app1 info is saved
        ApplicationState appState = rmAppState.get(app1.getApplicationId());
        Assert.assertNotNull(appState);
        Assert.assertEquals(0, appState.getAttemptCount());
        //TODO fix this assertion, whether we need to store the resource after
        //modified by scheduler or the original resource requeset by 
        //the application
        //Assert.assertEquals(appState.getApplicationSubmissionContext(),
        //        app1.getApplicationSubmissionContext());


        //kick the scheduling
        nm1.nodeHeartbeat(true);
        nm2.nodeHeartbeat(true);

        // assert app1 attempt is saved
        RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
        ApplicationAttemptId attemptId1 = attempt1.getAppAttemptId();
        rm1.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
        
        //Load data from ndb
        //rmState = ndbStore.loadState();
        rmAppState = rmState.getApplicationState();
        appState = rmAppState.get(app1.getApplicationId());
        //end load data
        
        Assert.assertEquals(1, appState.getAttemptCount());
        ApplicationAttemptState attemptState =
                appState.getAttempt(attemptId1);
        Assert.assertNotNull(attemptState);
        Assert.assertEquals(BuilderUtils.newContainerId(attemptId1, 1),
                attemptState.getMasterContainer().getId());


        MockAM am1 = rm1.sendAMLaunched(attempt1.getAppAttemptId());
        am1.registerAppAttempt();

        //request for containers
        am1.allocate("h1", 1000, 3, new ArrayList<ContainerId>());

        // kick the scheduler - XXX remove this part
        nm1.nodeHeartbeat(true);
        List<Container> conts = am1.allocate(new ArrayList<ResourceRequest>(),
                new ArrayList<ContainerId>()).getAllocatedContainers();
        int contReceived = conts.size();
        while (contReceived < 3) {//only 3 containers are available on node1
            conts.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
                    new ArrayList<ContainerId>()).getAllocatedContainers());
            // TODO this is wrong. Should be += 
            contReceived = conts.size();
            LOG.info("Got " + contReceived + " containers. Waiting to get " + 3);
            Thread.sleep(2000);
        }
        Assert.assertEquals(3, conts.size());

        // app that does not get launched
        RMApp app2 = rm1.submitApp(200);

        // assert app2 info is saved
        
        //Load data from ndb
        //rmState = ndbStore.loadState();
        rmAppState = rmState.getApplicationState();
        //end load data
        appState = rmAppState.get(app2.getApplicationId());
        Assert.assertNotNull(appState);
        Assert.assertEquals(0, appState.getAttemptCount());
        //TODO: fix this assert later, same as previous error in app1
        //Assert.assertEquals(appState.getApplicationSubmissionContext(),
          //      app2.getApplicationSubmissionContext());

        // create new RM
        MockRM rm2 = new MockRM(conf, true, ndbStore) {
            @Override
            protected void doSecureLogin() throws IOException {
                // Skip the login.
            }
        };

        // recover state
        rmState = ndbStore.loadState();
        rmAppState = rmState.getApplicationState();
        rm2.recover(rmState);

        nm1.setResourceTrackerService(rm2.getResourceTrackerService());
        nm2.setResourceTrackerService(rm2.getResourceTrackerService());

        // start with load of old state
        RMApp loadedApp1 = rm2.getRMContext().getRMApps().get(app1.getApplicationId());
        Assert.assertNotNull(loadedApp1);
        Assert.assertEquals(1, loadedApp1.getAppAttempts().size());
        //TODO: fix this assert
        //Assert.assertEquals(app1.getApplicationSubmissionContext(),
        //        loadedApp1.getApplicationSubmissionContext());
        RMApp loadedApp2 = rm2.getRMContext().getRMApps().get(app2.getApplicationId());
        Assert.assertNotNull(loadedApp2);
        Assert.assertEquals(0, loadedApp2.getAppAttempts().size());
        //TODO: fix this assert
        //Assert.assertEquals(app2.getApplicationSubmissionContext(),
        //        loadedApp2.getApplicationSubmissionContext());

        rm2.start();

        rm2.waitForState(loadedApp1.getApplicationId(), RMAppState.ACCEPTED);
        rm2.waitForState(loadedApp2.getApplicationId(), RMAppState.ACCEPTED);

        // starting should create new attempts
        Assert.assertEquals(2, loadedApp1.getAppAttempts().size());
        Assert.assertEquals(1, loadedApp2.getAppAttempts().size());

        // TODO add AM register exception here

        // NM should be rebooted on heartbeat, even first heartbeat for nm2
        HeartbeatResponse hbResponse = nm1.nodeHeartbeat(true);
        Assert.assertEquals(NodeAction.REBOOT, hbResponse.getNodeAction());
        hbResponse = nm2.nodeHeartbeat(true);
        Assert.assertEquals(NodeAction.REBOOT, hbResponse.getNodeAction());

        nm1 = rm2.registerNode("h1:1234", 15120);
        nm2 = rm2.registerNode("h2:5678", 15120);

        hbResponse = nm1.nodeHeartbeat(true);
        Assert.assertTrue(NodeAction.REBOOT != hbResponse.getNodeAction());
        hbResponse = nm2.nodeHeartbeat(true);
        Assert.assertTrue(NodeAction.REBOOT != hbResponse.getNodeAction());

        // TODO test for AM register and finish 
        // old AM should get reboot
        am1.setAMRMProtocol(rm2.getApplicationMasterService());
        AMResponse amResponse = am1.allocate(new ArrayList<ResourceRequest>(),
                new ArrayList<ContainerId>());
        Assert.assertTrue(amResponse.getReboot());

        nm1.nodeHeartbeat(true);
        nm2.nodeHeartbeat(true);

        // assert app1 attempt is saved
        attempt1 = loadedApp1.getCurrentAppAttempt();
        attemptId1 = attempt1.getAppAttemptId();
        rm2.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
        
        //Load data from ndb
        rmState = ndbStore.loadState();
        rmAppState = rmState.getApplicationState();
        //end load data
        
        appState = rmAppState.get(loadedApp1.getApplicationId());
        attemptState = appState.getAttempt(attemptId1);
        Assert.assertNotNull(attemptState);
        Assert.assertEquals(BuilderUtils.newContainerId(attemptId1, 1),
                attemptState.getMasterContainer().getId());

        nm1.nodeHeartbeat(true);
        nm2.nodeHeartbeat(true);

        // assert app1 attempt is saved
        RMAppAttempt attempt2 = loadedApp2.getCurrentAppAttempt();
        ApplicationAttemptId attemptId2 = attempt2.getAppAttemptId();
        rm2.waitForState(attemptId2, RMAppAttemptState.ALLOCATED);
        
        //Load data from ndb
        rmState = ndbStore.loadState();
        rmAppState = rmState.getApplicationState();
        //end load data
        
        appState = rmAppState.get(loadedApp2.getApplicationId());
        attemptState = appState.getAttempt(attemptId2);
        Assert.assertNotNull(attemptState);
        Assert.assertEquals(BuilderUtils.newContainerId(attemptId2, 1),
                attemptState.getMasterContainer().getId());

        am1 = rm2.sendAMLaunched(attempt1.getAppAttemptId());
        am1.registerAppAttempt();

        MockAM am2 = rm2.sendAMLaunched(attempt2.getAppAttemptId());
        am2.registerAppAttempt();

        //request for containers
        am1.allocate("h1", 1000, 3, new ArrayList<ContainerId>());

        // kick the scheduler - XXX remove this part
        nm1.nodeHeartbeat(true);
        nm2.nodeHeartbeat(true);
        conts = am1.allocate(new ArrayList<ResourceRequest>(),
                new ArrayList<ContainerId>()).getAllocatedContainers();
        contReceived = conts.size();
        while (contReceived < 3) {//only 3 containers are available on node1
            conts.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
                    new ArrayList<ContainerId>()).getAllocatedContainers());
            contReceived = conts.size();
            LOG.info("Got " + contReceived + " containers. Waiting to get " + 3);
            Thread.sleep(2000);
        }
        Assert.assertEquals(3, conts.size());

        am1.unregisterAppAttempt();
        nm1.nodeHeartbeat(attempt1.getAppAttemptId(), 1, ContainerState.COMPLETE);
        am1.waitForState(RMAppAttemptState.FINISHED);

        am2.unregisterAppAttempt();
        nm1.nodeHeartbeat(attempt2.getAppAttemptId(), 1, ContainerState.COMPLETE);
        am2.waitForState(RMAppAttemptState.FINISHED);

        rm2.stop();
        rm1.stop();
    }

    //@Test
    public void testCapacitySchedulerBug() throws Exception {
        Logger rootLogger = LogManager.getRootLogger();
        rootLogger.setLevel(Level.DEBUG);
        YarnConfiguration conf = new YarnConfiguration();

        MockRM rm1 = new MockRM(conf) {
            @Override
            protected void doSecureLogin() throws IOException {
                // Skip the login.
            }
        };

        // start like normal because state is empty
        rm1.start();

        // app that gets launched
        RMApp app1 = rm1.submitApp(200);

        // app that does not get launched
        RMApp app2 = rm1.submitApp(200);

        MockNM nm1 = new MockNM("h1:1234", 15120, rm1.getResourceTrackerService());
        MockNM nm2 = new MockNM("h2:5678", 15120, rm1.getResourceTrackerService());
        nm1.registerNode();
        nm2.registerNode();

        //kick the scheduling
        nm1.nodeHeartbeat(true);
        nm2.nodeHeartbeat(true);

        // assert app1 attempt is saved
        RMAppAttempt attempt1 = app1.getCurrentAppAttempt();
        ApplicationAttemptId attemptId1 = attempt1.getAppAttemptId();
        rm1.waitForState(attemptId1, RMAppAttemptState.ALLOCATED);
        RMAppAttempt attempt2 = app2.getCurrentAppAttempt();
        ApplicationAttemptId attemptId2 = attempt2.getAppAttemptId();
        rm1.waitForState(attemptId2, RMAppAttemptState.ALLOCATED);

        MockAM am1 = rm1.sendAMLaunched(attempt1.getAppAttemptId());
        am1.registerAppAttempt();

        //request for containers
        am1.allocate("h1", 1000, 3, new ArrayList<ContainerId>());

        // kick the scheduler - XXX remove this part
        nm1.nodeHeartbeat(true);
        List<Container> conts = am1.allocate(new ArrayList<ResourceRequest>(),
                new ArrayList<ContainerId>()).getAllocatedContainers();
        int contReceived = conts.size();
        while (contReceived < 3) {//only 3 containers are available on node1
            conts.addAll(am1.allocate(new ArrayList<ResourceRequest>(),
                    new ArrayList<ContainerId>()).getAllocatedContainers());
            // TODO this is wrong. Should be += 
            contReceived = conts.size();
            LOG.info("Got " + contReceived + " containers. Waiting to get " + 3);
            Thread.sleep(2000);
        }
        Assert.assertEquals(3, conts.size());

        am1.unregisterAppAttempt();
        nm1.nodeHeartbeat(attempt1.getAppAttemptId(), 1, ContainerState.COMPLETE);
        am1.waitForState(RMAppAttemptState.FINISHED);

        rm1.stop();
    }
}

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptEventType;


public class MemoryRMStateStore implements RMStateStore {
  
  public class MemoryApplicationAttemptState implements ApplicationAttemptState {
    Container masterContainer;

    @Override
    public Container getMasterContainer() {
      return masterContainer;
    }
  }
  
  public class MemoryApplicationState implements ApplicationState {    
    ApplicationId appId;
    long submitTime;
    ApplicationSubmissionContext applicationSubmissionContext;
    HashMap<ApplicationAttemptId, MemoryApplicationAttemptState> attempts =
        new HashMap<ApplicationAttemptId, MemoryApplicationAttemptState>();
    
    @Override
    public ApplicationId getId() {
      return appId;
    }

    @Override
    public long getSubmitTime() {
      return submitTime;
    }

    @Override
    public ApplicationSubmissionContext getApplicationSubmissionContext() {
      return applicationSubmissionContext;
    }

    @Override
    public int getAttemptCount() {
      return attempts.size();
    }

    @Override
    public ApplicationAttemptState getAttempt(ApplicationAttemptId attemptId) {
      MemoryApplicationAttemptState attemptState = attempts.get(attemptId);
      return attemptState;
    }
    
  }
  
  public class MemoryRMState implements RMState {
    HashMap<ApplicationId, ApplicationState> appState = 
        new HashMap<ApplicationId, ApplicationState>();

    @Override
    public Map<ApplicationId, ApplicationState> getApplicationState() {
      return appState;
    }
    
  }
  
  MemoryRMState state = new MemoryRMState();
  
  Dispatcher dispatcher;
  
  @Override
  public void setDispatcher(Dispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }
  
  @Override
  public RMState loadState() {
    return state;
  }

  @Override
  public void storeApplication(RMApp app) {
    MemoryApplicationState storedApp = new MemoryApplicationState();
    storedApp.appId = app.getApplicationId();
    storedApp.submitTime = app.getSubmitTime();
    storedApp.applicationSubmissionContext = app.getApplicationSubmissionContext();
    state.appState.put(app.getApplicationId(), storedApp);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void storeApplicationAttempt(RMAppAttempt appAttempt) {
    MemoryApplicationState appState = (MemoryApplicationState) 
    state.getApplicationState().get(appAttempt.getAppAttemptId().getApplicationId());
    assert appState != null;
    
    MemoryApplicationAttemptState attemptState = new MemoryApplicationAttemptState();
    attemptState.masterContainer = appAttempt.getMasterContainer();
    appState.attempts.put(appAttempt.getAppAttemptId(), attemptState);
        
    //TODO move this to base class method and remove java imports
    dispatcher.getEventHandler().handle(
        new RMAppAttemptEvent(appAttempt.getAppAttemptId(), 
                              RMAppAttemptEventType.ATTEMPT_SAVED));
  }

}

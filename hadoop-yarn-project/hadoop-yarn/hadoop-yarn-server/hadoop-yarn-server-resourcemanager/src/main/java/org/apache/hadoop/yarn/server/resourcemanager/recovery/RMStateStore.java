/**
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

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;

@Private
@Unstable
public interface RMStateStore {
  
  public interface ApplicationAttemptState {
    Container getMasterContainer();
  }
  
  public interface ApplicationState {
    ApplicationId getId();
    long getSubmitTime();
    ApplicationSubmissionContext getApplicationSubmissionContext();
    int getAttemptCount();
    ApplicationAttemptState getAttempt(ApplicationAttemptId attemptId);
  }
  
  public interface RMState {
    Map<ApplicationId, ApplicationState> getApplicationState();
  }
  
  void setDispatcher(Dispatcher dispatcher);
  
  RMState loadState();
  
  /**
   * Blocking API
   */
  void storeApplication(RMApp app);
  
  /**
   * Non-blocking API
   */
  void storeApplicationAttempt(RMAppAttempt appAttempt);

}
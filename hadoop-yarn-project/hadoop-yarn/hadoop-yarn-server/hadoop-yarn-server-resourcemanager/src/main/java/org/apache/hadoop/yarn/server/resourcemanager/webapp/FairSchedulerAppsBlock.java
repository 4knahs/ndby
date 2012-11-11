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

package org.apache.hadoop.yarn.server.resourcemanager.webapp;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.YarnWebParams.APP_STATE;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._PROGRESSBAR;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._PROGRESSBAR_VALUE;

import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.FairSchedulerInfo;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;
import org.apache.hadoop.yarn.webapp.view.JQueryUI.Render;

import com.google.inject.Inject;

/**
 * Shows application information specific to the fair
 * scheduler as part of the fair scheduler page.
 */
public class FairSchedulerAppsBlock extends HtmlBlock {
  final AppsList list;
  final FairSchedulerInfo fsinfo;
  
  @Inject public FairSchedulerAppsBlock(AppsList list, 
      ResourceManager rm, ViewContext ctx) {
    super(ctx);
    this.list = list;
    FairScheduler scheduler = (FairScheduler) rm.getResourceScheduler();
    fsinfo = new FairSchedulerInfo(scheduler);
  }
  
  @Override public void render(Block html) {
    TBODY<TABLE<Hamlet>> tbody = html.
      table("#apps").
        thead().
          tr().
            th(".id", "ID").
            th(".user", "User").
            th(".name", "Name").
            th(".queue", "Queue").
            th(".fairshare", "Fair Share").
            th(".starttime", "StartTime").
            th(".finishtime", "FinishTime").
            th(".state", "State").
            th(".finalstatus", "FinalStatus").
            th(".progress", "Progress").
            th(".ui", "Tracking UI")._()._().
        tbody();
    int i = 0;
    Collection<RMAppState> reqAppStates = null;
    String reqStateString = $(APP_STATE);
    if (reqStateString != null && !reqStateString.isEmpty()) {
      String[] appStateStrings = reqStateString.split(",");
      reqAppStates = new HashSet<RMAppState>(appStateStrings.length);
      for(String stateString : appStateStrings) {
        reqAppStates.add(RMAppState.valueOf(stateString));
      }
    }
    for (RMApp app : list.apps.values()) {
      if (reqAppStates != null && !reqAppStates.contains(app.getState())) {
        continue;
      }
      AppInfo appInfo = new AppInfo(app, true);
      String percent = String.format("%.1f", appInfo.getProgress());
      String startTime = Times.format(appInfo.getStartTime());
      String finishTime = Times.format(appInfo.getFinishTime());
      ApplicationAttemptId attemptId = app.getCurrentAppAttempt().getAppAttemptId();
      int fairShare = fsinfo.getAppFairShare(attemptId);

      tbody.
        tr().
          td().
            br().$title(appInfo.getAppIdNum())._(). // for sorting
            a(url("app", appInfo.getAppId()), appInfo.getAppId())._().
          td(appInfo.getUser()).
          td(appInfo.getName()).
          td(appInfo.getQueue()).
          td("" + fairShare).
          td().
            br().$title(String.valueOf(appInfo.getStartTime()))._().
            _(startTime)._().
          td().
            br().$title(String.valueOf(appInfo.getFinishTime()))._().
            _(finishTime)._().
          td(appInfo.getState()).
          td(appInfo.getFinalStatus()).
          td().
            br().$title(percent)._(). // for sorting
            div(_PROGRESSBAR).
              $title(join(percent, '%')). // tooltip
              div(_PROGRESSBAR_VALUE).
                $style(join("width:", percent, '%'))._()._()._().
          td().
            a(!appInfo.isTrackingUrlReady()?
              "#" : appInfo.getTrackingUrlPretty(), appInfo.getTrackingUI())._()._();
      if (list.rendering != Render.HTML && ++i >= 20) break;
    }
    tbody._()._();

    if (list.rendering == Render.JS_ARRAY) {
      echo("<script type='text/javascript'>\n",
           "var appsData=");
      list.toDataTableArrays(reqAppStates, writer());
      echo("\n</script>\n");
    }
  }
}

import { get, isArray } from "lodash";
import { currentUser, clientConfig } from "@/services/auth";

/* eslint-disable class-methods-use-this */

export default class DefaultPolicy {
  refresh() {
    return Promise.resolve(this);
  }

  canCreateDataSource() {
    return currentUser.isAdmin;
  }

  isCreateDataSourceEnabled() {
    return currentUser.isAdmin;
  }

  canCreateDestination() {
    return currentUser.isAdmin;
  }

  isCreateDestinationEnabled() {
    return currentUser.isAdmin;
  }

  canCreateDashboard() {
    return currentUser.hasPermission("create_dashboard");
  }

  isCreateDashboardEnabled() {
    return currentUser.hasPermission("create_dashboard");
  }

  canCreateAlert() {
    return true;
  }

  canCreateUser() {
    return currentUser.isAdmin;
  }

  isCreateUserEnabled() {
    return currentUser.isAdmin;
  }

  isCreateQuerySnippetEnabled() {
    return true;
  }

  getDashboardRefreshIntervals() {
    const result = clientConfig.dashboardRefreshIntervals;
    return isArray(result) ? result : null;
  }

  getQueryRefreshIntervals() {
    const result = clientConfig.queryRefreshIntervals;
    return isArray(result) ? result : null;
  }

  canEdit(object) {
    return get(object, "can_edit", false);
  }

  canRefresh(object) {
    return get(object, "can_refresh", true)
  }

  getDashboardRestrictedRefreshAlertMessage(object) {
    return get(object, "dashboard_refresh_restricted_alert_message", "")
  }

  canRun() {
    return true;
  }

  isDestinationSyncEnabled(object) {
    return get(object, "is_destination_sync_enabled", false)
  }
}

import {OnTimerJobProgressDTO} from './job/JobProgressDTO';
import {NotificationDTO} from './NotificationDTO';

export enum SSEEventType {
  jobProgress = 'job-progress',
  notification = 'notification',
  heartbeat = 'heartbeat',
}

export interface SSEEnvelope<T = unknown> {
  type: SSEEventType;
  payload: T;
}

export interface SSEJobProgressPayload {
  progresses: Record<string, OnTimerJobProgressDTO>;
}

export interface SSENotificationPayload {
  notifications: NotificationDTO[];
}

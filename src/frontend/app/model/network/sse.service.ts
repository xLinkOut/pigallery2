import {Injectable, OnDestroy} from '@angular/core';
import {Observable, Subject, Subscription} from 'rxjs';
import {filter} from 'rxjs/operators';
import {AuthenticationService} from './authentication.service';
import {UserRoles} from '../../../../common/entities/UserDTO';
import {SSEEnvelope, SSEEventType, SSEJobProgressPayload, SSENotificationPayload} from '../../../../common/entities/SSEEventDTO';
import {Config} from '../../../../common/config/public/Config';
import {Utils} from '../../../../common/Utils';

@Injectable({providedIn: 'root'})
export class SseService implements OnDestroy {
  private eventSource: EventSource | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectDelay = 1_000;
  private readonly maxReconnectDelay = 30_000;
  private readonly events$ = new Subject<SSEEnvelope>();
  private authSub: Subscription;

  readonly events: Observable<SSEEnvelope> = this.events$.asObservable();
  readonly jobProgress$: Observable<SSEJobProgressPayload> = this.events.pipe(
    filter(e => e.type === SSEEventType.jobProgress)
  ) as Observable<SSEJobProgressPayload>;
  readonly notification$: Observable<SSENotificationPayload> = this.events.pipe(
    filter(e => e.type === SSEEventType.notification)
  ) as Observable<SSENotificationPayload>;

  constructor(private authService: AuthenticationService) {
    this.authSub = this.authService.user.subscribe(user => {
      if (user && user.role >= UserRoles.Admin) {
        this.connect();
      } else {
        this.disconnect();
      }
    });
  }

  ngOnDestroy(): void {
    this.disconnect();
    this.authSub.unsubscribe();
  }

  private get sseUrl(): string {
    return Utils.concatUrls(
      Config.Server.urlBase,
      Config.Server.apiPath,
      '/admin/jobs/scheduled/progress/stream'
    );
  }

  private connect(): void {
    if (this.eventSource !== null) {
      return;
    }
    if (!('EventSource' in window)) {
      return;
    }

    this.eventSource = new EventSource(this.sseUrl, {withCredentials: true});
    this.reconnectDelay = 1_000;

    this.eventSource.onmessage = (event: MessageEvent) => {
      try {
        const envelope: SSEEnvelope = JSON.parse(event.data as string);
        if (envelope.type !== SSEEventType.heartbeat) {
          this.events$.next(envelope);
        }
      } catch (e) {
        console.error('[SseService] parse error', e);
      }
    };

    this.eventSource.onerror = () => {
      this.cleanupEventSource();
      if (this.authService.isAuthorized(UserRoles.Admin)) {
        this.scheduleReconnect();
      }
    };
  }

  private cleanupEventSource(): void {
    if (this.eventSource) {
      this.eventSource.close();
      this.eventSource = null;
    }
  }

  private disconnect(): void {
    if (this.reconnectTimer !== null) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.cleanupEventSource();
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer !== null) {
      return;
    }
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, this.reconnectDelay);
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, this.maxReconnectDelay);
  }
}

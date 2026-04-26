import {Response} from 'express';
import * as crypto from 'crypto';
import {UserRoles} from '../../common/entities/UserDTO';
import {SSEEnvelope, SSEEventType} from '../../common/entities/SSEEventDTO';
import {Logger} from '../Logger';

const LOG_TAG = '[SSEManager]';
const HEARTBEAT_INTERVAL_MS = 20_000;

interface SSEClient {
  id: string;
  res: Response;
  role: UserRoles;
}

export class SSEManager {
  private static readonly clients = new Map<string, SSEClient>();
  private static heartbeatTimer: NodeJS.Timeout | null = null;

  static addClient(res: Response, role: UserRoles): string {
    const id = crypto.randomUUID();
    SSEManager.clients.set(id, {id, res, role});
    res.on('close', () => SSEManager.removeClient(id));
    Logger.debug(LOG_TAG, `client connected: ${id}, total: ${SSEManager.clients.size}`);
    SSEManager.ensureHeartbeat();
    return id;
  }

  static removeClient(id: string): void {
    SSEManager.clients.delete(id);
    Logger.debug(LOG_TAG, `client removed: ${id}, total: ${SSEManager.clients.size}`);
    if (SSEManager.clients.size === 0) {
      SSEManager.stopHeartbeat();
    }
  }

  static broadcast<T>(envelope: SSEEnvelope<T>, minRole: UserRoles = UserRoles.Guest): void {
    const data = `data: ${JSON.stringify(envelope)}\n\n`;
    for (const client of SSEManager.clients.values()) {
      if (client.role >= minRole) {
        try {
          client.res.write(data);
        } catch (e) {
          Logger.warn(LOG_TAG, `write failed for client ${client.id}, removing`);
          SSEManager.removeClient(client.id);
        }
      }
    }
  }

  private static ensureHeartbeat(): void {
    if (SSEManager.heartbeatTimer !== null) {
      return;
    }
    SSEManager.heartbeatTimer = setInterval(() => {
      SSEManager.broadcast({type: SSEEventType.heartbeat, payload: null});
    }, HEARTBEAT_INTERVAL_MS);
  }

  private static stopHeartbeat(): void {
    if (SSEManager.heartbeatTimer !== null) {
      clearInterval(SSEManager.heartbeatTimer);
      SSEManager.heartbeatTimer = null;
    }
  }
}

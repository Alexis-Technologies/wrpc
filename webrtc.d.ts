import type { Context, RouterDefinition, ConnectionHook } from './rpc.js';

/**
 * `@alexify/wrpc/webrtc` — peer-to-peer wrpc over WebRTC data channels.
 *
 * Two browsers (or a browser and a Node process with an injected RTC
 * implementation) each serve a router and call the other's, over ONE
 * RTCPeerConnection carrying two negotiated data channels — one per
 * client→host direction — so the ordinary WrpcClient and dispatcher speak
 * across the link unchanged: calls, events, ask/respond, subscriptions with
 * resume, binary streams, heartbeat and reconnect.
 *
 * ```js
 * // the signaling server (any wrpc server)
 * const router = defineRouter({ ...createSignalingUnit() }, { hooks: createSignalingHooks() });
 *
 * // a peer (browser or Node)
 * const client = await connect('wss://host/api');
 * const peer = new WrpcPeer({ router: myRouter, signaler: wrpcSignaler(client) });
 * const mesh = peer.join('lobby');
 * mesh.on('join', ({ id }) => mesh.link(id).load('chat'));
 * ```
 *
 * In a browser bundle this subpath resolves to the peer half alone
 * (webrtc.browser.d.ts); the server-side signaling unit below is Node-only.
 */

export * from './webrtc.browser.js';

export interface SignalingUnitOptions {
  /** The unit name (default 'signaling'); the client helper must agree. */
  name?: string;
  /** Applied to every method and the signal event. Default 'session'. */
  access?: 'session' | 'public';
  /**
   * Runs before a join and before every relayed signal. Return false to
   * refuse with 403, or throw a coded error of your own.
   */
  authorize?:
    | ((
        context: Context,
        info:
          | { action: 'join'; room: string; data: unknown }
          | { action: 'signal'; room: string | null; to: string; message: unknown },
      ) => boolean | void | Promise<boolean | void>)
    | null;
  /** 'room' (default): a signal reaches `to` only while both share the room; 'any': any connected id. */
  relay?: 'room' | 'any';
  /** The room-registry namespace signaling rooms live under. Default 'rtc:'. */
  prefix?: string;
}

/**
 * The built-in signaling unit as a router definition fragment — spread it
 * into `defineRouter`: `whoami`, `join`, `leave`, `members`, and the
 * inbound `signal` event relayed through `RpcServer.sendTo` (so it clusters
 * with no extra state). A peer's id is its signaling client id.
 */
export declare function createSignalingUnit(options?: SignalingUnitOptions): RouterDefinition;

/**
 * The router-level `onDisconnect` hook that announces a dropped signaling
 * connection's leave to the rooms it was in. `name`/`prefix` must match the
 * unit's.
 */
export declare function createSignalingHooks(options?: { name?: string; prefix?: string }): {
  onDisconnect: ConnectionHook;
};

'use strict';

// Shared by the server half and the (browser-bundled) client half; nothing
// here may import anything.

// The header a POST (or a re-attaching GET) names its channel with.
const CHANNEL_HEADER = 'x-wrpc-channel';

// What the header carries: `<id>.<secret>` — the channel id the server
// minted (whatever the application's generator makes of it: a uuid, a
// cuid, a counter) and the secret it drew for the channel, both handed out
// in the `ready` frame. Split at the LAST dot: the secret is base64url and
// holds none, the id may hold anything. A value without a dot is an id
// alone — what a client that predates the secret sends — and presents no
// secret. One header rather than two, so a cross-origin deployment's
// `Access-Control-Allow-Headers` stays what it was.
const CHANNEL_SEPARATOR = '.';

const joinChannelRef = (id, secret) => (secret ? `${id}${CHANNEL_SEPARATOR}${secret}` : id);

const splitChannelRef = (value) => {
  if (typeof value !== 'string' || value.length === 0) return null;
  const dot = value.lastIndexOf(CHANNEL_SEPARATOR);
  if (dot === -1) return { id: value, secret: '' };
  return { id: value.slice(0, dot), secret: value.slice(dot + 1) };
};

module.exports = { CHANNEL_HEADER, joinChannelRef, splitChannelRef };

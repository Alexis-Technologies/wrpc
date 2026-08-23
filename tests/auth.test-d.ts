import { expectAssignable, expectError, expectType } from 'tsd';
import * as auth from '../auth.js';
import type { TokenStore } from '../auth.js';
import type { WrpcClientOptions } from '../index.js';
import { connect } from '../index.js';

// A Map IS the store contract — the reason memoryStore is nearly free.
expectAssignable<TokenStore>(new Map<string, unknown>());
expectType<Map<string, unknown>>(auth.memoryStore());
expectAssignable<TokenStore>(auth.memoryStore());
expectError<TokenStore>({ get: () => null }); // set/delete are required

// webStorage takes anything localStorage-shaped, structurally
declare const storage: {
  getItem(k: string): string | null;
  setItem(k: string, v: string): void;
  removeItem(k: string): void;
};
expectAssignable<TokenStore>(auth.webStorage(storage));
expectAssignable<TokenStore>(auth.webStorage(storage, { prefix: 'app:' }));
expectAssignable<TokenStore>(auth.cookieStorage({ cookie: '' }));

// bearerAuth composes straight into the client options
const options = auth.bearerAuth({
  store: auth.memoryStore(),
  signIn: async (client) => (await client.call('auth/signIn')) as { access: string },
  refresh: async (client, tokens) => client.call('auth/refresh', { tokens }),
  on: [401, 403],
});
expectAssignable<WrpcClientOptions>(options);
void connect('ws://host', options);

// The server halves are TokenTransport-shaped
expectType<string | null>(auth.bearerTransport().read({ headers: { authorization: 'Bearer x' } }));
expectType<null>(auth.bearerTransport({ scheme: 'Token' }).write('x'));
expectType<string | null>(auth.payloadTransport({ field: 'token' }).read({ url: '/api?wrpc_meta=%7B%7D' }));
expectType<false>(auth.payloadTransport().ambient);

import { expectType } from 'tsd';
import wrpc from '../index.js';

expectType<Record<string, unknown>>(wrpc);

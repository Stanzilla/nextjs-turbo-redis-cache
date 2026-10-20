import RedisStringsHandler, {
  CreateRedisStringsHandlerOptions,
} from './RedisStringsHandler';
import { debugVerbose } from './utils/debug';
import { resolveKeyPrefix } from './utils/prefix';

let cachedHandler: RedisStringsHandler | undefined;

type NextCacheHandlerOptions = CreateRedisStringsHandlerOptions & {
  serverDistDir?: string;
};

export default class CachedHandler {
  private handler: RedisStringsHandler;

  constructor(options: NextCacheHandlerOptions) {
    if (!cachedHandler) {
      console.log('created cached handler');
      const keyPrefix = resolveKeyPrefix({
        optionKeyPrefix: options.keyPrefix,
        serverDistDir: options.serverDistDir,
        env: process.env,
      });

      cachedHandler = new RedisStringsHandler({
        ...options,
        keyPrefix,
      });
    }
    this.handler = cachedHandler;
  }
  get(
    ...args: Parameters<RedisStringsHandler['get']>
  ): ReturnType<RedisStringsHandler['get']> {
    debugVerbose('CachedHandler.get called with', args);
    return this.handler.get(...args);
  }
  set(
    ...args: Parameters<RedisStringsHandler['set']>
  ): ReturnType<RedisStringsHandler['set']> {
    debugVerbose('CachedHandler.set called with', args);
    return this.handler.set(...args);
  }
  revalidateTag(
    ...args: Parameters<RedisStringsHandler['revalidateTag']>
  ): ReturnType<RedisStringsHandler['revalidateTag']> {
    debugVerbose('CachedHandler.revalidateTag called with', args);
    return this.handler.revalidateTag(...args);
  }
  resetRequestCache(
    ...args: Parameters<RedisStringsHandler['resetRequestCache']>
  ): ReturnType<RedisStringsHandler['resetRequestCache']> {
    // debug("CachedHandler.resetRequestCache called with", args);
    return this.handler.resetRequestCache(...args);
  }
  /**
   * Closes the shared handler. The next CachedHandler creates a new one.
   */
  close(): Promise<void> {
    if (cachedHandler === this.handler) cachedHandler = undefined;
    return this.handler.close();
  }
}

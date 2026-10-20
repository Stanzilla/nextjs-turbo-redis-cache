import type { MapIndex } from '../SyncedMap';

/** Tag to cache keys: the reverse of the shared tags map. */
export class TagIndex implements MapIndex<string[]> {
  private keysByTag = new Map<string, Set<string>>();

  set(key: string, tags: string[], previous: string[] | undefined) {
    if (previous) this.delete(key, previous);
    for (const tag of tags) {
      let keys = this.keysByTag.get(tag);
      if (!keys) this.keysByTag.set(tag, (keys = new Set()));
      keys.add(key);
    }
  }

  delete(key: string, tags: string[]) {
    for (const tag of tags) {
      const keys = this.keysByTag.get(tag);
      if (!keys) continue;
      keys.delete(key);
      if (keys.size === 0) this.keysByTag.delete(tag);
    }
  }

  /** The keys that have at least one of the tags. */
  keysWithAnyTag(tags: Iterable<string>): Set<string> {
    const found = new Set<string>();
    for (const tag of tags) {
      for (const key of this.keysByTag.get(tag) ?? []) found.add(key);
    }
    return found;
  }
}

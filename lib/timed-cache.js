class TimedLruCache {
  constructor(options = {}) {
    this.ttlMs = Math.max(0, Number(options.ttlMs || 0) || 0);
    this.maxEntries = Math.max(1, Number(options.maxEntries || 1000) || 1000);
    this.cloneValue = typeof options.cloneValue === "function" ? options.cloneValue : (value) => value;
    this.store = new Map();
  }

  get(key) {
    const entry = this.store.get(key);
    if (!entry) {
      return undefined;
    }

    if (entry.expiresAt <= Date.now()) {
      this.store.delete(key);
      return undefined;
    }

    this.store.delete(key);
    this.store.set(key, entry);
    return this.cloneValue(entry.value);
  }

  set(key, value, ttlMs = this.ttlMs) {
    const resolvedTtl = Math.max(0, Number(ttlMs || 0) || 0);
    if (resolvedTtl <= 0) {
      return this;
    }

    if (this.store.has(key)) {
      this.store.delete(key);
    }

    this.store.set(key, {
      value: this.cloneValue(value),
      expiresAt: Date.now() + resolvedTtl
    });
    this.prune();
    return this;
  }

  delete(key) {
    return this.store.delete(key);
  }

  clear() {
    this.store.clear();
  }

  has(key) {
    return this.get(key) !== undefined;
  }

  prune() {
    const now = Date.now();
    for (const [key, entry] of this.store.entries()) {
      if (entry.expiresAt <= now) {
        this.store.delete(key);
      }
    }

    while (this.store.size > this.maxEntries) {
      const oldestKey = this.store.keys().next().value;
      this.store.delete(oldestKey);
    }
  }

  get size() {
    this.prune();
    return this.store.size;
  }
}

function createTimedLruCache(options) {
  return new TimedLruCache(options);
}

module.exports = {
  TimedLruCache,
  createTimedLruCache
};

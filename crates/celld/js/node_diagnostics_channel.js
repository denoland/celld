// node:diagnostics_channel for Cells. Workerd stores channels and subscribers
// in native isolate state, but the public behavior needs no host operation.
// Keep the registry in this lazy module so each isolate owns its channels and
// a Worker that never imports the module pays no setup cost.
(() => {
  const channels = new Map();

  const validateName = (name) => {
    if (typeof name !== "string" && typeof name !== "symbol") {
      throw new TypeError('The "name" argument must be a string or symbol');
    }
  };

  const validateCallback = (callback) => {
    if (typeof callback !== "function") {
      throw new TypeError('The "callback" argument must be a function');
    }
  };

  class Channel {
    #name;
    #subscribers = new Set();
    #stores = new Map();

    constructor(name) {
      validateName(name);
      this.#name = name;
    }

    get name() {
      return this.#name;
    }

    // Cells follows Node and current Workerd compatibility dates, where this
    // is a getter. Undici relies on the boolean property to skip publication.
    get hasSubscribers() {
      return this.#subscribers.size !== 0;
    }

    publish(message) {
      // A snapshot prevents a subscription added during publication from
      // observing the event that caused its registration.
      for (const callback of [...this.#subscribers]) {
        Reflect.apply(callback, undefined, [message, this.#name]);
      }
    }

    subscribe(callback) {
      validateCallback(callback);
      this.#subscribers.add(callback);
    }

    unsubscribe(callback) {
      validateCallback(callback);
      this.#subscribers.delete(callback);
    }

    bindStore(store, transform = (message) => message) {
      if (store === null || typeof store !== "object" || typeof store.run !== "function") {
        throw new TypeError('The "store" argument must be an AsyncLocalStorage');
      }
      validateCallback(transform);
      this.#stores.set(store, transform);
    }

    unbindStore(store) {
      this.#stores.delete(store);
    }

    runStores(message, callback, thisArg = globalThis, ...args) {
      validateCallback(callback);
      const stores = [...this.#stores];

      const run = (index) => {
        if (index === stores.length) {
          this.publish(message);
          return Reflect.apply(callback, thisArg, args);
        }
        const [store, transform] = stores[index];
        return store.run(transform(message), () => run(index + 1));
      };

      return run(0);
    }
  }

  const channel = (name) => {
    validateName(name);
    let value = channels.get(name);
    if (value === undefined) {
      value = new Channel(name);
      channels.set(name, value);
    }
    return value;
  };

  const hasSubscribers = (name) => {
    validateName(name);
    return channels.get(name)?.hasSubscribers ?? false;
  };
  const subscribe = (name, callback) => channel(name).subscribe(callback);
  const unsubscribe = (name, callback) => {
    validateName(name);
    validateCallback(callback);
    channels.get(name)?.unsubscribe(callback);
  };

  const tracingNames = ["start", "end", "asyncStart", "asyncEnd", "error"];
  const tracingToken = Symbol();

  class TracingChannel {
    constructor(token, nameOrChannels) {
      if (token !== tracingToken) {
        throw new Error("Use diagnostics_channel.tracingChannel() to create TracingChannel");
      }
      if (typeof nameOrChannels === "string") {
        for (const name of tracingNames) {
          this[name] = channel(`tracing:${nameOrChannels}:${name}`);
        }
      } else {
        if (nameOrChannels === null || typeof nameOrChannels !== "object") {
          throw new TypeError('The "channels" argument must be an object');
        }
        for (const name of tracingNames) {
          const value = nameOrChannels[name];
          if (!(value instanceof Channel)) {
            throw new TypeError(`The "channels.${name}" argument must be a Channel`);
          }
          this[name] = value;
        }
      }
    }

    get hasSubscribers() {
      return tracingNames.some((name) => this[name].hasSubscribers);
    }

    subscribe(subscriptions) {
      for (const name of tracingNames) {
        if (subscriptions[name] !== undefined) this[name].subscribe(subscriptions[name]);
      }
    }

    unsubscribe(subscriptions) {
      for (const name of tracingNames) {
        if (subscriptions[name] !== undefined) this[name].unsubscribe(subscriptions[name]);
      }
    }

    traceSync(fn, context = {}, thisArg = globalThis, ...args) {
      return this.start.runStores(context, () => {
        try {
          const result = Reflect.apply(fn, thisArg, args);
          context.result = result;
          return result;
        } catch (error) {
          context.error = error;
          this.error.publish(context);
          throw error;
        } finally {
          this.end.publish(context);
        }
      }, thisArg);
    }

    tracePromise(fn, context = {}, thisArg = globalThis, ...args) {
      return this.start.runStores(context, () => {
        let promise;
        try {
          promise = Promise.resolve(Reflect.apply(fn, thisArg, args));
        } catch (error) {
          context.error = error;
          this.error.publish(context);
          throw error;
        } finally {
          this.end.publish(context);
        }
        return promise.then(
          (result) => {
            context.result = result;
            this.asyncStart.publish(context);
            this.asyncEnd.publish(context);
            return result;
          },
          (error) => {
            context.error = error;
            this.error.publish(context);
            this.asyncStart.publish(context);
            this.asyncEnd.publish(context);
            throw error;
          },
        );
      }, thisArg);
    }

    traceCallback(fn, position = -1, context = {}, thisArg = globalThis, ...args) {
      const callback = args.at(position);
      validateCallback(callback);
      const tracing = this;
      function wrapped(error, result, ...callbackArgs) {
        if (error) {
          context.error = error;
          tracing.error.publish(context);
        } else {
          context.result = result;
        }
        return tracing.asyncStart.runStores(context, () => {
          try {
            return Reflect.apply(callback, this, [error, result, ...callbackArgs]);
          } finally {
            tracing.asyncEnd.publish(context);
          }
        }, thisArg);
      }
      args.splice(position, 1, wrapped);
      return this.start.runStores(context, () => {
        try {
          return Reflect.apply(fn, thisArg, args);
        } catch (error) {
          context.error = error;
          this.error.publish(context);
          throw error;
        } finally {
          this.end.publish(context);
        }
      }, thisArg);
    }
  }

  const tracingChannel = (nameOrChannels) =>
    new TracingChannel(tracingToken, nameOrChannels);

  globalThis.__diagnosticsChannelModule = {
    Channel,
    TracingChannel,
    channel,
    hasSubscribers,
    subscribe,
    unsubscribe,
    tracingChannel,
  };
})();

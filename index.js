"use strict";
var EventEmitter = require('events');
var noop = function () {
};

function Sifaka(backend, options) {
    EventEmitter.call(this);
    var self = this;

    this.options = options || {};
    this.backend = backend;
    this.storage = {};
    this.debugLogger = (options.debug === true ? console.log : options.debug) || null;
    this.initialLockCheckDelayMs = typeof options.initialLockCheckDelayMs !== "undefined" ? options.initialLockCheckDelayMs : 20; // Wait this long before performing the first lock check
    this.lockCheckIntervalMs = typeof options.lockCheckIntervalMs !== "undefined" ? options.lockCheckIntervalMs : 50; // After the first check, wait another (lockCheckIntervalMs + n* lockCheckBackoff)
    this.lockCheckBackoff = typeof options.lockCheckBackoffMs !== "undefined" ? options.lockCheckBackoffMs : 100;
    this.lockCheckBackoffExponent = typeof options.lockCheckBackoffExponent !== "undefined" ? options.lockCheckBackoffExponent : 1; // Allow for an exponential backoff
    this.cachePolicy = options.cachePolicy || new (require("./cache_policies/static"))();
    this.statsInterval = options.statsInterval || 0;
    this.serializer = options.serializer || null;
    this.stats = {hit: 0, miss: 0, work: 0};
    this.name = options.name || null;
    this.lockTimeoutMs = options.lockTimeoutMs || 120 * 1000;
    this.pendingTimeoutMs = options.pendingTimeoutMs || this.lockTimeoutMs;

    if(this.statsInterval) {
        this.lastStats = new Date();
        setTimeout(function () {
            self._logStats();
        }, this.statsInterval);
    }

    if(this.debugLogger) {
        this.debug("", "---- Initialising Cache ----")
    }

    // Provide an otel tracer instance and its trace API to enable
    // tracing in sifaka.
    // https://open-telemetry.github.io/opentelemetry-js/interfaces/_opentelemetry_api.Tracer.html
    // https://open-telemetry.github.io/opentelemetry-js/classes/_opentelemetry_api.TraceAPI.html
    this.tracer = options.tracer || null;
    this.traceApi = options.traceApi || null;
    this.namespace = options.namespace || null;
    this.pendingCallbacks = {};
    this.pendingTimeouts = {};
    this.remoteLockChecks = {};
    this.localLocks = {};
}

require("util").inherits(Sifaka, EventEmitter);

Sifaka.prototype.debug = function (key, message) {
    this.traceEvent(message);

    if(this.namespace) {
        message = this.namespace + ":" + key + "\t" + message;
    } else {
        message = key + "\t" + message;
    }

    if(this.name) {
        message = "[" + this.name + "]\t" + message;
    }

    if(this.debugLogger) {
        this.debugLogger(new Date() * 1 + "\t" + message);
    }
};

Sifaka.prototype.toString = function () {
    var message = "Sifaka";
    if(this.name) {
        message += " [" + this.name + "]\n";
    }

    message += require("util").inspect(this.stats);
    return message;
};

Sifaka.prototype.exists = function (key, options, callback) {
    if(arguments.length === 2 && typeof arguments[1] === "function") {
        callback = options;
        options = {};
    }
    this.backend.exists(key, options, callback);
};

/**
 * Instrument a function with otel. This starts a trace span immediately which
 * ends when the returned callback is invoked.
 *
 * If fn is provided, we wrap that function with the span end logic; the provided callback
 * will be called as normal in the returned function. If no function is provided, the
 * returned function will simply close the span.
 *
 * attrs can be provided to add additional attributes to the resulting span.
 *
 * trace support automatic nesting of spans such that is an already-traced function
 * is provided as fn, the returned function will end a span belonging to that
 * parent.
 */
Sifaka.prototype.trace = function(name, fn, attrs) {
    if (!this.tracer) {
        return fn;
    }

    let options = attrs ? {attributes: attrs} : {};

    var ctx;
    let parent = (typeof fn === "function") ? fn?.parent : fn;
    if (this.traceApi && parent) {
        ctx = this.traceApi.trace.setSpan(
            this.traceApi.context.active(),
            parent
        );
    }

    const span = this.tracer.startSpan(`sifaka:${name}`, options, ctx)

    let ret = function () {
        span.end();
        if (typeof fn === "function") {
            fn.apply(this, arguments);
        }
    }
    ret.parent = span;
    return ret;
}

/**
 * Registers a single event to the current trace.
 *
 * Useful for understanding lock/queue behaviour.
 */
Sifaka.prototype.traceEvent = function(name) {
    if (!this.tracer || !this.traceApi) {
        return;
    }

    let span = this.traceApi.trace.getActiveSpan();
    if (!span || span.ended()) {
        return;
    }

    span.addEvent(`sifaka:${name}`)
}


Sifaka.prototype.get = function (key, workFn, options, callback) {
    callback = this.trace("get()", callback)
    var self = this;

    if(arguments.length === 3 && typeof arguments[2] === "function") {
        callback = options;
        options = {};
    }
    if(options.metaOnly && ["hit", "miss"].indexOf(options.metaOnly) == -1) {
        throw new RangeError("options.metaOnly should be one of hit or miss");
    }

    self.debug(key, "GET");

    if(this._pendingQueueExists(key)) {
        // This node is already waiting for data on this key, add to the queue
        self.debug(key, "CACHE MISS - ADDING TO PENDING");
        self._addPending(key, self.trace("queued", callback, {"reason": "node already waiting"}), options);
    } else {
        // Otherwise, hit the backend
        self.backend.get(key, options, function (err, data, state, extra) {
            callback = self.trace("process-backend-get", callback);

            if(err) {
                if(typeof err === "string") {
                    err = new Error(err);
                }
                if(typeof err.cached === 'undefined') {
                    err.cached = true;
                }
            }

            if(state.hit === true) {
                self.stats.hit++;
                self.debug(key, "CACHE HIT");

                if(options.metaOnly === "hit") {
                    // Pass "data" through here - so that we can verify in the tests that the backends are returning
                    // undefined (and hopefully not fetching data from the store)
                    callback(err, data, state, extra);
                } else {
                    let traceDone = self.trace("deserialize", callback?.parent)
                    self._deserialize(data, extra, function (deserializeErr, deSerializedData, deSerializedExtra) {
                        traceDone();
                        callback(err || deserializeErr, deSerializedData, state, deSerializedExtra);
                    });
                }

                // check if we need to refresh the data in the background
                if(state.stale) {
                    self.stats.stale++;
                    if(!self._hasLocalLock(key)) {
                        let done = self.trace("stale: acquiring lock", callback?.parent)
                        self.backend.lock(key, null, function (err, acquired) {
                            done();
                            if(acquired) {
                                self._setLocalLock(key);
                                self.debug(key, "GOT LOCK FOR STALE REFRESH");
                                self._doWork(key, options, workFn, state);
                            } else {
                                self.debug(key, "LOCK DENIED FOR STALE REFRESH"); // Another worker is doing the refresh
                            }
                        });
                    } else {
                        self.debug(key, "STALE REFRESH SKIPPED - LOCK ALREADY HELD LOCALLY")
                    }
                }
            } else {
                self.stats.miss++;
                if(options.metaOnly === "miss") {
                    // We don't need to wait for a result to be calculated, or trigger one
                    self.debug(key, "META ONLY CACHE MISS");
                    return callback(err, void 0, state);
                }

                self._addPending(key, self.trace("queued", callback, {"reason": "cache miss"}), options);
                self.debug(key, "CACHE MISS");

                if(err && err.cacheUnavailable === true) { // Failed to get, so lets try to do the work once, locally
                    // If we're already checking for a remote lock, add to the list
                    if(self._hasLocalLock(key)) {
                        self.debug(key, "UNABLE TO READ CACHE - ADDED TO PENDING");
                    } else {
                        self.debug(key, "UNABLE TO READ CACHE - DOING WORK");
                        self._setLocalLock(key);
                        self._doWork(key, options, workFn, state);
                    }
                } else {
                    if(self._hasLocalLock(key)) {
                        // Do nothing, we're already on the pending list, and have the lock.
                        return;
                    } else {

                        if(self._hasRemoteLockCheck(key)) {
                            // We know there's already a remote lock out there, and we should already be watching it.
                            // We're already on the pending list, so no need to do anything else
                            return;
                        } else {
                            // We don't know about a remote lock, so we have to try obtain it ourselves.
                            let done = self.trace("cache miss: acquiring lock", callback?.parent)
                            self.backend.lock(key, null, function (err, acquired) {
                                done();
                                if(acquired === true) {
                                    // We got the lock ourselves, so we can do the work
                                    self._setLocalLock(key);
                                    self.debug(key, "GOT LOCK");
                                    self._doWork(key, options, workFn, state);
                                } else {
                                    if(self._hasLocalLock(key)) { // Something local got the lock in the meantime, so no further action required
                                        return;
                                    }else {
                                        self.debug(key, "WAITING FOR REMOTE LOCK / WORK");
                                        self._addRemoteLockCheck(key, options, workFn, state); // We need to poll for a result / lock expiry
                                    }
                                }
                            });
                        }
                    }
                }
            }
        });
    }
};

Sifaka.prototype._addPending = function (key, callback, options) {
    var self = this;
    var pendingID = Math.floor((Math.random() * 1E12)).toString(36);
    this.debug(key, "PENDING ADDED: " + pendingID);
    this.pendingCallbacks[key] = this.pendingCallbacks[key] || [];
    if(!this.pendingTimeouts[key]) {
        this.pendingTimeouts[key] = setTimeout(function () {
            self._pendingTimedOut(key);
        }, self.pendingTimeoutMs)
    }
    this.pendingCallbacks[key].push({options: options, cb: callback, id: pendingID, dt: new Date()});
};

/**
 * Clear any pending requests if we hit the timeout
 * @param key
 * @private
 */
Sifaka.prototype._pendingTimedOut = function (key) {
    if(this.pendingTimeouts[key]) {
        var timeout = this.pendingTimeouts[key];
        clearTimeout(timeout);
        delete this.pendingTimeouts[key];
        this.debug(key, "PENDING TIMED OUT");
        this._resolvePendingCallbacks(key, new Error("Timed Out"), null, {}, false, {});
    }
};

Sifaka.prototype._pendingQueueExists = function (key) {
    if(this.pendingCallbacks[key] && this.pendingCallbacks[key].length) {
        return true;
    }
    return false;
};

Sifaka.prototype._calculateCacheTimes = function (key, duration, data, extra, state, options, callback) {
    options = options || {};
    extra = extra || {};
    (options.policy || this.cachePolicy).calculate(key, duration, data, extra, state, callback);
};
Sifaka.prototype._checkForBackendResult = function (key) {
    var self = this;

    this.backend.get(key, {noLock: true}, function (err, data, state, extra) {
        if(state.hit) {
            
            if(self._hasRemoteLockCheck(key)) {
                self._removeRemoteLockCheck(key);
            }
            
            if(err) {
                if(typeof err === "string") {
                    err = new Error(err);
                }
                err.cached = true;
            }
            self.debug(key, "RESULT CHECK: HIT");
            self._deserialize(data, extra, function (err, deSerializedData, deSerializedExtra) {
                self._resolvePendingCallbacks(key, err, deSerializedData, deSerializedExtra, false, state);
            });
        } else {
            if(state.locked == false) {
                // Miss, and the lock has gone (e.g. client died whilst recalculating, leaving hanging lock, which then timed out)
                return self.backend.lock(key, null, function (err, acquired) {
                    if(acquired) {
                        self._setLocalLock(key);
                        self.debug(key, "GOT LOCK AFTER REMOTE LOCK");
                        
                        if(self.remoteLockChecks[key] && self.remoteLockChecks[key].workFn) {
                            return self._doWork(key, self.remoteLockChecks[key].options, self.remoteLockChecks[key].workFn, self.remoteLockChecks[key].state, function () {
                                if(self._hasRemoteLockCheck(key)) {
                                    self._removeRemoteLockCheck(key, "check backend result (not locked - lock acquired)");
                                }
                            });
                        }else{
                            // We were expecting the lock check to still be there. Throw with details of the last fn to remove it.
                                self.debug(key, "ERROR - Attempted to set next lock check on a removed remote lock check. NS: "+self.namespace);
                                return;
                        }
                    } else {
                        self.debug(key, "LOCK NOT ACQUIRED AFTER REMOTE LOCK CHECK - WAITING AGAIN");
                    }
                });
            } else if(state.ownLock) {
                // Now we have the lock locally, we do not need to check remotely.
                if(self._hasRemoteLockCheck(key)) {
                    self._removeRemoteLockCheck(key);
                }
                return;
            } else {
                // Otherwise schedule another check, backing off as necessary
                var nextInterval;
                
                
                if(!self.remoteLockChecks[key]){
                    // The backend check has been removed elsewhere. Throwing is unlikely to be productive, as it will be unlikely to be caught.
                    self.debug(key, "ERROR - Attempted to set next lock check on a removed remote lock check. NS: "+self.namespace);
                    return;
                }
                
                if(self.lockCheckBackoffExponent === 1) {
                    nextInterval = self.lockCheckIntervalMs + (self.remoteLockChecks[key].count * (self.lockCheckBackoff));
                } else {
                    var val = self.lockCheckBackoff * Math.pow(self.remoteLockChecks[key].count, self.lockCheckBackoffExponent);
                    nextInterval = self.lockCheckIntervalMs + val;
                }
                self.remoteLockChecks[key].count += 1;
                self.remoteLockChecks[key].timeout = setTimeout(function () {
                    self._checkForBackendResult(key)
                }, nextInterval);
                self.debug(key, "RESULT CHECK: MISS - CHECKING AGAIN IN " + nextInterval + "ms");
                return;
            }
        }
    });
};

Sifaka.prototype._serialize = function (data, extra, callback) {
    var serializer = this.options.serializer;
    if(serializer) {
        serializer.serialize(data, extra, {}, callback);
    } else {
        callback(null, data, extra);
    }
};
Sifaka.prototype._deserialize = function (data, extra, callback) {
    var serializer = this.options.serializer;
    if(serializer) {
        try {
            serializer.deserialize(data, extra, {}, callback);
        }catch(e){
            callback(e, null, null);
        }
    } else {
        callback(null, data, extra);
    }
};

Sifaka.prototype._doWork = function (key, options, workFunction, state, callback) {
    var self = this;
    self.debug(key, "TRIGGERING WORK");
    var start = new Date();
    workFunction(function (workError, data, extra, storedCallback) {
        if(arguments.length == 3 && typeof extra === "function") {
            storedCallback = extra;
            extra = null;
        }

        if(typeof extra == "undefined") {
            extra = null;
        }

        self.stats.work++;
        var duration = new Date() - start;
        if(workError && !workError.cache) {
            self.debug(key, "ERROR, NOT CACHED: " + workError + " - UNLOCKING");

            self.backend.unlock(key, {}, function () {
                self.debug(key, "UNLOCKED AFTER WORK THAT ERRORED");
                self._resolvePendingCallbacks(key, workError, data, extra, true, state);
            });
        } else {
            self._calculateCacheTimes(key, duration, data, extra, state, options, function (err, cachePolicyResult) {

                if(!cachePolicyResult.noCache) {

                    state.staleTime = cachePolicyResult.staleTimeAbs;
                    state.expiryTime = cachePolicyResult.expiryTimeAbs;

                    self._serialize(data, extra, function (err, serializedData, serializedExtra) {
                        if(err) {
                            // We've done the work, but cannot serialize it / save.
                            self.debug(key, "SERIALIZATION ERROR. NOT STORING: " + err);
                            return self.backend.unlock(key, {}, function () {
                                self.debug(key, "UNLOCKED AFTER SERIALIZATION ERROR");
                                self._resolvePendingCallbacks(key, err, data, extra, true, state);
                            });
                        } else {
                            self.debug(key, "STORING AND UNLOCKING....");
                            self.backend.store(key, serializedData, serializedExtra, workError, cachePolicyResult, {
                                unlock: true
                            }, function (storedError, storedResult) {
                                self._removeLocalLock(key);
                                self._resolvePendingCallbacks(key, workError, data, extra, true, state);
                                if(storedCallback) {
                                    storedCallback(storedError, storedResult);
                                }
                            });
                        }
                    });
                } else {
                    self.debug(key, "UNLOCKING - NOCACHE FROM POLICY");
                    self.backend.unlock(key, options, function () {
                        self._removeLocalLock(key);
                        self._resolvePendingCallbacks(key, workError, data, extra, true, state);
                    });
                }

                // Needed?
                if(callback) {
                    callback(null);
                }
            });
        }
    });
};
Sifaka.prototype._logStats = function () {
    var self = this;
    var currentStats = this.stats;
    var lastStats = this.lastStats;
    this.lastStats = new Date();
    setTimeout(function () {
        self._logStats()
    }, this.statsInterval);
    currentStats.duration = this.lastStats - lastStats;
    this.stats = {hit: 0, miss: 0, work: 0};
    this.emit("stats", currentStats);
};
Sifaka.prototype._resolvePendingCallbacks = function (key, err, data, extra, didWork, state) {
    var self = this;
    if(self._hasRemoteLockCheck(key)) {
        self._removeRemoteLockCheck(key);
    }
    
    // Remove the timeout on the pending list
    var timeout = this.pendingTimeouts[key];
    if(timeout) {
        clearTimeout(timeout);
        delete this.pendingTimeouts[key];
    }
    
    // Make sure the local lock is gone, even after an error
    self._removeLocalLock(key);

    this.debug(key, "RESOLVING PENDING CALLBACKS");
    var pendingCallbacks = this.pendingCallbacks[key] || [];
    this.pendingCallbacks[key] = [];

    state.pending = true;
    while(pendingCallbacks.length) {
        var pending = pendingCallbacks.shift();
        this.debug(key, "RESOLVING CALLBACK: " + pending.id + " (" + (new Date() - pending.dt) / 1000 + "s)");
        var cb = pending.cb;
        var options = pending.options;

        if(options.metaOnly && options.metaOnly == "miss") {
            cb(err, void 0, state, extra);
        } else {
            cb(err, data, state, extra);
        }
    }
    this.debug(key, "RESOLVED ALL CALLBACKS");

};

Sifaka.prototype._hasLocalLock = function (key) {
    if(this.localLocks[key] && this.localLocks[key]._called !== true) {
        return true;
    }
    return false;
}

Sifaka.prototype._setLocalLock = function (key) {
    var self = this;
    this.debug(key, "SETTING LOCAL LOCK");
    this.localLocks[key] = setTimeout(function () {
        delete self.localLocks[key];
    }, self.lockTimeoutMs);
}

Sifaka.prototype._removeLocalLock = function (key) {
    var self = this;
    this.debug(key, "REMOVING LOCAL LOCK");
    var lockTimeout = self.localLocks[key];
    delete self.localLocks[key];
    clearTimeout(lockTimeout);
}

/**
 * Is there anything waiting for a lock check to return?
 * @param key
 * @returns {boolean}
 * @private
 */
Sifaka.prototype._hasRemoteLockCheck = function (key) {
    if(this.remoteLockChecks[key]) {
        return true;
    }
    return false;
};

Sifaka.prototype._addRemoteLockCheck = function (key, options, workFn, state) {
    var self = this;
    if(!self._hasRemoteLockCheck(key)) {
        self.remoteLockChecks[key] = {options: options, workFn: workFn, state: state, count: 0};
        self.remoteLockChecks[key].timeout = setTimeout(function () {
            self._checkForBackendResult(key)
        }, self.options.initialLockCheckDelayMs);
        return;
    } else {
        return;
    }
};

Sifaka.prototype._removeRemoteLockCheck = function (key) {
    var self = this;
    if(self.remoteLockChecks[key]) {
        clearTimeout(self.remoteLockChecks[key].timeout);
        self.remoteLockChecks[key] = null;
        delete self.remoteLockChecks[key];
    }
}

module.exports = {
    Sifaka: Sifaka,
    backends: require("./backends"),
    cache_policies: require("./cache_policies"),
    serializers: require("./serializers")
};
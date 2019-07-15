"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const leveldown_1 = require("leveldown");
const GET_OPTIONS = { asBuffer: true };
class Leveldb {
    constructor(location, options) {
        this.options = options;
        this._db = new leveldown_1.default(location);
    }
    async open(options) {
        return new Promise((resolve, reject) => {
            const callback = (err) => {
                if (err == null) {
                    resolve();
                }
                else {
                    reject(err);
                }
            };
            if (options != null) {
                this._db.open(options, callback);
            }
            else {
                this._db.open(callback);
            }
        });
    }
    async close() {
        return new Promise((resolve, reject) => {
            this._db.close((err) => {
                if (err == null) {
                    resolve();
                }
                else {
                    reject(err);
                }
            });
        });
    }
    async get(key) {
        const ek = this.options.encodeKey(key);
        return new Promise((resolve, reject) => {
            this._db.get(ek, GET_OPTIONS, (err, data) => {
                if (err == null) {
                    const rdata = this.options.decodeValue(data);
                    resolve(rdata);
                }
                else {
                    reject(err);
                }
            });
        });
    }
    async put(key, value) {
        const ek = this.options.encodeKey(key);
        const buffer = this.options.encodeValue(value);
        return new Promise((resolve, reject) => {
            this._db.put(ek, buffer, function (err) {
                if (err == null) {
                    resolve();
                }
                else {
                    reject(err);
                }
            });
        });
    }
    iterator() {
        return new LeveldbIterator(this._db.iterator(), this.options);
    }
    static async destroy(location) {
        return new Promise((resolve, reject) => {
            leveldown_1.default.destroy(location, function (err) {
                if (err == null) {
                    resolve();
                }
                else {
                    reject(err);
                }
            });
        });
    }
}
exports.Leveldb = Leveldb;
class LeveldbIterator {
    constructor(iterator, options) {
        this.iterator = iterator;
        this.options = options;
    }
    seek(key) {
        const ek = this.options.encodeKey(key);
        this.iterator.seek(ek);
    }
    async next() {
        return new Promise((resolve, reject) => {
            this.iterator.next((err, keyData, valueData) => {
                if (err == null) {
                    if (keyData != null) {
                        const key = this.options.decodeKey(keyData);
                        const value = valueData != null ? this.options.decodeValue(valueData) : null;
                        resolve({ key, value });
                    }
                    else {
                        resolve(null);
                    }
                }
                else {
                    reject(err);
                }
            });
        });
    }
    async end() {
        return new Promise((resolve, reject) => {
            this.iterator.end(function (err) {
                if (err == null) {
                    resolve();
                }
                else {
                    reject(err);
                }
            });
        });
    }
}
exports.LeveldbIterator = LeveldbIterator;

import LevelDOWN, { Bytes, LevelDownOpenOptions, LevelDown, LevelDownIterator } from 'leveldown';


const GET_OPTIONS = { asBuffer: true };

export interface ILeveldbOptions<K, V> {
  encodeKey: (key: K) => Buffer;
  decodeKey: (buffer: Buffer) => K;
  encodeValue: (key: V) => Buffer;
  decodeValue: (buffer: Buffer) => V;
}



export class Leveldb<K, V> {
  private readonly _db: LevelDown;

  constructor(location: string, public options: ILeveldbOptions<K, V>) {
    this._db = new LevelDOWN(location);
  }

  async open(options?: LevelDownOpenOptions) {
    return new Promise((resolve, reject) => {
      const callback = (err: Error | undefined) => {
        if (err == null) { resolve(); } else { reject(err); }
      };
      if (options != null) {
        this._db.open(options, callback);
      } else {
        this._db.open(callback);
      }
    });
  }

  async close() {
    return new Promise((resolve, reject) => {
      this._db.close((err: Error | undefined) => {
        if (err == null) { resolve(); } else { reject(err); }
      });
    });
  }

  async get(key: K) {
    const ek = this.options.encodeKey(key);
    return new Promise<any>((resolve, reject) => {
      this._db.get(ek, GET_OPTIONS, (err: Error | undefined, data: Bytes) => {
        if (err == null) {
          const rdata = this.options.decodeValue(<Buffer>data);
          resolve(rdata);
        } else {
          reject(err);
        }
      });
    });
  }

  async put(key: K, value: V) {
    const ek = this.options.encodeKey(key);
    const buffer = <Buffer>this.options.encodeValue(value);
    return new Promise((resolve, reject) => {
      this._db.put(ek, buffer, function (err: Error | undefined) {
        if (err == null) {
          resolve();
        } else {
          reject(err);
        }
      });
    });
  }

  iterator() {
    return new LeveldbIterator<K, V>(this._db.iterator(), this.options);
  }

  static async destroy(location: string) {
    return new Promise((resolve, reject) => {
      (<any>LevelDOWN).destroy(location, function (err: Error | undefined) {
        if (err == null) {
          resolve();
        } else {
          reject(err);
        }
      });
    });
  }

}

export class LeveldbIterator<K, V> {
  constructor(public iterator: LevelDownIterator, public options: ILeveldbOptions<K, V>) {

  }

  seek(key: K) {
    const ek = this.options.encodeKey(key);
    this.iterator.seek(ek);
  }

  async next() {
    return new Promise<{ key: K, value: V | null } | null>((resolve, reject) => {
      this.iterator.next((err: Error | undefined, keyData: Bytes, valueData: Bytes) => {
        if (err == null) {
          if (keyData != null) {
            const key = this.options.decodeKey(<Buffer>keyData);
            const value = valueData != null ? this.options.decodeValue(<Buffer>valueData) : null;
            resolve({ key, value });
          } else {
            resolve(null);
          }
        } else {
          reject(err);
        }
      });
    });
  }

  async end() {
    return new Promise((resolve, reject) => {
      this.iterator.end(function (err: Error | undefined) {
        if (err == null) {
          resolve();
        } else {
          reject(err);
        }
      });
    });
  }
}

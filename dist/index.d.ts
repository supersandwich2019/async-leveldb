/// <reference types="node" />
import { LevelDownOpenOptions, LevelDownIterator } from 'leveldown';
export interface ILeveldbOptions<K, V> {
    encodeKey: (key: K) => Buffer;
    decodeKey: (buffer: Buffer) => K;
    encodeValue: (key: V) => Buffer;
    decodeValue: (buffer: Buffer) => V;
}
export declare class Leveldb<K, V> {
    options: ILeveldbOptions<K, V>;
    private readonly _db;
    constructor(location: string, options: ILeveldbOptions<K, V>);
    open(options?: LevelDownOpenOptions): Promise<{}>;
    close(): Promise<{}>;
    get(key: K): Promise<any>;
    put(key: K, value: V): Promise<{}>;
    iterator(): LeveldbIterator<K, V>;
    static destroy(location: string): Promise<{}>;
}
export declare class LeveldbIterator<K, V> {
    iterator: LevelDownIterator;
    options: ILeveldbOptions<K, V>;
    constructor(iterator: LevelDownIterator, options: ILeveldbOptions<K, V>);
    seek(key: K): void;
    next(): Promise<{
        key: K;
        value: V | null;
    } | null>;
    end(): Promise<{}>;
}

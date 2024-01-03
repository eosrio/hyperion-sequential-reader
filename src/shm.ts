import {BSON} from "bson";

export class SharedObject<T> {

    readonly raw: Uint8Array;
    readonly obj: T;

    constructor(mainBuf: Uint8Array, bounds: MemoryBounds) {
        this.raw = mainBuf.slice(bounds.index, bounds.size);
        this.obj = BSON.deserialize(mainBuf, {
            index: bounds.index,
            allowObjectSmallerThanBufferSize: true
        }) as T;
    }

    static fromObject<ST>(obj: ST): SharedObject<ST> {
        const rawObj = BSON.serialize(obj);
        return new SharedObject<ST>(rawObj, {index: 0, size: rawObj.length});
    }

    static fromMemoryArray<ST>(array: Uint8Array, bounds: MemoryBounds): SharedObject<ST> {
        return new SharedObject<ST>(array, bounds);
    }
}

export interface MemoryBounds {
    index: number;
    size: number;
}

export class SharedObjectStore<T> {
    readonly sharedMem: SharedArrayBuffer;
    readonly arrayView: Uint8Array;
    private objectMap: Map<string, MemoryBounds>;

    constructor(opts: {
        bufferLengthBytes?: number
        sharedMemRef?: SharedArrayBuffer
    }) {
        if ('bufferLengthBytes' in opts)
            this.sharedMem = new SharedArrayBuffer(opts.bufferLengthBytes);

        else if (opts.sharedMemRef)
            this.sharedMem = opts.sharedMemRef;

        else
            throw new Error(`Missing shared memory init params`);

        this.objectMap = new Map<string, MemoryBounds>();
        this.arrayView = new Uint8Array(this.sharedMem);
    }

    private loadUInt8Array(raw: Uint8Array, bounds: MemoryBounds) {
        for (const [subIndex, byte] of raw.entries())
            this.arrayView[bounds.index + subIndex] = byte;
    }

    static fromObjects<ST>(objects: {[key: string]: SharedObject<ST>}): SharedObjectStore<ST> {
        let totalSize = 0;
        for (const obj of Object.values(objects))
            totalSize += obj.raw.length;

        const store = new SharedObjectStore<ST>({bufferLengthBytes: totalSize});
        let currentIndex = 0;

        // Load objects into shared memory
        for (const [key, obj] of Object.entries(objects)) {
            const rawObj = obj.raw;
            store.objectMap.set(key, { index: currentIndex, size: rawObj.length });
            store.loadUInt8Array(rawObj, { index: currentIndex, size: rawObj.length });
            currentIndex += rawObj.length;
        }

        return store;
    }

    static fromMemoryMap<ST>(
        shmMemRef: SharedArrayBuffer,
        memMap: {[key: string]: MemoryBounds}
    ): SharedObjectStore<ST> {
        const store = new SharedObjectStore<ST>({sharedMemRef: shmMemRef});
        for (const [key, bounds] of Object.entries(memMap)) {
            store.objectMap.set(key, bounds);
        }
        return store;
    }

    get(key: string): T {
        const bounds = this.objectMap.get(key);
        if (!bounds) throw new Error(`Memory bounds not found for ${key}!`);
        return new SharedObject<T>(this.arrayView, bounds).obj;
    }

    getMemoryMap(): {[key: string]: MemoryBounds} {
        const map = {};
        for (const [key, bound] of this.objectMap.entries())
            map[key] = bound;
        return map;
    }
}
export interface TimedSetEntry {
    timestamp: number;
    index: number;
};

export class TimedSet<T> {

    dict: Map<T, TimedSetEntry> = new Map();
    queue: Array<T> = [];

    maxTimeMS: number;

    constructor(maxTimeMS: number) {
        this.maxTimeMS = maxTimeMS;
    }

    add(value: T) {
        this.dict.set(
            value, {timestamp: Date.now(), index: this.queue.length});
        this.queue.push(value);
    }

    has(value: T) { return this.dict.has(value); }

    delete(value: T) {
        const entry = this.dict.get(value);

        // delete entry from queue
        this.queue.splice(entry.index, 1);

        // fix remaining map indexes
        for (let i = entry.index; i < this.queue.length; i++) {
            const tmpVal = this.queue[i];
            const tmpEntry = this.dict.get(tmpVal);
            this.dict.set(
                tmpVal, {timestamp: tmpEntry.timestamp, index: i});
        }

        // finally delete from map
        this.dict.delete(value);
    }

    deleteFrom(value: T) {
        const startIndex = this.dict.get(value).index;
        let i = startIndex;

        while (i < this.queue.length) {
            this.dict.delete(this.queue[i]);
            i++;
        }

        this.queue.splice(startIndex, this.queue.length - startIndex);
    }

    tick() {
        const now = Date.now();
        let i = 0;
        let currentEntry = this.dict.get(this.queue[i]);
        while (i < this.queue.length &&
               now - currentEntry.timestamp > this.maxTimeMS) {
            this.dict.delete(this.queue[i]);
            i++;
            currentEntry = this.dict.get(this.queue[i]);
        }
        this.queue.splice(0, i);
    }
};

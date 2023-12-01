export class OrderedSet<T> {

    dict: Map<T, number> = new Map();
    queue: Array<T> = [];

    maxEntries: number;

    constructor(maxEntries: number) {
        this.maxEntries = maxEntries;
    }

    add(value: T) {
        if (this.queue.length >= this.maxEntries) {
            // Remove the first element from the queue
            const first = this.queue.shift();

            // Delete the first element from the map
            if (first !== undefined) {
                this.dict.delete(first);
            }

            // Update the indices in the map
            this.dict.forEach((index, key) => {
                this.dict.set(key, index - 1);
            });
        }

        // Add the new value
        this.dict.set(value, this.queue.length);
        this.queue.push(value);
    }


    has(value: T) { return this.dict.has(value); }

    delete(value: T) {
        const entry = this.dict.get(value);

        // delete entry from queue
        this.queue.splice(entry, 1);

        // fix remaining map indexes
        for (let i = entry; i < this.queue.length; i++) {
            const tmpVal = this.queue[i];
            const tmpEntry = this.dict.get(tmpVal);
            this.dict.set(tmpVal, i);
        }

        // finally delete from map
        this.dict.delete(value);
    }

    deleteFrom(value: T) {
        const startIndex = this.dict.get(value);
        let i = startIndex;

        while (i < this.queue.length) {
            this.dict.delete(this.queue[i]);
            i++;
        }

        this.queue.splice(startIndex, this.queue.length - startIndex);
    }

    clear() {
        this.queue = []
        this.dict.clear()
    }
};

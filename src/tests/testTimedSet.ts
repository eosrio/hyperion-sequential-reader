import {TimedSet} from '../timedset.js';

function assert(bool: boolean, errorMsg: string = 'assertion failed!') {
    if (!bool)
        throw new Error(errorMsg);
}

function testInsertion() {

    const tset = new TimedSet<number>(1000);

    for (let i = 0; i < 10; i++)
       tset.add(i);

    for (let i = 0; i < 10; i++) {
        assert(tset.dict.has(i), `Map insertion of ${i} failed!`);
        const entry = tset.dict.get(i);
        assert(
            tset.queue.indexOf(i) == entry.index, `Integrity check of ${i} failed!`);
    }

}

function testLookup() {
    const tset = new TimedSet<number>(1000);

    for (let i = 0; i < 10; i++) {
       tset.add(i);
       assert(tset.has(i), `Lookup of ${i} failed!`)
    }
}

function testDeletionFirstItem() {
    const tset = new TimedSet<number>(1000);

    for (let i = 0; i < 3; i++) {
       tset.add(i);
    }

    tset.delete(0);

    assert(tset.dict.has(1));
    const entry = tset.dict.get(1);
    assert(tset.queue.indexOf(1) == entry.index);

    assert(tset.dict.has(2));
    assert(tset.queue.indexOf(2) == entry.index + 1);
}

function testDeletionMiddleItem() {
    const tset = new TimedSet<number>(1000);

    for (let i = 0; i < 3; i++) {
       tset.add(i);
    }

    tset.delete(1);

    assert(tset.dict.has(0));
    const entry = tset.dict.get(0);
    assert(tset.queue.indexOf(0) == entry.index);

    assert(tset.dict.has(2));
    assert(tset.queue.indexOf(2) == entry.index + 1);
}

function testDeletionLastItem() {
    const tset = new TimedSet<number>(1000);

    for (let i = 0; i < 3; i++) {
       tset.add(i);
    }

    tset.delete(2);

    assert(tset.dict.has(0));
    const entry = tset.dict.get(0);
    assert(tset.queue.indexOf(0) == entry.index);

    assert(tset.dict.has(1));
    assert(tset.queue.indexOf(1) == entry.index + 1);
}

function testDeleteFrom() {
    const tset = new TimedSet<number>(1000);

    for (let i = 0; i < 10; i++) {
       tset.add(i);
    }

    tset.deleteFrom(5);

    for (let i = 0; i < 5; i++) {
        assert(tset.dict.has(i));
        const entry = tset.dict.get(i);
        assert(tset.queue.indexOf(i) == entry.index);
    }

    assert(tset.queue.length == 5);

    for (let i = 5; i < 10; i++) {
        assert(!tset.dict.has(i));
    }
}

function pollingSleep(ms: number) {
    const startTime = Date.now();
    while (Date.now() <= startTime + ms)
        continue
}

function testTick() {
    const maxMS = 1000;
    const tset = new TimedSet<number>(maxMS);

    for (let i = 0; i < 9; i++) {
        tset.add(i);
        pollingSleep(100);
        tset.tick();
    }
    tset.add(9);

    for (let i = 0; i < 10; i++) {
        assert(tset.has(i), `Lookup of ${i} failed!`);
        pollingSleep(100);
        tset.tick();
        assert(!tset.has(i), `${i} present on timedset when it shouldn't\n${JSON.stringify(tset)}`);
    }
}

try {
    testInsertion();

    testLookup();

    testDeletionFirstItem();
    testDeletionMiddleItem();
    testDeletionLastItem();

    testDeleteFrom();

    testTick();

    console.log('Test passed!');
} catch (error) {
    console.log(error.stack);
    console.log('Test failed!');
}

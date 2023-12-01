import {OrderedSet} from '../orderedset.js';

function assert(bool: boolean, errorMsg: string = 'assertion failed!') {
    if (!bool)
        throw new Error(errorMsg);
}

function testInsertion() {

    const tset = new OrderedSet<number>(1000);

    for (let i = 0; i < 10; i++)
       tset.add(i);

    for (let i = 0; i < 10; i++) {
        assert(tset.dict.has(i), `Map insertion of ${i} failed!`);
        const entry = tset.dict.get(i);
        assert(
            tset.queue.indexOf(i) == entry, `Integrity check of ${i} failed!`);
    }

}

function testInsertionMoreThanMax() {

    const tset = new OrderedSet<number>(10);

    for (let i = 0; i < 100; i++)
        tset.add(i);

    assert(tset.queue.length == 10, "Queue is bigger than maxEntries!")
    assert(tset.dict.size == 10, "Dict is bigger than maxEntries!")
}

function testLookup() {
    const tset = new OrderedSet<number>(1000);

    for (let i = 0; i < 10; i++) {
       tset.add(i);
       assert(tset.has(i), `Lookup of ${i} failed!`)
    }
}

function testDeletionFirstItem() {
    const tset = new OrderedSet<number>(1000);

    for (let i = 0; i < 3; i++) {
       tset.add(i);
    }

    tset.delete(0);

    assert(tset.dict.has(1));
    const entry = tset.dict.get(1);
    assert(tset.queue.indexOf(1) == entry);

    assert(tset.dict.has(2));
    assert(tset.queue.indexOf(2) == entry + 1);
}

function testDeletionMiddleItem() {
    const tset = new OrderedSet<number>(1000);

    for (let i = 0; i < 3; i++) {
       tset.add(i);
    }

    tset.delete(1);

    assert(tset.dict.has(0));
    const entry = tset.dict.get(0);
    assert(tset.queue.indexOf(0) == entry);

    assert(tset.dict.has(2));
    assert(tset.queue.indexOf(2) == entry + 1);
}

function testDeletionLastItem() {
    const tset = new OrderedSet<number>(1000);

    for (let i = 0; i < 3; i++) {
       tset.add(i);
    }

    tset.delete(2);

    assert(tset.dict.has(0));
    const entry = tset.dict.get(0);
    assert(tset.queue.indexOf(0) == entry);

    assert(tset.dict.has(1));
    assert(tset.queue.indexOf(1) == entry + 1);
}

function testDeleteFrom() {
    const tset = new OrderedSet<number>(1000);

    for (let i = 0; i < 10; i++) {
       tset.add(i);
    }

    tset.deleteFrom(5);

    for (let i = 0; i < 5; i++) {
        assert(tset.dict.has(i));
        const entry = tset.dict.get(i);
        assert(tset.queue.indexOf(i) == entry);
    }

    assert(tset.queue.length == 5);

    for (let i = 5; i < 10; i++) {
        assert(!tset.dict.has(i));
    }
}

function testDeleteFromFork() {
    const blocks = [
        312799279, 312799280, 312799281, 312799282,
        312799283, 312799284, 312799285, 312799286,
        312799287, 312799288, 312799289, 312799290,
        312799291, 312799292, 312799293, 312799294,
        312799295, 312799296, 312799297, 312799298,
        312799299, 312799300, 312799301, 312799302,
        312799303, 312799304, 312799305, 312799306,
        312799307, 312799308, 312799309, 312799310,
        312799311, 312799312, 312799313, 312799314,
        312799315, 312799316, 312799317, 312799318
    ]
    const tset = new OrderedSet<number>(1000);
    for (const blockNum of blocks) {
       tset.add(blockNum);
    }
    tset.deleteFrom(312799314);

    for (let i = 0; i < 35; i++) {
        const blockNum = blocks[i];
        assert(tset.dict.has(blockNum));
        const entry = tset.dict.get(blockNum);
        assert(tset.queue.indexOf(blockNum) == entry);
    }

    assert(tset.queue.length == 35);

    for (let i = 35; i < 40; i++) {
        assert(!tset.dict.has(blocks[i]));
    }
}

try {
    testInsertion();
    testInsertionMoreThanMax();

    testLookup();

    testDeletionFirstItem();
    testDeletionMiddleItem();
    testDeletionLastItem();

    testDeleteFrom();
    testDeleteFromFork();

    console.log('Test passed!');
} catch (error) {
    console.log(error.stack);
    console.log('Test failed!');
}

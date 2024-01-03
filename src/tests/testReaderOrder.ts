import {HyperionSequentialReader} from "../reader.js";
import {ABI} from "@greymass/eosio";
import {readFileSync} from "node:fs";
import {expect} from 'chai';
import {BSON} from "bson";
import * as console from "console";

const options = {
    shipApi: 'ws://127.0.0.1:29999',
    chainApi: 'http://127.0.0.1:8888',
    poolSize: 16,
    blockConcurrency: 16,
    blockHistorySize: 1000,
    inputQueueLimit: 8000,
    outputQueueLimit: 8000,
    startBlock: 312087081,
    endBlock: 312187080,
    actionWhitelist: {
        'eosio.token': ['transfer'],
        'eosio.evm': ['raw', 'withdraw']
    },
    tableWhitelist: {},
    logLevel: 'info',
    // workerLogLevel: 'debug',
    maxPayloadMb: Math.floor(1024 * 1.5)
};

const reader = new HyperionSequentialReader(options);
reader.onError = (err) => {throw err};

const abis = ['eosio', 'telos.evm', 'eosio.token'].map((abiFileNames) => {
    const jsonAbi = JSON.parse(readFileSync(`./${abiFileNames}.abi`).toString())
    return {account: jsonAbi.account_name, abi: ABI.from(jsonAbi.abi)};
});

reader.addContracts(abis);

let lastPushed = options.startBlock - 1;
let lastPushedTS = 'unknown';
let totalRead = 0;
let firstBlock = -1;
let firstBlockTs: number;

const statsTask = setInterval(() => {
    if (lastPushed >= options.startBlock && reader.perfMetrics.max > 0) {
        const avgSpeed = reader.perfMetrics.average.toFixed(2).padStart(8, ' ');
        const maxSpeed = reader.perfMetrics.max.toFixed(2).padStart(8, ' ')
        console.log(`${lastPushed} @ ${lastPushedTS}: avg speed: ${avgSpeed} blocks/s | max speed: ${maxSpeed} blocks/s`);
    }
}, 1000);

reader.events.on('block', async (block) => {
    const currentBlock = block.blockInfo.this_block.block_num;

    if (firstBlock < 0) {
        firstBlock = currentBlock;
        firstBlockTs = performance.now();
    }

    expect(currentBlock).to.be.equal(lastPushed + 1);
    lastPushed = block.blockInfo.this_block.block_num;
    lastPushedTS = block.blockHeader.timestamp;
    totalRead++;
    reader.ack();
});

reader.events.on('stop', () => {
    const elapsedMs = performance.now() - firstBlockTs;
    const elapsedS = elapsedMs / 1000;
    console.log(`elapsed sec: ${elapsedS}`);
    console.log(`avg speed: ${(totalRead / elapsedS).toFixed(2)}`);

    if (options.startBlock > 0)
        expect(firstBlock, 'First block received mismatch!').to.be.equal(options.startBlock);

    if (options.endBlock > 0)
        expect(lastPushed, 'Last block received mismatch!').to.be.equal(options.endBlock);

    clearInterval(statsTask);
});

await reader.start();
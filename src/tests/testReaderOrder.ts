import {HyperionSequentialReader} from "../reader.js";
import {ABI} from "@greymass/eosio";
import {readFileSync} from "node:fs";
import {expect} from 'chai';

const options = {
    shipApi: 'ws://127.0.0.1:29999',
    chainApi: 'http://127.0.0.1:8888',
    poolSize: 4,
    blockConcurrency: 4,
    blockHistorySize: 1000,
    inputQueueLimit: 200,
    outputQueueLimit: 1000,
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
await Promise.all(abis.map(abiInfo => reader.addContract(abiInfo.account, abiInfo.abi)));

let pushed = 0;
let lastLogTime = new Date().getTime() / 1000;
let lastPushed = options.startBlock - 1;
let lastPushedTS = 'unknown';
let firstBlock = -1;

const statsTask = setInterval(() => {
   const now = new Date().getTime() / 1000;
   const delta = now - lastLogTime;
   const speed = pushed / delta;
   if (!reader.isShipAbiReady)
       console.log(`ship abi not ready...`);
   console.log(`${lastPushed} @ ${lastPushedTS}: ${speed.toFixed(2)} blocks/s`);
   pushed = 0;
   lastLogTime = now;
}, 1000);

reader.events.on('block', async (block) => {
    const currentBlock = block.blockInfo.this_block.block_num;

    if (firstBlock < 0) firstBlock = currentBlock;

    expect(currentBlock).to.be.equal(lastPushed + 1);
    lastPushed = block.blockInfo.this_block.block_num;
    lastPushedTS = block.blockHeader.timestamp;
    pushed++;
    reader.ack();
});

reader.events.on('stop', () => {
    if (options.startBlock > 0)
        expect(firstBlock, 'First block received mismatch!').to.be.equal(options.startBlock);

    if (options.endBlock > 0)
        expect(lastPushed, 'Last block received mismatch!').to.be.equal(options.endBlock);

    clearInterval(statsTask);
});

await reader.start();
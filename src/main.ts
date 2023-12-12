import {HyperionSequentialReader} from "./reader.js";
import {ABI} from "@greymass/eosio";
import {readFileSync} from "node:fs";

const reader = new HyperionSequentialReader({
    shipApi: 'ws://127.0.0.1:29999',
    chainApi: 'http://127.0.0.1:8888',
    poolSize: 1,
    blockConcurrency: 1,
    blockHistorySize: 20,
    outputQueueLimit: 10,
    startBlock: 1,
    logLevel: 'info'
});

['eosio', 'eosio.evm', 'eosio.token'].forEach(c => {
    const abi = ABI.from(JSON.parse(readFileSync(`./${c}.abi`).toString()));
    reader.addContract(c, abi);
})

let pushed = 0;
let lastLogTime = new Date().getTime() / 1000;
let lastPushed = -1;
let lastPushedTS = 'unknown';

setInterval(() => {
   const now = new Date().getTime() / 1000;
   const delta = now - lastLogTime;
   const speed = pushed / delta;
   console.log(`${lastPushed} @ ${lastPushedTS}: ${speed.toFixed(2)} blocks/s`);
   pushed = 0;
   lastLogTime = now;
}, 1000);

reader.events.on('block', async (block) => {
    lastPushed = block.blockInfo.this_block.block_num;
    lastPushedTS = block.blockHeader.timestamp;
    pushed++;
    reader.ack();
});

reader.start();

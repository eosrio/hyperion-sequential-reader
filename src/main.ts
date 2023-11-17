import {HyperionSequentialReader} from "./reader.js";
import {ABI} from "@greymass/eosio";
import {readFileSync} from "node:fs";

const reader = new HyperionSequentialReader({
    shipApi: 'ws://127.0.0.1:29999',
    chainApi: 'http://127.0.0.1:8888',
    poolSize: 4,
    blockConcurrency: 2,
    outputQueueLimit: 1000,
    startBlock: -1
});

['eosio', 'eosio.evm', 'eosio.token'].forEach(c => {
    const abi = ABI.from(JSON.parse(readFileSync(`./${c}.abi`).toString()));
    reader.addContract(c, abi);
})

let pushed = 0;
let lastBlockLogged = 0;
reader.events.on('block', async (block) => {
    // const now = Date.now();
    const thisBlock = block.blockInfo.this_block.block_num;
    pushed++;

    if (lastBlockLogged === 0) {
        lastBlockLogged = thisBlock;
    } else {
        if (thisBlock !== lastBlockLogged + 1) {
            console.log(`Block out of order! expected ${lastBlockLogged + 1} and got ${thisBlock}`);
        } else {
            lastBlockLogged = thisBlock;
        }
    }

    if (pushed > 30) {
        pushed = 0;
        reader.restart();
    }

    reader.ack();
});

reader.start();

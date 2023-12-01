import {HyperionSequentialReader} from "./reader.js";
import {ABI} from "@greymass/eosio";
import {readFileSync} from "node:fs";

const reader = new HyperionSequentialReader({
    shipApi: 'ws://test1.us.telos.net:29999',
    chainApi: 'http://test1.us.telos.net:8888',
    poolSize: 1,
    blockConcurrency: 1,
    outputQueueLimit: 10,
    startBlock: -1,
    logLevel: 'debug'
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
    // console.log(thisBlock);
    pushed++;

    if (lastBlockLogged === 0) {
        lastBlockLogged = thisBlock;
    } else {
        if (thisBlock !== lastBlockLogged + 1) {
            // console.log(`Block out of order! expected ${lastBlockLogged + 1} and got ${thisBlock}`);
        } else {
            lastBlockLogged = thisBlock;
        }
    }

    reader.ack();
});

reader.start();

import {HyperionSequentialReader} from "./reader.js";
import {ABI} from "@greymass/eosio";
import {readFileSync} from "node:fs";

const reader = new HyperionSequentialReader({
    shipApi: 'ws://test1.us.telos.net:28999',
    chainApi: 'http://testnet.telos.net',
    poolSize: 4,
    blockConcurrency: 2,
    outputQueueLimit: 1000,
    startBlock: 174582074
});

['eosio', 'eosio.evm', 'eosio.token'].forEach(c => {
    const abi = ABI.from(JSON.parse(readFileSync(`./${c}.abi`).toString()));
    reader.addContract(c, abi);
})

let lastBlockLogged = 0;
let lastLogTime = Date.now();
reader.events.on('block', async (block) => {
    // const now = Date.now();
    const thisBlock = block.blockInfo.this_block.block_num;
    // console.log(thisBlock);
    if (lastBlockLogged === 0) {
        lastBlockLogged = thisBlock;
    } else {
        if (thisBlock !== lastBlockLogged + 1) {
            console.log('Block out of order!');
        } else {
            lastBlockLogged = thisBlock;
        }
    }

    // await new Promise<void>(resolve => {
    //     setTimeout(() => {
    //         resolve();
    //     }, 150);
    // });

    // if ((now - lastLogTime) > 5000) {
    //     const blocksSeen = thisBlock - lastBlockLogged;
    //     const elapsedSeconds = (now - lastLogTime) / 1000;
    //     const blocksPerSecond = blocksSeen / elapsedSeconds;
    //     console.log(`Processing speed: ${blocksPerSecond} blocks/sec`)
    //     lastBlockLogged = thisBlock;
    //     lastLogTime = now;
    // }
    // console.log(block);
    // if (block.transactions && block.transactions.length > 0) {
    //     console.log(block.transactions);
    // }
    // for (let action of block.actions) {
    //     console.log(action);
    // }
    // for (let delta of block.deltas) {
    //     console.log(delta);
    // }
    // console.log(`Block: ${thisBlock}`);
    reader.ack();
    // console.log(Object.keys(block));
    //console.log(block.acts);
    //console.log(block.contractRows);
});

reader.start();

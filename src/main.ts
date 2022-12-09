import {HyperionSequentialReader} from "./reader.js";
import {ABI} from "@greymass/eosio";
import {readFileSync} from "fs";

const reader = new HyperionSequentialReader('ws://23.19.195.55:28999', {
    poolSize: 4,
    startBlock: 136393814
});

['eosio', 'eosio.evm', 'eosio.token'].forEach(c => {
    const abi = ABI.from(JSON.parse(readFileSync(`./${c}.abi`).toString()));
    reader.addContract(c, abi);
})

let lastBlockLogged = 0;
let lastLogTime = Date.now();
reader.events.on('block', (block) => {
    let now = Date.now()
    const thisBlock = block.this_block.block_num;
    if (lastBlockLogged === 0) {
        lastBlockLogged = thisBlock;
    }

    if ((now - lastLogTime) > 5000) {
        const blocksSeen = thisBlock - lastBlockLogged;
        const elapsedSeconds = (now - lastLogTime) / 1000;
        const blocksPerSecond = blocksSeen / elapsedSeconds;
        console.log(`Processing speed: ${blocksPerSecond} blocks/sec`)
        lastBlockLogged = thisBlock;
        lastLogTime = now;
    }
    reader.ack();
     //console.log(block.acts);
     //console.log(block.contractRows);
});

reader.start();

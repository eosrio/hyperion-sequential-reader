import {HyperionSequentialReader} from "./reader.js";
import {ABI} from "@greymass/eosio";
import {readFileSync} from "fs";

const reader = new HyperionSequentialReader('ws://192.168.0.130:48081', {
    poolSize: 4,
    startBlock: 2,
    endBlock: 1000
});

const abi = ABI.from(JSON.parse(readFileSync('./eosio.abi').toString()));
reader.addContract('eosio', abi);

reader.events.on('block', (block) => {
    const block_num = block.this_block.block_num;
    console.log(block_num, block.acts.length, block.contractRows.length);
    reader.ack();
});

reader.start();

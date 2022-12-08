import {HyperionSequentialReader} from "./reader.js";
import {ABI} from "@greymass/eosio";
import {readFileSync} from "fs";

const reader = new HyperionSequentialReader('ws://127.0.0.1:48080', {
    poolSize: 4
});

const abi = ABI.from(JSON.parse(readFileSync('./eosio.abi').toString()));
reader.addContract('eosio', abi);

reader.events.on('block', (block) => {
    const block_num = block.this_block.block_num;
    console.log(block_num, block.acts.length, block.contractRows.length);
    console.log(block.acts);
    reader.ack();
});

reader.start();

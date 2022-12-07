import {HyperionSequentialReader} from "./reader.js";
import {ABI} from "@greymass/eosio";
import {readFileSync} from "fs";

const reader = new HyperionSequentialReader('ws://127.0.0.1:8080', {
    poolSize: 4
});

const abi = ABI.from(JSON.parse(readFileSync('./eosio.abi').toString()));
reader.addContract('eosio', abi);

reader.events.on('block', (block) => {
    // console.log(block.acts);
    // console.log(block.contractRows);
});

reader.start();

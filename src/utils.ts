export const sleep = (ms: number) => new Promise(res => setTimeout(res, ms));

import {ABI} from "@greymass/eosio";

const onblockAbiAction: ABI.Action = {
    name: 'onblock',
    type: 'onblock',
    ricardian_contract: ''
};

const onblockAbiStruct: ABI.Struct = {
    name: 'onblock',
    base: '',
    fields: [
        { name: 'header', type: 'block_header' } // Placeholder for ignored type
    ]
};

export function addOnBlockToABI(abi: ABI) {
    abi.structs = [onblockAbiStruct, ...abi.structs];
    abi.actions = [onblockAbiAction, ...abi.actions];
}

export function logLevelToInt(level: string) {
    const levels = [
        'error', 'warning', 'info', 'debug'
    ];
    if (!levels.includes(level))
        throw new Error(`Unimplemented level ${level}`);
    return levels.indexOf(level);
}
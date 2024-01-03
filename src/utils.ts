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

function isObject(item) {
    return (item && typeof item === 'object' && !Array.isArray(item));
}

/**
 * Deep merge two objects.
 * @param target
 * @param ...sources
 */
export function mergeDeep(target, ...sources) {
    if (!sources.length) return target;
    const source = sources.shift();

    if (isObject(target) && isObject(source)) {
        for (const key in source) {
            if (isObject(source[key])) {
                if (!target[key]) Object.assign(target, { [key]: {} });
                mergeDeep(target[key], source[key]);
            } else {
                Object.assign(target, { [key]: source[key] });
            }
        }
    }

    return mergeDeep(target, ...sources);
}

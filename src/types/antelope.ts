export interface Block {
    block_num: number;
    block_id: string;
}

export interface BlockInfo {
    head: Block;
    last_irreversible: Block;
    this_block: Block;
    prev_block: Block;
}

export interface ProducerScheduleType {
    version: number;
    producers: string[];
}

export type ExtensionsType = [number, Uint8Array][];
export interface BlockHeader {
    timestamp: string;
    producer: string;
    confirmed: number;
    previous: string;
    transaction_mroot: string;
    action_mroot: string;
    schedule_version: number;
    new_producers?: null | undefined | ProducerScheduleType;  // legacy
    header_extensions: ExtensionsType;
    producer_signature: string;
    block_extensions: ExtensionsType;
}

export interface TableDelta {
    present: boolean;
    code: string;
    scope: string;
    table: string;
    primary_key: number;
    payer: string;
    value: any;  // decoded table JSON, depends entirely on abi type
}

export interface AccountDelta {
  account: string;
  delta: number;
}

export interface AuthSequence {
    account: string;
    sequence: number;
}

export interface ActionReceipt {
      receiver: string;
      act_digest: string;
      global_sequence: number; ///< total number of actions dispatched since genesis
      recv_sequence: number; ///< total number of actions with this receiver since genesis
      auth_sequence: AuthSequence[];
      code_sequence: number; ///< total number of setcodes
      abi_sequence: number ; ///< total number of setabis
}

export interface PermissionLevel {
    actor: string;
    permission: string;
}

export interface Action {
    account: string;
    name: string;
    authorization: PermissionLevel[];
    data: string;
}

export interface DecodedAction {
    account: string;
    name: string;
    authorization: PermissionLevel[];
    data: any;  // will contain action arguments as json
}
export interface ActionTrace {
  action_ordinal: number;
  creator_action_ordinal: number;
  closest_unnotified_ancestor_action_ordinal: number;
  receipt: ActionReceipt;
  receiver: string;
  act: Action;
  context_free: boolean;
  elapsed: number;
  console: string;
  trx_id: string;
  block_num: number;
  block_time: string;
  producer_block_id: string;
  account_ram_deltas: AccountDelta[];
  except?: any;
  error_code?: number;
}

export interface ActionWithExtras {
    actionOrdinal: number;
    creatorActionOrdinal: number;
    trxId: string;
    cpu: number;
    net: number;
    ram: AccountDelta[];
    receipt: ActionReceipt;
    receiver: string;
    console: string;
    signatures: string[];
    act: Action;
}


export interface DecodedBlock {
    blockNum: number;
    blockInfo: BlockInfo;
    blockHeader: BlockHeader;
    deltas: TableDelta[];
    actions: ActionWithExtras[];
    decodeElapsed: number
}
// CCIP batches may execute messages from different UTC days in one transaction.
// Store a per-message key in tx_hash; public API queries expose the original hash.
// This keeps the existing transactions unique index and other bridges unchanged.
export const CCIP_HASH_SEPARATOR = ":ccip:";

export const encodeCCIPTransactionHash = (transactionHash: string, messageId: string) =>
  `${transactionHash}${CCIP_HASH_SEPARATOR}${messageId}`;

export const decodeCCIPTransactionHash = (storedHash: string) => storedHash.split(CCIP_HASH_SEPARATOR)[0];

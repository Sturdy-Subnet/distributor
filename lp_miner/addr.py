import hashlib
from scalecodec.utils.ss58 import ss58_encode


def h160_to_ss58(h160_addr: str) -> str:
    """
    Convert H160 address to SS58 address using the same logic as the TypeScript version.
    """
    # Validate H160 address format
    if not h160_addr.startswith("0x") or len(h160_addr) != 42:
        raise ValueError(f"Invalid H160 address format: {h160_addr}")

    try:
        # Remove '0x' prefix and convert to bytes
        address_bytes = bytes.fromhex(h160_addr[2:])

        # Add 'evm:' prefix as bytes
        prefix_bytes = b"evm:"
        convert_bytes = prefix_bytes + address_bytes

        # Blake2 hash with 256-bit output (32 bytes)
        blake2_hash = hashlib.blake2b(convert_bytes, digest_size=32).digest()

        # Encode as SS58 with prefix 42 (generic substrate)
        ss58_address = ss58_encode(blake2_hash, ss58_format=42)

        return ss58_address
    except Exception as e:
        raise ValueError(f"Failed to convert H160 {h160_addr} to SS58: {e}")

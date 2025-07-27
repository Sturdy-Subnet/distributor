import asyncio
import json
import time
import typing
import sturdy
from sturdy.base.miner import BaseMinerNeuron
import bittensor as bt
from constants import TOKEN_IDS_FILE


class Miner(BaseMinerNeuron):
    """
    Your miner neuron class. You should use this class to define your miner's behavior. In particular, you should replace the
    forward function with your own logic. You may also want to override the blacklist and priority functions according to your
    needs.

    This class inherits from the BaseMinerNeuron class, which in turn inherits from BaseNeuron. The BaseNeuron class takes
    care of routine tasks such as setting up wallet, subtensor, metagraph, logging directory, parsing config, etc. You can
    override any of the methods in BaseNeuron if you need to customize the behavior.

    This class provides reasonable default behavior for a miner such as blacklisting unrecognized hotkeys, prioritizing
    requests based on stake, and forwarding requests to the forward function. If you need to define custom
    """

    async def _init_async(self, config=None) -> None:
        await super()._init_async(config=config)

    async def uniswap_v3_lp_forward(
        self, synapse: sturdy.protocol.UniswapV3PoolLiquidity
    ) -> sturdy.protocol.UniswapV3PoolLiquidity:
        """
        Forward function for Uniswap V3 LP positions. This function should be overridden with your own logic.
        """
        # Implement your logic here
        # read the token ids from the token ids file
        with open(TOKEN_IDS_FILE, "r") as f:
            token_ids = json.load(f)
        # update the token ids in the synapse
        synapse.token_ids = token_ids
        return synapse

    async def forward(
        self, synapse: sturdy.protocol.AllocateAssets
    ) -> sturdy.protocol.AllocateAssets:
        return synapse

    async def blacklist(
        self, synapse: sturdy.protocol.AllocateAssets
    ) -> typing.Tuple[bool, str]:
        """
        Determines whether an incoming request should be blacklisted and thus ignored. Your implementation should
        define the logic for blacklisting requests based on your needs and desired security parameters.

        Blacklist runs before the synapse data has been deserialized (i.e. before synapse.data is available).
        The synapse is instead contructed via the headers of the request. It is important to blacklist
        requests before they are deserialized to avoid wasting resources on requests that will be ignored.

        Args:
            synapse (template.protocol.AllocateAssets): A synapse object constructed from the headers of the incoming request.

        Returns:
            Tuple[bool, str]: A tuple containing a boolean indicating whether the synapse's hotkey is blacklisted,
                            and a string providing the reason for the decision.

        This function is a security measure to prevent resource wastage on undesired requests. It should be enhanced
        to include checks against the metagraph for entity registration, validator status, and sufficient stake
        before deserialization of synapse data to minimize processing overhead.

        Example blacklist logic:
        - Reject if the hotkey is not a registered entity within the metagraph.
        - Consider blacklisting entities that are not validators or have insufficient stake.

        In practice it would be wise to blacklist requests from entities that are not validators, or do not have
        enough stake. This can be checked via metagraph.S and metagraph.validator_permit. You can always attain
        the uid of the sender via a metagraph.hotkeys.index( synapse.dendrite.hotkey ) call.

        Otherwise, allow the request to be processed further.
        """

        bt.logging.info("Checking miner blacklist")

        if synapse.dendrite.hotkey not in self.metagraph.hotkeys:  # type: ignore[]
            return True, "Hotkey is not registered"

        requesting_uid = self.metagraph.hotkeys.index(synapse.dendrite.hotkey)  # type: ignore[]
        stake = self.metagraph.S[requesting_uid].item()

        bt.logging.info(f"Requesting UID: {requesting_uid} | Stake at UID: {stake}")

        if stake <= self.config.validator.min_stake:
            bt.logging.info(
                f"Hotkey: {synapse.dendrite.hotkey}: stake below minimum threshold of {self.config.validator.min_stake}"  # type: ignore[]
            )
            return True, "Stake below minimum threshold"

        validator_permit = self.metagraph.validator_permit[requesting_uid].item()
        if not validator_permit:
            return True, "Requesting UID has no validator permit"

        bt.logging.trace(f"Allowing request from UID: {requesting_uid}")
        return False, "Allowed"

    async def priority(self, synapse: sturdy.protocol.AllocateAssets) -> float:
        """
        The priority function determines the order in which requests are handled. More valuable or higher-priority
        requests are processed before others. You should design your own priority mechanism with care.

        This implementation assigns priority to incoming requests based on the calling entity's stake in the metagraph.

        Args:
            synapse (template.protocol.AllocateAssets): The synapse object that contains metadata about the incoming request.

        Returns:
            float: A priority score derived from the stake of the calling entity.

        Miners may recieve messages from multiple entities at once. This function determines which request should be
        processed first. Higher values indicate that the request should be processed first. Lower values indicate
        that the request should be processed later.

        Example priority logic:
        - A higher stake results in a higher priority value.
        """
        caller_uid = self.metagraph.hotkeys.index(
            synapse.dendrite.hotkey
        )  # Get the caller index. # type: ignore[]
        priority = float(
            self.metagraph.S[caller_uid]
        )  # Return the stake as the priority.
        bt.logging.trace(
            f"Prioritizing {synapse.dendrite.hotkey} with value: ", priority
        )  # type: ignore[]
        return priority

    async def save_state(self) -> None:
        """Saves the miner state - currently no state to save for miners."""
        bt.logging.info("Miner state saving skipped - no state to save.")
        pass

    async def load_state(self) -> None:
        """Loads the miner state - currently no state to load for miners."""
        bt.logging.info("Miner state loading skipped - no state to load.")
        pass


async def main() -> None:
    miner = await Miner.create()
    async with miner:
        while True:
            bt.logging.info(f"Miner running... {time.time()}")
            await asyncio.sleep(10)


# This is the main function, which runs the miner.
if __name__ == "__main__":
    asyncio.run(main())

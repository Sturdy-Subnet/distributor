import sturdy
from sturdy.base.miner import BaseMinerNeuron
import bittensor as bt


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
        return await super().uniswap_v3_lp_forward(synapse)


def main():
    print("Hello from sturdy-lp-miner!")


if __name__ == "__main__":
    main()

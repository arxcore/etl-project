# from .business import US_BUSINESS
from .consumer import US_CONSUMER
from .labour import US_LABOUR
from .money import US_MONEY
from .trade import US_TRADE
from .price import US_PRICE


USA_INDICATORS = {
    "price": US_PRICE,
    "labour": US_LABOUR,
    "trade": US_TRADE,
    "consumer": US_CONSUMER,
    "money": US_MONEY,
    # Access Latter if neded
    # "business": US_BUSINESS,
}
__all__ = ["USA_INDICATORS"]

from .consumer import UK_CONSUMER
from .labour import UK_LABOUR
from .price import UK_PRICE
# from .business import UK_BUSINESS
# from .trade import UK_TRADE

UK_INDICATORS = {
    "price": UK_PRICE,
    "labour": UK_LABOUR,
    "consumer": UK_CONSUMER,
    # Access Latter
    # "business": UK_BUSINESS,
    # "trade": UK_TRADE
}

__all__ = ["UK_INDICATORS"]

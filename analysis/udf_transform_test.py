# %%
from textwrap import indent
import simplejson as json
from pprint import pprint
from copy import deepcopy

decoded_data = {
    "address": "0x00000000006c3852cbEf3e08E8dF289169EdE581",
    "data": [
        {
            "decoded": True,
            "name": "orderHash",
            "type": "bytes32",
            "value": "0x71c2d239d193ead00563eb4c1aa2a0b67b472bd1ec07e30e1cc5305847f6ed71",
        },
        {
            "decoded": True,
            "name": "offerer",
            "type": "address",
            "value": "0x412426adf9dc080cc4e2d0d90f12166ef9d75d84",
        },
        {
            "decoded": True,
            "name": "zone",
            "type": "address",
            "value": "0x004c00500000ad104d7dbd00e3ae0a5c00560c00",
        },
        {
            "decoded": True,
            "name": "recipient",
            "type": "address",
            "value": "0x22034382da3f3e76ed455f96faf186e84e1541a0",
        },
        {
            "components": [
                {"internalType": "enum ItemType", "name": "itemType", "type": "uint8"},
                {"internalType": "address", "name": "token", "type": "address"},
                {"internalType": "uint256", "name": "identifier", "type": "uint256"},
                {"internalType": "uint256", "name": "amount", "type": "uint256"},
            ],
            "decoded": True,
            "name": "offer",
            "type": "tuple[]",
            "value": [[2, "0xa35aa193f94a90eca0ae2a3fb5616e53c1f35193", 8272, 1]],
        },
        {
            "components": [
                {"internalType": "enum ItemType", "name": "itemType", "type": "uint8"},
                {"internalType": "address", "name": "token", "type": "address"},
                {"internalType": "uint256", "name": "identifier", "type": "uint256"},
                {"internalType": "uint256", "name": "amount", "type": "uint256"},
                {
                    "internalType": "address payable",
                    "name": "recipient",
                    "type": "address",
                },
            ],
            "decoded": True,
            "name": "consideration",
            "type": "tuple[]",
            "value": [
                [
                    0,
                    "0x0000000000000000000000000000000000000000",
                    0,
                    "23493000000000000",
                    "0x412426adf9dc080cc4e2d0d90f12166ef9d75d84",
                ],
                [
                    0,
                    "0x0000000000000000000000000000000000000000",
                    0,
                    615000000000000,
                    "0x0000a26b00c1f0df003000390027140000faa719",
                ],
                [
                    0,
                    "0x0000000000000000000000000000000000000000",
                    0,
                    492000000000000,
                    "0xe718904d67cba3c65e4895fb27247b0fd96d733a",
                ],
            ],
        },
    ],
    "decoded": True,
    "name": "OrderFulfilled",
}
decoded_data2 = {
    "address": "0xA35aa193f94A90eca0AE2a3fB5616E53C1F35193",
    "data": [
        {
            "decoded": True,
            "name": "from",
            "type": "address",
            "value": "0x412426adf9dc080cc4e2d0d90f12166ef9d75d84",
        },
        {
            "decoded": True,
            "name": "to",
            "type": "address",
            "value": "0x22034382da3f3e76ed455f96faf186e84e1541a0",
        },
        {"decoded": True, "name": "tokenId", "type": "uint256", "value": 8272},
    ],
    "decoded": True,
    "name": "Transfer",
}


def transform_event(event: dict):
    new_event = deepcopy(event)
    if new_event.get("components"):
        components = new_event.get("components")
        for iy, y in enumerate(new_event["value"]):
            for i, c in enumerate(components):
                y[i] = {"value": y[i], **c}
            new_event["value"][iy] = {z["name"]: z["value"] for z in y}
        return new_event
    else:
        return event


def transform(events: list):
    try:
        results = [
            transform_event(event) if event["decoded"] else event
            for event in events["data"]
        ]
        events["data"] = results
        return events
    except:
        return events


# %%

# if __name__ == "__main__":
results = transform(decoded_data)

print(json.dumps(results, indent=4))

# %%

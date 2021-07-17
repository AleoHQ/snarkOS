Create a new transaction from a given transaction kernel, returning the encoded transaction and the new records.

### Protected Endpoint

Yes

### Arguments

|       Parameter      |  Type      | Required |             Description            |
|:--------------------:|:----------:|:--------:|:----------------------------------:|
| `private_keys`       | \[string\] |    Yes   | An array of private key strings    |
| `transaction_kernel` | string     |    Yes   | The hex encoded transaction kernel |

### Response

|       Parameter       |  Type  |                  Description                  |
|:---------------------:|:------:|:--------------------------------------------- |
| `encoded_transaction` | string | The hex encoding of the generated transaction |
| `encoded_records`     | array  | The hex encodings of the generated records    |

### Example
```ignore
curl --user username:password --data-binary '{ 
    "jsonrpc":"2.0",
    "id": "1",
    "method": "createtransaction",
    "params": ["[private_key_0, private_key_1]", "transaction_kernel_hexstring"]
}' -H 'content-type: application/json' http://127.0.0.1:3030/
```

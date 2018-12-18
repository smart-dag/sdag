1. generate 100 wallets by default, if you want to generate [n] wallets, run the order below:

```
test_case wallets [n]
```

2. create genesis joint and first payment
```
test_case genesis
```

3. send genesis joint and first payment
```
test_case raw_post --genesis 
test_case raw_post --first_pay
```

4. transfer coins to  generated wallets
```
test_case send --pay [coins]
```


5. transfer to eacher other among generated [num] wallets, and [num] should be less than the number of the wallets.
```
test_case send --continue
```

6. transfer coins to  generated wallets [n] times
```
test_case send --pay [coins] --continue [n]
```

7. find some wallet transaction history
```
test_case log  [ADDRESS] -[n] -[v]
```

8. find some wallet information
```
test_case info  [ADDRESS] 
```

9. get some wallet balance
```
test_case balance  [ADDRESS] 
```

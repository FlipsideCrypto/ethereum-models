{% docs eth_traces_table_doc %}

This table contains flattened trace data for internal contract calls on the Ethereum blockchain. Hex encoded fields can be decoded to integers by using `TO_NUMBER(<FIELD>, 'XXXXXXXXXXXX')`, with the number of Xs being the max length of the encoded field. You must also remove the `0x` from your field to use the `TO_NUMBER()` function, if applicable. 

{% enddocs %}
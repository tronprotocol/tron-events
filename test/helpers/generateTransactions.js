
let example = {
  transaction_id: '4a438dc54bfde07d32c3f63936a205563692ee513b1a6d41ea763a873f039775',
  result:
      {
        transferred: 8082,
        address: '0x7ac191e4323604ee43754b3f6b14ad087cbc4faf'
      },
  resource_Node: 'FullNode',
  result_type: {transferred: 'uint256', address: 'address'},
  block_timestamp: '1542679812000',
  block_number: '8080',
  event_name: 'Transfer',
  contract_address: 'TGjzXqsWtxEiKWTjnU5RSA5uCJefRCyPMM',
  event_index: '2',
  raw_data:
      {
        data: '0000000000000000000000000000000000000000000000000000000000001f92',
        topics:
            ['1cf6f3b24c5afcea9fc4c045b621e03743b6f8f796bb7a0513dc7f5374270a0e',
              '0000000000000000000000007ac191e4323604ee43754b3f6b14ad087cbc4faf']
      }
}


class GenerateTransactions {




}


module.exports = new GenerateTransactions
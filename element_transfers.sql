with transfers_data as (
select * from (
select block_time, 
    block_number, 
    contract_address,
--    '' as token, Need to cross reference with token table if re-running. Did this locally before. 
    substring(topic2, 13, 20) as "sender", 
    substring(topic3, 13, 20) as "recipient",
    bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, 
    tx_hash, 
    index, 
    tx_index 
from ethereum.logs
where contract_address IN ('\xf6dc4640d2783654bef88e0df3fb0f051f0dfc1a','\x6dd0f7c8f4793ed2531c0df4fea8633a21fdcff4','\xb03c6b351a283bc1cd26b9cf6d7b0c4556013bdb','\xce16e7ed7654a3453e8faf748f2c82e57069278f','\x544c823194218f0640dae8291c1f59752d25fae3','\x3a693eb97b500008d4bb6258906f7bbca1d09cc5','\xc80fef22fccc277ac0ffea84d111888a767f717e','\x13cf9e8115f35828a26062b6c05a56c72f54e0c6','\xc9ad279994980f8df348b526901006972509677f','\x63e9b50dd3eb63bfbf93b26f57b9efb574e59576','\x6ac02ecd0c2a23b11f9afb3b3aaf237169475cac','\x489eedb33f82574afeabb3f4e156fbf662308ada','\x09b1b33bad0e87454ff05696b1151bfbd208a43f','\x14792d3f6fcf2661795d1e08ef818bf612708bbf','\xabb93e3787b984cb62dcd962af8732f52ff57586','\x893b30574bf183d69413717f30b17062ec9dfd8b','\xa8d4433badaa1a35506804b43657b0694dea928d','\x56f30398d13f111401d6e7ffe758254a0946687d','\x71628c66c502f988fbb9e17081f2bd14e361faf4','\x8ffd1dc7c3ef65f833cf84dbcd15b6ad7f9c54ec','\xa47d1251cf21ad42685cc6b8b3a186a73dbd06cf','\xedf085f65b4f6c155e13155502ef925c9a756003','\x56df5ef1a0a86c2a5dd9cc001aa8152545bdbdec','\x90ca5cef5b29342b229fb8ae2db5d8f4f894d652','\x10a2f8bd81ee2898d7ed18fb8f114034a549fa59','\x7edde0cb05ed19e03a9a47cd5e53fc57fde1c80c','\x787546bf2c05e3e19e2b6bde57a203da7f682eff','\x4db9024fc9f477134e00da0da3c77de98d9836ac','\x4bd6d86debdb9f5413e631ad386c4427dc9d01b2','\xd16847480d6bc218048cd31ad98b63cc34e5c2bf','\x4abb6fd289fa70056cfcb58cebab8689921eb922','\x062f38735aac32320db5e2dbbeb07968351d7c72','\xd5d7bc115b32ad1449c6d0083e43c87be95f2809','\x4212be3c7b255ba4b29705573abd023cdce21542','\xf94a7df264a2ec8bceef2cfe54d7ca3f6c6dfc7a','\x802d0f2f4b5f1fb5bfc9b2040a703c1464e1d4cb','\x10f21c9bd8128a29aa785ab2de0d044dcdd79436','\x5fa3ce1fb47bc8a29b5c02e2e7167799bbaf5f41','\x1d310a6238e11c8be91d83193c88a99eb66279be','\x6fe95fafe2f86158c77bf18350672d360bfc78a2','\x67f8fcb9d3c463da05de1392efdbb2a87f8599ea','\xde620bb8be43ee54d7aa73f8e99a7409fe511084','\x8e9d636bbe6939bd0f52849afc02c0c66f6a3603','\xe54b3f5c444a801e61becdca93e74cdc1c4c1f90','\xb70c25d96ef260ea07f650037bf68f5d6583885e','\x415747ee98d482e6dd9b431fa76ad5553744f247','\x7c9cf12d783821d5c63d8e9427af5c44bad92445','\x9e030b67a8384cbba09d5927533aa98010c87d91','\x7173b184525fead2ffbde5fbe6fcb65ea8246ee7','\x2d6e3515c8b47192ca3913770fa741d3c4dac354','\x7320d680ca9bce8048a286f00a79a2c9f8dcd7b3','\xcf354603a9aebd2ff9f33e1b04246d8ea204ae95','\x7f4a33dee068c4fa012d64677c61519a578dfa35','\x5746afd392b13946aacbda40317751db27d8b918','\x07f589ea6b789249c83992dd1ed324c3b80fd06b','\x28b0379d98fb80da460c190c95f97c74302214b1')
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
) tokens
--Exclude mints and burns by the Balancer contract
where "sender" not in ('\x0000000000000000000000000000000000000000', '\xba12222222228d8ba445958a75a0704d566bf2c8') 
and "recipient" not in ('\x0000000000000000000000000000000000000000', '\xba12222222228d8ba445958a75a0704d566bf2c8')
and "tokens_transferred" > 0
)

select block_time,block_number,contract_address::varchar,sender::varchar,recipient::varchar,tokens_transferred,tx_hash::varchar,"index",tx_index from transfers_data
with transfers_data as (
select * from
(
select block_time, block_number, 'ePyvcrvSTETH-15OCT21' as token, substring(topic2, 13, 20) as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x26941c63f4587796abe199348ecd3d7c44f9ae0c' --ePyvcrvSTETH-15OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvcrvSTETH-15OCT21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x94046274b5aa816ab236a9eab42b5563b56e1931' --eYyvcrvSTETH-15OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvcrvSTETH-28JAN22' as token, substring(topic2, 13, 20) as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x720465a4ae6547348056885060eeb51f9cadb571' --ePyvcrvSTETH-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvcrvSTETH-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xaf5d6d2e724f43769fa9e44284f0433a8f5be973' --eYyvcrvSTETH-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvcrvSTETH-15APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x2361102893ccabfb543bc55ac4cc8d6d0824a67e' --ePyvcrvSTETH-15APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvcrvSTETH-15APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xeb1a6c6ea0cd20847150c27b5985fa198b2f90bd' --eYyvcrvSTETH-15APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvCurve-EURS-11FEB22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x2a8f5649de50462ff9699ccc75a2fb0b53447503' --ePyvCurve-EURS-11FEB22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvCurve-EURS-11FEB22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x1ac5d65a987d448b0ecfe7e39017c3ec516d1d87' --eYyvCurve-EURS-11FEB22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvCurveLUSD-28SEP21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x9b44ed798a10df31dee52c5256dcb4754bcf097e' --ePyvCurveLUSD-28SEP21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvCurveLUSD-28SEP21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xbabd64a87881d8df7680907fcde176ff11fa0292' --eYyvCurveLUSD-28SEP21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvCurveLUSD-27DEC21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xa2b3d083aa1eaa8453bfb477f062a208ed85cbbf' --ePyvCurveLUSD-27DEC21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvCurveLUSD-27DEC21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xba8c8b50ecd5b580f464f7611b8549ffee4d8da2' --eYyvCurveLUSD-27DEC21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvCurveLUSD-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x0740a6cfb9468b8b53070c0b327099293dccb82d' --ePyvCurveLUSD-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvCurveLUSD-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x594b1aba4ed1ecc32a012f85527415a470a5352a' --eYyvCurveLUSD-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvCrvTriCrypto-15AUG21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x237535da7e2f0aba1b68262abcf7c4e60b42600c' --ePyvCrvTriCrypto-15AUG21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvCrvTriCrypto-15AUG21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xc080f19d9e7ccb6ef2096dfa08570e0057490940' --eYyvCrvTriCrypto-15AUG21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvcrv3crypto-12NOV21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x9cf2ab51ac93711ec2fa32ec861349568a16c729' --ePyvcrv3crypto-12NOV21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvcrv3crypto-12NOV21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x4f4500b3885bc72199373abfe7adefd0366bafed' --eYyvcrv3crypto-12NOV21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvcrv3crypto-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x285328906d0d33cb757c1e471f5e2176683247c2' --ePyvcrv3crypto-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvcrv3crypto-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x939fd8bfcfed01ec51f86df105821e3c5dc53c1c' --eYyvcrv3crypto-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvDAI-16OCT21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xb1cc77e701de60fe246607565cf7edc9d9b6b963' --ePyvDAI-16OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvDAI-16OCT21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xa1cc9bbcd3731a9fd43e1f1416f9b6bf824f37d7' --eYyvDAI-16OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvDAI-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x449d7c2e096e9f867339078535b15440d42f78e8' --ePyvDAI-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvDAI-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xf6d2699b035fc8fd5e023d4a6da216112ad8a985' --eYyvDAI-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvDAI-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x2c72692e94e757679289ac85d3556b2c0f717e0e' --ePyvDAI-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvDAI-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x38c9728e474a73bccf82705e29de4ca41852b8c8' --eYyvDAI-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvUSDC-29OCT21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xf38c3e836be9cd35072055ff6a9ba570e0b70797' --ePyvUSDC-29OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvUSDC-29OCT21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x5d67c1c829ab93867d865cf2008deb45df67044f' --eYyvUSDC-29OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvUSDC-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x8a2228705ec979961f0e16df311debcf097a2766' --ePyvUSDC-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvUSDC-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xf1294e805b992320a3515682c6ab0fe6251067e5' --eYyvUSDC-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvUSDC-17DEC21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x76a34d72b9cf97d972fb0e390eb053a37f211c74' --ePyvUSDC-17DEC21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvUSDC-17DEC21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x33dde19c163cdcce4e5a81f04a2af561b9ef58d7' --eYyvUSDC-17DEC21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvUSDC-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x52c9886d5d87b0f06ebacbeff750b5ffad5d17d9' --ePyvUSDC-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvUSDC-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x29cca1dba3f2db3c2708608d2676ff8044c14073' --eYyvUSDC-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvCurve-alUSD-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x55096a35bf827919b3bb0a5e6b5e2af8095f3d4d' --ePyvCurve-alUSD-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvCurve-alUSD-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x782be9330969aa7b9db56382752a1364580f199f' --eYyvCurve-alUSD-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvCurve-alUSD-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xeaa1cba8cc3cf01a92e9e853e90277b5b8a23e07' --ePyvCurve-alUSD-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvCurve-alUSD-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x394442cd20208c9bfdc6535d5d89bb932a05ea87' --eYyvCurve-alUSD-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvCurve-MIM-11FEB22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x418de6227499181b045cadf554030722e460881a' --ePyvCurve-MIM-11FEB22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvCurve-MIM-11FEB22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x8c981f68015d8eb13883bfd25aaf4b7c05ec7df5' --eYyvCurve-MIM-11FEB22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvCurve-MIM-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xc63958d9d01efa6b8266b1df3862c6323cbdb52b' --ePyvCurve-MIM-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvCurve-MIM-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x83c32857df72019bc71264ea8e3e06c3031641a2' --eYyvCurve-MIM-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvWBTC-26NOV21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x6bf924137e769c0a5c443dce6ec885552d31d579' --ePyvWBTC-26NOV21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvWBTC-26NOV21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x3b32f63c1e0fb810f0a06814ead1d4431b237560' --eYyvWBTC-26NOV21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'ePyvWBTC-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x49e9e169f0b661ea0a883f490564f4cc275123ed' --ePyvWBTC-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'eYyvWBTC-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x6b25b806a48b0d7cfa8399e3537479ddde7c931f' --eYyvWBTC-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvcrvSTETH-15OCT21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xce16E7ed7654a3453E8FaF748f2c82E57069278f' --LPePyvcrvSTETH-15OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvcrvSTETH-15OCT21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xD5D7bc115B32ad1449C6D0083E43C87be95F2809' --LPeYyvcrvSTETH-15OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvcrvSTETH-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x544c823194218f0640daE8291c1f59752d25faE3' --LPePyvcrvSTETH-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvcrvSTETH-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x4212bE3C7b255bA4B29705573ABD023cdcE21542' --LPeYyvcrvSTETH-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvcrvSTETH-15APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xb03C6B351A283bc1Cd26b9cf6d7B0c4556013bDb' --LPePyvcrvSTETH-15APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvcrvSTETH-15APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x062F38735AAC32320DB5e2DBBEb07968351D7C72' --LPeYyvcrvSTETH-15APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvCurveLUSD-28SEP21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xA8D4433BAdAa1A35506804B43657B0694deA928d' --LPePyvCurveLUSD-28SEP21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvCurveLUSD-28SEP21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xDe620bb8BE43ee54d7aa73f8E99A7409Fe511084' --LPeYyvCurveLUSD-28SEP21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvCurveLUSD-27DEC21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x893B30574BF183d69413717f30b17062eC9DFD8b' --LPePyvCurveLUSD-27DEC21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvCurveLUSD-27DEC21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x67F8FCb9D3c463da05DE1392EfDbB2A87F8599Ea' --LPeYyvCurveLUSD-27DEC21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvCurveLUSD-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x56F30398d13F111401d6e7ffE758254a0946687d' --LPePyvCurveLUSD-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvCurveLUSD-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x8E9d636BbE6939BD0F52849afc02C0c66F6A3603' --LPeYyvCurveLUSD-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvCrvTriCrypto-15AUG21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x3A693EB97b500008d4Bb6258906f7Bbca1D09Cc5' --LPePyvCrvTriCrypto-15AUG21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvCrvTriCrypto-15AUG21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xF94A7Df264A2ec8bCEef2cFE54d7cA3f6C6DFC7a' --LPeYyvCrvTriCrypto-15AUG21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvDAI-16OCT21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x71628c66C502F988Fbb9e17081F2bD14e361FAF4' --LPePyvDAI-16OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvDAI-16OCT21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xE54B3F5c444a801e61BECDCa93e74CdC1C4C1F90' --LPeYyvDAI-16OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvDAI-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xA47D1251CF21AD42685Cc6B8B3a186a73Dbd06cf' --LPePyvDAI-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvDAI-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xB70c25D96EF260eA07F650037Bf68F5d6583885e' --LPeYyvDAI-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvDAI-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xedf085f65b4f6c155e13155502ef925c9a756003' --LPePyvDAI-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvDAI-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x415747ee98d482e6dd9b431fa76ad5553744f247' --LPeYyvDAI-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvUSDC-29OCT21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x787546Bf2c05e3e19e2b6BDE57A203da7f682efF' --LPePyvUSDC-29OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvUSDC-29OCT21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x2D6e3515C8b47192Ca3913770fa741d3C4Dac354' --LPeYyvUSDC-29OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvUSDC-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x10a2F8bd81Ee2898D7eD18fb8f114034a549FA59' --LPePyvUSDC-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvUSDC-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x9e030b67a8384cbba09D5927533Aa98010C87d91' --LPeYyvUSDC-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvUSDC-17DEC21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x90ca5cef5b29342b229fb8ae2db5d8f4f894d652' --LPePyvUSDC-17DEC21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvUSDC-17DEC21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x7C9cF12d783821d5C63d8E9427aF5C44bAd92445' --LPeYyvUSDC-17DEC21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvUSDC-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x7edde0cb05ed19e03a9a47cd5e53fc57fde1c80c' --LPePyvUSDC-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvUSDC-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x7173b184525fead2ffbde5fbe6fcb65ea8246ee7' --LPeYyvUSDC-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvcrv3crypto-12NOV21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xF6dc4640D2783654BeF88E0dF3fb0F051f0DfC1A' --LPePyvcrv3crypto-12NOV21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvcrv3crypto-12NOV21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xd16847480D6bc218048CD31Ad98b63CC34e5c2bF' --LPeYyvcrv3crypto-12NOV21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvcrv3crypto-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x6dd0f7c8f4793ed2531c0df4fea8633a21fdcff4' --LPePyvcrv3crypto-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvcrv3crypto-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x4abb6fd289fa70056cfcb58cebab8689921eb922' --LPeYyvcrv3crypto-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvWBTC-26NOV21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x4Db9024fc9F477134e00Da0DA3c77DE98d9836aC' --LPePyvWBTC-26NOV21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvWBTC-26NOV21' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x7320d680Ca9BCE8048a286f00A79A2c9f8DCD7b3' --LPeYyvWBTC-26NOV21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvWBTC-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x4bd6d86debdb9f5413e631ad386c4427dc9d01b2' --LPePyvWBTC-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvWBTC-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xcf354603a9aebd2ff9f33e1b04246d8ea204ae95' --LPeYyvWBTC-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvCurve-alUSD-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\xC9AD279994980F8DF348b526901006972509677F' --LPePyvCurve-alUSD-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvCurve-alUSD-28JAN22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x802d0f2f4b5f1fb5BfC9b2040a703c1464e1D4CB' --LPeYyvCurve-alUSD-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvCurve-alUSD-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x63E9B50DD3eB63BfBF93B26F57b9EFB574e59576' --LPePyvCurve-alUSD-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvCurve-alUSD-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x10f21C9bD8128a29Aa785Ab2dE0d044DCdd79436' --LPeYyvCurve-alUSD-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvCurve-MIM-11FEB22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x09b1b33BaD0e87454ff05696b1151BFbD208a43F' --LPePyvCurve-MIM-11FEB22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvCurve-MIM-11FEB22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x1D310a6238e11c8BE91D83193C88A99eB66279bE' --LPeYyvCurve-MIM-11FEB22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvCurve-MIM-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x14792d3f6fcf2661795d1e08ef818bf612708bbf' --LPePyvCurve-MIM-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvCurve-MIM-29APR22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x6fe95fafe2f86158c77bf18350672d360bfc78a2' --LPeYyvCurve-MIM-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPePyvCurve-EURS-11FEB22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x6AC02eCD0c2A23B11f9AFb3b3Aaf237169475cac' --LPePyvCurve-EURS-11FEB22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
union all
select block_time, block_number, 'LPeYyvCurve-EURS-11FEB22' as token, substring(topic2, 13, 20)  as "sender", substring(topic3, 13, 20) as "recipient",
bytea2numericpy(substring(data FROM 1 FOR 32))/10^18 as tokens_transferred, tx_hash, index, tx_index from ethereum.logs
where contract_address = '\x5fA3ce1fB47bC8A29B5C02e2e7167799BBAf5F41' --LPeYyvCurve-EURS-11FEB22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
) tokens
--Exclude mints and burns by the Balancer contract
where "sender" not in ('\x0000000000000000000000000000000000000000', '\xba12222222228d8ba445958a75a0704d566bf2c8') 
and "recipient" not in ('\x0000000000000000000000000000000000000000', '\xba12222222228d8ba445958a75a0704d566bf2c8')
)

select block_time,block_number,token,sender::varchar,recipient::varchar,tokens_transferred,tx_hash::varchar,"index",tx_index from transfers_data
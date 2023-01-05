with day_series as (
SELECT generate_series('2021-06-28'::TIMESTAMP, date_trunc('day', NOW()), '1 day') AS day
),

steth_lp_prices as (
select day as date_trunc, first_value(steth_lp_token) over (partition by grp order by steth_lp_token desc nulls last) as steth_lp_token from
(
select day, steth_lp_token, sum(case when steth_lp_token is not null then 1 end) over (order by day) as grp from day_series
left join
(
select date_trunc('day', block_time), PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY lp_token_price) as "steth_lp_token" from (
--stETH AddLiquidity events
select evt_block_time as block_time, el.evt_tx_hash as tx_hash, token_amounts[1]/10^18 as eth_deposits,
token_amounts[2]/10^18 as steth_deposits, p.price as eth_price, minting.lp_tokens,
(token_amounts[1]/10^18 + token_amounts[2]/10^18)*p.price/minting.lp_tokens as lp_token_price
from curvefi."steth_swap_evt_AddLiquidity" el
left join
(
select evt_tx_hash as tx_hash, _value/10^18 as lp_tokens from curvefi."steth_evt_Transfer"
where "_from" = '\x0000000000000000000000000000000000000000'
and "_value" <> 0
and date_trunc('day', evt_block_time) >= '2021-06-28'
) minting
on minting.tx_hash = el.evt_tx_hash
LEFT JOIN (select * from prices."usd" where symbol = 'WETH' and date_trunc('day', minute) >= '2021-06-28') p
ON p.minute = date_trunc('minute', el.evt_block_time)
where date_trunc('day', evt_block_time) >= '2021-06-28'
order by 1 desc
) steth
group by 1
order by 1 desc
) prices
on day_series.day = prices.date_trunc
) corrected
order by 1 asc
),

lusd_lp_prices as (
select day as date_trunc, first_value(lusd_lp_token) over (partition by grp order by lusd_lp_token desc nulls last) as lusd_lp_token from
(
select day, lusd_lp_token, sum(case when lusd_lp_token is not null then 1 end) over (order by day) as grp from day_series
left join
(
select date_trunc('day', block_time), PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY lp_token_price) as "lusd_lp_token" from (
--LUSD AddLiquidity events
select evt_block_time as block_time, el.evt_tx_hash as tx_hash, token_amounts[1]/10^18 as lusd_deposits,
token_amounts[2]/10^18 as crv3_deposits, 1 as stable_price, minting.lp_tokens,
(token_amounts[1]/10^18 + token_amounts[2]/10^18)*1/minting.lp_tokens as lp_token_price
from curvefi."lusd_swap_evt_AddLiquidity" el
left join
(
select evt_tx_hash as tx_hash, value/10^18 as lp_tokens from curvefi."lusd_swap_evt_Transfer"
where "sender" = '\x0000000000000000000000000000000000000000'
and "value" <> 0
and date_trunc('day', evt_block_time) >= '2021-06-28'
) minting
on minting.tx_hash = el.evt_tx_hash
where date_trunc('day', evt_block_time) >= '2021-06-28'
order by 1 desc
) lusd
group by 1
order by 1 desc
) prices
on day_series.day = prices.date_trunc
) corrected
order by 1 asc
),

eurs_lp_prices as (
select day as date_trunc, first_value(eurs_lp_token) over (partition by grp order by eurs_lp_token desc nulls last) as eurs_lp_token from
(
select day, eurs_lp_token, sum(case when eurs_lp_token is not null then 1 end) over (order by day) as grp from day_series
left join
(
select date_trunc('day', block_time), PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY lp_token_price) as "eurs_lp_token" from (
--EURS AddLiquidity events
select evt_block_time as block_time, el.evt_tx_hash as tx_hash, token_amounts[1]/10^2 as eurs_deposits,
token_amounts[2]/10^18 as seur_deposits, p.price as eurs_price, minting.lp_tokens,
(token_amounts[1]/10^2 + token_amounts[2]/10^18)*p.price/minting.lp_tokens as lp_token_price
from curvefi."eurs_swap_evt_AddLiquidity" el
left join
(
select evt_tx_hash as tx_hash, _value/10^18 as lp_tokens from curvefi."eurs_evt_Transfer"
where "_from" = '\x0000000000000000000000000000000000000000'
and "_value" <> 0
and date_trunc('day', evt_block_time) >= '2021-06-28'
) minting
on minting.tx_hash = el.evt_tx_hash
LEFT JOIN (select * from prices."usd" where symbol = 'EURS' and date_trunc('day', minute) >= '2021-06-28') p
ON p.minute = date_trunc('minute', el.evt_block_time)
where date_trunc('day', evt_block_time) >= '2021-06-28'
order by 1 desc
) eurs
group by 1
order by 1 desc
) prices
on day_series.day = prices.date_trunc
) corrected
order by 1 asc
),

alusd_lp_prices as (
select day as date_trunc, first_value(alusd_lp_token) over (partition by grp order by alusd_lp_token desc nulls last) as alusd_lp_token from
(
select day, alusd_lp_token, sum(case when alusd_lp_token is not null then 1 end) over (order by day) as grp from day_series
left join
(
select date_trunc('day', block_time), PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY lp_token_price) as "alusd_lp_token" from (
--ALUSD AddLiquidity events
select evt_block_time as block_time, el.evt_tx_hash as tx_hash, token_amounts[1]/10^18 as alusd_deposits,
token_amounts[2]/10^18 as crv3_deposits, 1 as stable_price, minting.lp_tokens,
(token_amounts[1]/10^18 + token_amounts[2]/10^18)*1/minting.lp_tokens as lp_token_price
from curvefi."alusd_evt_AddLiquidity" el
left join
(
select evt_tx_hash as tx_hash, value/10^18 as lp_tokens from curvefi."alusd_evt_Transfer"
where "sender" = '\x0000000000000000000000000000000000000000'
and "value" <> 0
and date_trunc('day', evt_block_time) >= '2021-06-28'
) minting
on minting.tx_hash = el.evt_tx_hash
where date_trunc('day', evt_block_time) >= '2021-06-28'
order by 1 desc
) alusd
group by 1
order by 1 desc
) prices
on day_series.day = prices.date_trunc
) corrected
order by 1 asc
),

mim_lp_prices as (
select day as date_trunc, first_value(mim_lp_token) over (partition by grp order by mim_lp_token desc nulls last) as mim_lp_token from
(
select day, mim_lp_token, sum(case when mim_lp_token is not null then 1 end) over (order by day) as grp from day_series
left join
(
select date_trunc('day', block_time), PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY lp_token_price) as "mim_lp_token" from (
--MIM AddLiquidity events
select evt_block_time as block_time, el.evt_tx_hash as tx_hash, token_amounts[1]/10^18 as mim_deposits,
token_amounts[2]/10^18 as crv3_deposits, 1 as stable_price, minting.lp_tokens,
(token_amounts[1]/10^18 + token_amounts[2]/10^18)*1/minting.lp_tokens as lp_token_price
from curvefi."mim_evt_AddLiquidity" el
left join
(
select evt_tx_hash as tx_hash, value/10^18 as lp_tokens from curvefi."mim_evt_Transfer"
where "sender" = '\x0000000000000000000000000000000000000000'
and "value" <> 0
and date_trunc('day', evt_block_time) >= '2021-06-28'
) minting
on minting.tx_hash = el.evt_tx_hash
where date_trunc('day', evt_block_time) >= '2021-06-28'
order by 1 desc
) mim
group by 1
order by 1 desc
) prices
on day_series.day = prices.date_trunc
) corrected
order by 1 asc
),

tricrypto_lp_prices as (
select day as date_trunc, first_value(tricrypto_lp_token) over (partition by grp order by tricrypto_lp_token desc nulls last) as tricrypto_lp_token from
(
select day, tricrypto_lp_token, sum(case when tricrypto_lp_token is not null then 1 end) over (order by day) as grp from day_series
left join
(
select date_trunc('day', block_time), PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY lp_token_price) as "tricrypto_lp_token" from (
--tricrypto AddLiquidity events
select evt_block_time as block_time, el.evt_tx_hash as tx_hash, token_amounts[1]/10^6 as tether_deposits,
token_amounts[2]/10^8 as wbtc_deposits, token_amounts[3]/10^18 as weth_deposits,
peth.price as eth_price, pbtc.price as btc_price, 1 as tether_price, minting.lp_tokens,
(token_amounts[1]/10^6*1 + token_amounts[2]/10^8*pbtc.price + token_amounts[3]/10^18*peth.price)/minting.lp_tokens as lp_token_price
from curvefi."tricrypto_swap_evt_AddLiquidity" el
left join
(
select evt_tx_hash as tx_hash, _value/10^18 as lp_tokens from curvefi."tricrypto_evt_Transfer"
where "_from" = '\x0000000000000000000000000000000000000000'
and "_value" <> 0
and date_trunc('day', evt_block_time) >= '2021-06-28'
) minting
on minting.tx_hash = el.evt_tx_hash
LEFT JOIN (select * from prices."usd" where symbol = 'WETH' and date_trunc('day', minute) >= '2021-06-28') peth
ON peth.minute = date_trunc('minute', el.evt_block_time)
LEFT JOIN (select * from prices."usd" where symbol = 'WBTC' and date_trunc('day', minute) >= '2021-06-28') pbtc
ON pbtc.minute = date_trunc('minute', el.evt_block_time)
where date_trunc('day', evt_block_time) >= '2021-06-28'
order by 1 desc
) tricrypto
group by 1
order by 1 desc
) prices
on day_series.day = prices.date_trunc
) corrected
order by 1 asc
),

tricrypto2_lp_prices as (
select day as date_trunc, first_value(tricrypto2_lp_token) over (partition by grp order by tricrypto2_lp_token desc nulls last) as tricrypto2_lp_token from
(
select day, tricrypto2_lp_token, sum(case when tricrypto2_lp_token is not null then 1 end) over (order by day) as grp from day_series
left join
(
select date_trunc('day', block_time), PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY lp_token_price) as "tricrypto2_lp_token" from (
--tricrypto2 AddLiquidity events
select evt_block_time as block_time, el.evt_tx_hash as tx_hash, token_amounts[1]/10^6 as tether_deposits,
token_amounts[2]/10^8 as wbtc_deposits, token_amounts[3]/10^18 as weth_deposits,
peth.price as eth_price, pbtc.price as btc_price, 1 as tether_price, minting.lp_tokens,
(token_amounts[1]/10^6*1 + token_amounts[2]/10^8*pbtc.price + token_amounts[3]/10^18*peth.price)/minting.lp_tokens as lp_token_price
from curvefi."tricrypto2_swap_evt_AddLiquidity" el
left join
(
select evt_tx_hash as tx_hash, _value/10^18 as lp_tokens from curvefi."tricrypto2_evt_Transfer"
where "_from" = '\x0000000000000000000000000000000000000000'
and "_value" <> 0
and date_trunc('day', evt_block_time) >= '2021-06-28'
) minting
on minting.tx_hash = el.evt_tx_hash
LEFT JOIN (select * from prices."usd" where symbol = 'WETH' and date_trunc('day', minute) >= '2021-06-28') peth
ON peth.minute = date_trunc('minute', el.evt_block_time)
LEFT JOIN (select * from prices."usd" where symbol = 'WBTC' and date_trunc('day', minute) >= '2021-06-28') pbtc
ON pbtc.minute = date_trunc('minute', el.evt_block_time)
where date_trunc('day', evt_block_time) >= '2021-06-28'
order by 1 desc
) tricrypto2
group by 1
order by 1 desc
) prices
on day_series.day = prices.date_trunc
) corrected
order by 1 asc
),

dai_prices as (
select minute, avg(price) as "dai_price" from prices.usd
where symbol = 'DAI'
and date_trunc('day', minute) >= '2021-06-28'
group by 1
order by 1 desc
),

usdc_prices as (
select minute, avg(price) as "usdc_price" from prices.usd
where symbol = 'USDC'
and date_trunc('day', minute) >= '2021-06-28'
group by 1
order by 1 desc
),

wbtc_prices as (
select minute, avg(price) as "wbtc_price" from prices.usd
where symbol = 'WBTC'
and date_trunc('day', minute) >= '2021-06-28'
group by 1
order by 1 desc
),

bbausd_lp_prices as (
select day as date_trunc, first_value(bbausd_lp_token) over (partition by grp order by bbausd_lp_token desc nulls last) as bbausd_lp_token from
( -- START corrected)
select day, bbausd_lp_token, sum(case when bbausd_lp_token is not null then 1 end) over (order by day) as grp from day_series
left join
( -- START prices, calculate 50th percentile per day
select date_trunc('day', block_time), PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY lp_token_price) as "bbausd_lp_token" from (
--BBAUSD AddLiquidity events
select el.evt_block_time as block_time, el.evt_tx_hash as tx_hash, 1 as stable_price, minting.lp_tokens,
(deltas[1]/10^18 + deltas[2]/10^18)*1/minting.lp_tokens as lp_token_price
from balancer_v2."Vault_evt_PoolBalanceChanged" el
left join
(
select *, value/10^18 as lp_tokens from erc20."ERC20_evt_Transfer"
where "from" = '\x0000000000000000000000000000000000000000'
and "value" <> 0
and date_trunc('day', evt_block_time) >= '2021-06-28'
and "contract_address" = '\x28b0379d98fb80da460c190c95f97c74302214b1'
) minting
on minting.evt_tx_hash = el.evt_tx_hash
where date_trunc('day', el.evt_block_time) >= '2021-06-28'
order by 1 desc
) bbausd
group by 1
order by 1 desc
) prices
on day_series.day = prices.date_trunc
) corrected
order by 1 asc
),


--All Element Add/Remove Liquidity Events

liquidity_data as (
--Dai Pools (ePyvDAI-16OCT21,eYyvDAI-16OCT21,ePyvDAI-28JAN22,eYyvDAI-28JAN22,ePyvDAI-29APR22,eYyvDAI-29APR22)
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvDAI-16OCT21' as e_asset, 'LPePyvDAI-16OCT21' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*dai_prices.dai_price as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x71628c66C502F988Fbb9e17081F2bD14e361FAF4' --LPePyvDAI-16OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join dai_prices
on dai_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x71628c66c502f988fbb9e17081f2bd14e361faf4000200000000000000000078'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvDAI-16OCT21' as e_asset, 'LPeYyvDAI-16OCT21' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*dai_prices.dai_price as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xE54B3F5c444a801e61BECDCa93e74CdC1C4C1F90' --LPeYyvDAI-16OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join dai_prices
on dai_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xe54b3f5c444a801e61becdca93e74cdc1c4c1f90000200000000000000000077'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvDAI-28JAN22' as e_asset, 'LPePyvDAI-28JAN22' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*dai_prices.dai_price as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xA47D1251CF21AD42685Cc6B8B3a186a73Dbd06cf' --LPePyvDAI-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join dai_prices
on dai_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xa47d1251cf21ad42685cc6b8b3a186a73dbd06cf000200000000000000000097'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvDAI-28JAN22' as e_asset, 'LPeYyvDAI-28JAN22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*dai_prices.dai_price as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xB70c25D96EF260eA07F650037Bf68F5d6583885e' --LPeYyvDAI-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join dai_prices
on dai_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xb70c25d96ef260ea07f650037bf68f5d6583885e000200000000000000000096'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvDAI-29APR22' as e_asset, 'LPePyvDAI-29APR22' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*dai_prices.dai_price as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xedf085f65b4f6c155e13155502ef925c9a756003' --LPePyvDAI-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join dai_prices
on dai_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xedf085f65b4f6c155e13155502ef925c9a756003000200000000000000000123'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvDAI-29APR22' as e_asset, 'LPeYyvDAI-29APR22' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*dai_prices.dai_price as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x415747ee98d482e6dd9b431fa76ad5553744f247' --LPeYyvDAI-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join dai_prices
on dai_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x415747ee98d482e6dd9b431fa76ad5553744f247000200000000000000000122'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvDAI-16SEP22' as e_asset, 'LPePyvDAI-16SEP22' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*dai_prices.dai_price as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x8ffd1dc7c3ef65f833cf84dbcd15b6ad7f9c54ec' --LPePyvDAI-16SEP22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join dai_prices
on dai_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x8ffd1dc7c3ef65f833cf84dbcd15b6ad7f9c54ec000200000000000000000199'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvDAI-24FEB23' as e_asset, 'LPePyvDAI-24FEB23' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*dai_prices.dai_price as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x7f4a33dee068c4fa012d64677c61519a578dfa35' --LPePyvDAI-24FEB23
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join dai_prices
on dai_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x7f4a33dee068c4fa012d64677c61519a578dfa35000200000000000000000346'
and deltas[2] <> 0
union all
--USDC Pools (ePyvUSDC-29OCT21,eYyvUSDC-29OCT21,ePyvUSDC-28JAN22,eYyvUSDC-28JAN22,ePyvUSDC-29APR22,eYyvUSDC-29APR22,ePyvUSDC-16SEP22)
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvUSDC-29OCT21' as e_asset, 'LPePyvUSDC-29OCT21' as lp_token,
deltas[1]/10^6 as deposit_size_base, deltas[1]/10^6*usdc_prices.usdc_price as deposit_size_base_usd, deltas[2]/10^6 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x787546Bf2c05e3e19e2b6BDE57A203da7f682efF' --LPePyvUSDC-29OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join usdc_prices
on usdc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x787546bf2c05e3e19e2b6bde57a203da7f682eff00020000000000000000007c'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvUSDC-29OCT21' as e_asset, 'LPeYyvUSDC-29OCT21' as lp_token,
deltas[2]/10^6 as deposit_size_base, deltas[2]/10^6*usdc_prices.usdc_price as deposit_size_base_usd, deltas[1]/10^6 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x2D6e3515C8b47192Ca3913770fa741d3C4Dac354' --LPeYyvUSDC-29OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join usdc_prices
on usdc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x2d6e3515c8b47192ca3913770fa741d3c4dac35400020000000000000000007b'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvUSDC-28JAN22' as e_asset, 'LPePyvUSDC-28JAN22' as lp_token,
deltas[2]/10^6 as deposit_size_base, deltas[2]/10^6*usdc_prices.usdc_price as deposit_size_base_usd, deltas[1]/10^6 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x10a2F8bd81Ee2898D7eD18fb8f114034a549FA59' --LPePyvUSDC-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join usdc_prices
on usdc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x10a2f8bd81ee2898d7ed18fb8f114034a549fa59000200000000000000000090'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvUSDC-28JAN22' as e_asset, 'LPeYyvUSDC-28JAN22' as lp_token,
deltas[1]/10^6 as deposit_size_base, deltas[1]/10^6*usdc_prices.usdc_price as deposit_size_base_usd, deltas[2]/10^6 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x9e030b67a8384cbba09D5927533Aa98010C87d91' --PeYyvUSDC-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join usdc_prices
on usdc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x9e030b67a8384cbba09d5927533aa98010c87d9100020000000000000000008f'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvUSDC-17DEC21' as e_asset, 'LPePyvUSDC-17DEC21' as lp_token,
deltas[2]/10^6 as deposit_size_base, deltas[2]/10^6*usdc_prices.usdc_price as deposit_size_base_usd, deltas[1]/10^6 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x90ca5cef5b29342b229fb8ae2db5d8f4f894d652' --LPePyvUSDC-17DEC21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join usdc_prices
on usdc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x90ca5cef5b29342b229fb8ae2db5d8f4f894d6520002000000000000000000b5'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvUSDC-17DEC21' as e_asset, 'LPeYyvUSDC-17DEC21' as lp_token,
deltas[2]/10^6 as deposit_size_base, deltas[2]/10^6*usdc_prices.usdc_price as deposit_size_base_usd, deltas[1]/10^6 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x7C9cF12d783821d5C63d8E9427aF5C44bAd92445' --LPeYyvUSDC-17DEC21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join usdc_prices
on usdc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x7c9cf12d783821d5c63d8e9427af5c44bad924450002000000000000000000b4'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvUSDC-29APR22' as e_asset, 'LPePyvUSDC-29APR22' as lp_token,
deltas[2]/10^6 as deposit_size_base, deltas[2]/10^6*usdc_prices.usdc_price as deposit_size_base_usd, deltas[1]/10^6 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x7edde0cb05ed19e03a9a47cd5e53fc57fde1c80c' --LPePyvUSDC-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join usdc_prices
on usdc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x7edde0cb05ed19e03a9a47cd5e53fc57fde1c80c0002000000000000000000c8'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvUSDC-29APR22' as e_asset, 'LPeYyvUSDC-29APR22' as lp_token,
deltas[2]/10^6 as deposit_size_base, deltas[2]/10^6*usdc_prices.usdc_price as deposit_size_base_usd, deltas[1]/10^6 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x7173b184525feAD2fFbde5FBe6FCB65Ea8246eE7' --LPeYyvUSDC-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join usdc_prices
on usdc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x7173b184525fead2ffbde5fbe6fcb65ea8246ee70002000000000000000000c7'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvUSDC-16SEP22' as e_asset, 'LPePyvUSDC-16SEP22' as lp_token,
deltas[2]/10^6 as deposit_size_base, deltas[2]/10^6*usdc_prices.usdc_price as deposit_size_base_usd, deltas[1]/10^6 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x56df5ef1a0a86c2a5dd9cc001aa8152545bdbdec' --LPePyvUSDC-16SEP22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join usdc_prices
on usdc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x56df5ef1a0a86c2a5dd9cc001aa8152545bdbdec000200000000000000000168'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvUSDC-24FEB23' as e_asset, 'LPePyvUSDC-24FEB23' as lp_token,
deltas[2]/10^6 as deposit_size_base, deltas[2]/10^6*usdc_prices.usdc_price as deposit_size_base_usd, deltas[1]/10^6 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x5746afd392b13946aacbda40317751db27d8b918' --LPePyvUSDC-24FEB23
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join usdc_prices
on usdc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x5746afd392b13946aacbda40317751db27d8b91800020000000000000000034c'
and deltas[2] <> 0
union all
--WBTC Pools (ePyvWBTC-26NOV21,eYyvWBTC-26NOV21,ePyvWBTC-29APR22,eYyvWBTC-29APR22)
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvWBTC-26NOV21' as e_asset, 'LPePyvWBTC-26NOV21' as lp_token,
deltas[1]/10^8 as deposit_size_base, deltas[1]/10^8*wbtc_prices.wbtc_price as deposit_size_base_usd, deltas[2]/10^8 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x4Db9024fc9F477134e00Da0DA3c77DE98d9836aC' --LPePyvWBTC-26NOV21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join wbtc_prices
on wbtc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x4db9024fc9f477134e00da0da3c77de98d9836ac000200000000000000000086'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvWBTC-26NOV21' as e_asset, 'LPeYyvWBTC-26NOV21' as lp_token,
deltas[1]/10^8 as deposit_size_base, deltas[1]/10^8*wbtc_prices.wbtc_price as deposit_size_base_usd, deltas[2]/10^8 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x7320d680Ca9BCE8048a286f00A79A2c9f8DCD7b3' --LPeYyvWBTC-26NOV21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join wbtc_prices
on wbtc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x7320d680ca9bce8048a286f00a79a2c9f8dcd7b3000200000000000000000085'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvWBTC-29APR22' as e_asset, 'LPePyvWBTC-29APR22' as lp_token,
deltas[1]/10^8 as deposit_size_base, deltas[1]/10^8*wbtc_prices.wbtc_price as deposit_size_base_usd, deltas[2]/10^8 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x4bd6D86dEBdB9F5413e631Ad386c4427DC9D01B2' --LPePyvWBTC-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join wbtc_prices
on wbtc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x4bd6d86debdb9f5413e631ad386c4427dc9d01b20002000000000000000000ec'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvWBTC-29APR22' as e_asset, 'LPeYyvWBTC-29APR22' as lp_token,
deltas[1]/10^8 as deposit_size_base, deltas[1]/10^8*wbtc_prices.wbtc_price as deposit_size_base_usd, deltas[2]/10^8 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xcf354603a9aebd2ff9f33e1b04246d8ea204ae95' --LPeYyvWBTC-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join wbtc_prices
on wbtc_prices.minute = date_trunc('minute', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xcf354603a9aebd2ff9f33e1b04246d8ea204ae950002000000000000000000eb'
and deltas[1] <> 0
union all
--steCRV Pools (ePyvcrvSTETH-15OCT21,eYyvcrvSTETH-15OCT21,ePyvcrvSTETH-28JAN22,eYyvcrvSTETH-28JAN22,ePyvcrvSTETH-15APR22,eYyvcrvSTETH-15APR22)
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvcrvSTETH-15OCT21' as e_asset, 'LPePyvcrvSTETH-15OCT21' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*steth_lp_prices.steth_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xce16E7ed7654a3453E8FaF748f2c82E57069278f' --LPePyvcrvSTETH-15OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join steth_lp_prices
on steth_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xce16e7ed7654a3453e8faf748f2c82e57069278f00020000000000000000006d'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvcrvSTETH-15OCT21' as e_asset, 'LPeYyvcrvSTETH-15OCT21' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*steth_lp_prices.steth_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xD5D7bc115B32ad1449C6D0083E43C87be95F2809' --LPeYyvcrvSTETH-15OCT21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join steth_lp_prices
on steth_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xd5d7bc115b32ad1449c6d0083e43c87be95f280900020000000000000000006c'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvcrvSTETH-28JAN22' as e_asset, 'LPePyvcrvSTETH-28JAN22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*steth_lp_prices.steth_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x544c823194218f0640daE8291c1f59752d25faE3' --LPePyvcrvSTETH-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join steth_lp_prices
on steth_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x544c823194218f0640dae8291c1f59752d25fae3000200000000000000000093'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvcrvSTETH-28JAN22' as e_asset, 'LPeYyvcrvSTETH-28JAN22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*steth_lp_prices.steth_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x4212bE3C7b255bA4B29705573ABD023cdcE21542' --LPeYyvcrvSTETH-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join steth_lp_prices
on steth_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x4212be3c7b255ba4b29705573abd023cdce21542000200000000000000000092'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvcrvSTETH-15APR22' as e_asset, 'LPePyvcrvSTETH-15APR22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*steth_lp_prices.steth_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xb03C6B351A283bc1Cd26b9cf6d7B0c4556013bDb' --LPePyvcrvSTETH-15APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join steth_lp_prices
on steth_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xb03c6b351a283bc1cd26b9cf6d7b0c4556013bdb0002000000000000000000ab'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvcrvSTETH-15APR22' as e_asset, 'LPeYyvcrvSTETH-15APR22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*steth_lp_prices.steth_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x062F38735AAC32320DB5e2DBBEb07968351D7C72' --LPeYyvcrvSTETH-15APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join steth_lp_prices
on steth_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x062f38735aac32320db5e2dbbeb07968351d7c720002000000000000000000ac'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvcrvSTETH-16SEP22' as e_asset, 'LPePyvcrvSTETH-16SEP22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*steth_lp_prices.steth_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xabb93e3787b984cb62dcd962af8732f52ff57586' --LPePyvcrvSTETH-16SEP22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join steth_lp_prices
on steth_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xabb93e3787b984cb62dcd962af8732f52ff57586000200000000000000000183'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvcrvSTETH-24FEB23' as e_asset, 'LPePyvcrvSTETH-24FEB23' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*steth_lp_prices.steth_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x07f589ea6b789249c83992dd1ed324c3b80fd06b' --LPePyvcrvSTETH-24FEB23
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join steth_lp_prices
on steth_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x07f589ea6b789249c83992dd1ed324c3b80fd06b00020000000000000000034e'
and deltas[1] <> 0
union all
--lusd3crv-f Pools (ePyvCurveLUSD-28SEP21,eYyvCurveLUSD-28SEP21,ePyvCurveLUSD-27DEC21,eYyvCurveLUSD-27DEC21,ePyvCurveLUSD-29APR22,eYyvCurveLUSD-29APR22)
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvCurveLUSD-28SEP21' as e_asset, 'LPePyvCurveLUSD-28SEP21' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*lusd_lp_prices.lusd_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xA8D4433BAdAa1A35506804B43657B0694deA928d' --LPePyvCurveLUSD-28SEP21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join lusd_lp_prices
on lusd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xa8d4433badaa1a35506804b43657b0694dea928d00020000000000000000005e'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvCurveLUSD-28SEP21' as e_asset, 'LPeYyvCurveLUSD-28SEP21' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*lusd_lp_prices.lusd_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xDe620bb8BE43ee54d7aa73f8E99A7409Fe511084' --LPeYyvCurveLUSD-28SEP21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join lusd_lp_prices
on lusd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xde620bb8be43ee54d7aa73f8e99a7409fe51108400020000000000000000005d'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvCurveLUSD-27DEC21' as e_asset, 'LPePyvCurveLUSD-27DEC21' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*lusd_lp_prices.lusd_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x893B30574BF183d69413717f30b17062eC9DFD8b' --LPePyvCurveLUSD-27DEC21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join lusd_lp_prices
on lusd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x893b30574bf183d69413717f30b17062ec9dfd8b000200000000000000000061'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvCurveLUSD-27DEC21' as e_asset, 'LPeYyvCurveLUSD-27DEC21' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*lusd_lp_prices.lusd_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x67F8FCb9D3c463da05DE1392EfDbB2A87F8599Ea' --LPeYyvCurveLUSD-27DEC21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join lusd_lp_prices
on lusd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x67f8fcb9d3c463da05de1392efdbb2a87f8599ea000200000000000000000060'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvCurveLUSD-29APR22' as e_asset, 'LPePyvCurveLUSD-29APR22' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*lusd_lp_prices.lusd_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x56F30398d13F111401d6e7ffE758254a0946687d' --LPePyvCurveLUSD-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join lusd_lp_prices
on lusd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x56f30398d13f111401d6e7ffe758254a0946687d000200000000000000000105'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvCurveLUSD-29APR22' as e_asset, 'LPeYyvCurveLUSD-29APR22' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*lusd_lp_prices.lusd_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x8E9d636BbE6939BD0F52849afc02C0c66F6A3603' --LPeYyvCurveLUSD-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join lusd_lp_prices
on lusd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x8e9d636bbe6939bd0f52849afc02c0c66f6a3603000200000000000000000104'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvCurveLUSD-16SEP22' as e_asset, 'LPePyvCurveLUSD-16SEP22' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*lusd_lp_prices.lusd_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x489eedb33f82574afeabb3f4e156fbf662308ada' --LPePyvCurveLUSD-16SEP22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join lusd_lp_prices
on lusd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x489eedb33f82574afeabb3f4e156fbf662308ada0002000000000000000001a3'
and deltas[2] <> 0
union all
--alusd3crv-f Pools (ePyvCurve-alUSD-28JAN22,eYyvCurve-alUSD-28JAN22)
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvCurve-alUSD-28JAN22' as e_asset, 'LPePyvCurve-alUSD-28JAN22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*alusd_lp_prices.alusd_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xC9AD279994980F8DF348b526901006972509677F' --LPePyvCurve-alUSD-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join alusd_lp_prices
on alusd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xc9ad279994980f8df348b526901006972509677f00020000000000000000009e'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvCurve-alUSD-28JAN22' as e_asset, 'LPeYyvCurve-alUSD-28JAN22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*alusd_lp_prices.alusd_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x802d0f2f4b5f1fb5BfC9b2040a703c1464e1D4CB' --LPeYyvCurve-alUSD-28JAN22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join alusd_lp_prices
on alusd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x802d0f2f4b5f1fb5bfc9b2040a703c1464e1d4cb00020000000000000000009d'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvCurve-alUSD-29APR22' as e_asset, 'LPePyvCurve-alUSD-29APR22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*alusd_lp_prices.alusd_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x63E9B50DD3eB63BfBF93B26F57b9EFB574e59576' --LPePyvCurve-alUSD-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join alusd_lp_prices
on alusd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x63e9b50dd3eb63bfbf93b26f57b9efb574e595760002000000000000000000cf'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvCurve-alUSD-29APR22' as e_asset, 'LPeYyvCurve-alUSD-29APR22' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*alusd_lp_prices.alusd_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x10f21C9bD8128a29Aa785Ab2dE0d044DCdd79436' --LPeYyvCurve-alUSD-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join alusd_lp_prices
on alusd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x10f21c9bd8128a29aa785ab2de0d044dcdd794360002000000000000000000ce'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvCurve-alUSD-16SEP22' as e_asset, 'LPePyvCurve-alUSD-16SEP22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*alusd_lp_prices.alusd_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x13cf9e8115f35828a26062b6c05a56c72f54e0c6' --LPePyvCurve-alUSD-16SEP22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join alusd_lp_prices
on alusd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x13cf9e8115f35828a26062b6c05a56c72f54e0c60002000000000000000001d9'
and deltas[1] <> 0
union all
--mim-3lp3crv-f Pools (ePyvCurve-MIM-11FEB22,eYyvCurve-MIM-11FEB22,ePyvCurve-MIM-29APR22,eYyvCurve-MIM-29APR22)
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvCurve-MIM-11FEB22' as e_asset, 'LPePyvCurve-MIM-11FEB22' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*mim_lp_prices.mim_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x09b1b33BaD0e87454ff05696b1151BFbD208a43F' --LPePyvCurve-MIM-11FEB22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join mim_lp_prices
on mim_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x09b1b33bad0e87454ff05696b1151bfbd208a43f0002000000000000000000a6'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvCurve-MIM-11FEB22' as e_asset, 'LPeYyvCurve-MIM-11FEB22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*mim_lp_prices.mim_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x1D310a6238e11c8BE91D83193C88A99eB66279bE' --LPeYyvCurve-MIM-11FEB22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join mim_lp_prices
on mim_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x1d310a6238e11c8be91d83193c88a99eb66279be0002000000000000000000a2'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvCurve-MIM-29APR22' as e_asset, 'LPePyvCurve-MIM-29APR22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*mim_lp_prices.mim_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x14792d3F6FcF2661795d1E08ef818bf612708BbF' --LPePyvCurve-MIM-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join mim_lp_prices
on mim_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x14792d3f6fcf2661795d1e08ef818bf612708bbf0002000000000000000000be'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvCurve-MIM-29APR22' as e_asset, 'LPeYyvCurve-MIM-29APR22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*mim_lp_prices.mim_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x6FE95FafE2F86158c77Bf18350672D360BfC78a2' --LPeYyvCurve-MIM-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join mim_lp_prices
on mim_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x6fe95fafe2f86158c77bf18350672d360bfc78a20002000000000000000000bd'
and deltas[1] <> 0
union all
--eurscrv Pools (ePyvCurve-EURS-11FEB22,eYyvCurve-EURS-11FEB22)
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvCurve-EURS-11FEB22' as e_asset, 'LPePyvCurve-EURS-11FEB22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*eurs_lp_prices.eurs_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x6AC02eCD0c2A23B11f9AFb3b3Aaf237169475cac' --LPePyvCurve-EURS-11FEB22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join eurs_lp_prices
on eurs_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x6ac02ecd0c2a23b11f9afb3b3aaf237169475cac0002000000000000000000a8'
and deltas[1] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvCurve-EURS-11FEB22' as e_asset, 'LPeYyvCurve-EURS-11FEB22' as lp_token,
deltas[1]/10^18 as deposit_size_base, deltas[1]/10^18*eurs_lp_prices.eurs_lp_token as deposit_size_base_usd, deltas[2]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x5fA3ce1fB47bC8A29B5C02e2e7167799BBAf5F41' --LPeYyvCurve-EURS-11FEB22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join eurs_lp_prices
on eurs_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x5fa3ce1fb47bc8a29b5c02e2e7167799bbaf5f410002000000000000000000a7'
and deltas[1] <> 0
union all
--crvtricrypto Pools (ePyvCrvTriCrypto-15AUG21,eYyvCrvTriCrypto-15AUG21)
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvCrvTriCrypto-15AUG21' as e_asset, 'LPePyvCrvTriCrypto-15AUG21' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*tricrypto_lp_prices.tricrypto_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x3A693EB97b500008d4Bb6258906f7Bbca1D09Cc5' --LPePyvCrvTriCrypto-15AUG21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join tricrypto_lp_prices
on tricrypto_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x3a693eb97b500008d4bb6258906f7bbca1d09cc5000200000000000000000065'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvCrvTriCrypto-15AUG21' as e_asset, 'LPeYyvCrvTriCrypto-15AUG21' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*tricrypto_lp_prices.tricrypto_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xF94A7Df264A2ec8bCEef2cFE54d7cA3f6C6DFC7a' --LPeYyvCrvTriCrypto-15AUG21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join tricrypto_lp_prices
on tricrypto_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xf94a7df264a2ec8bceef2cfe54d7ca3f6c6dfc7a000200000000000000000064'
and deltas[2] <> 0
union all
--crv3crypto Pools (ePyvcrv3crypto-12NOV21,eYyvcrv3crypto-12NOV21,ePyvcrv3crypto-29APR22,eYyvcrv3crypto-29APR22)
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvcrv3crypto-12NOV21' as e_asset, 'LPePyvcrv3crypto-12NOV21' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*tricrypto2_lp_prices.tricrypto2_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xF6dc4640D2783654BeF88E0dF3fb0F051f0DfC1A' --LPePyvcrv3crypto-12NOV21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join tricrypto2_lp_prices
on tricrypto2_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xf6dc4640d2783654bef88e0df3fb0f051f0dfc1a00020000000000000000007e'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvcrv3crypto-12NOV21' as e_asset, 'LPeYyvcrv3crypto-12NOV21' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*tricrypto2_lp_prices.tricrypto2_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xd16847480D6bc218048CD31Ad98b63CC34e5c2bF' --LPeYyvcrv3crypto-12NOV21
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join tricrypto2_lp_prices
on tricrypto2_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xd16847480d6bc218048cd31ad98b63cc34e5c2bf00020000000000000000007d'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvcrv3crypto-29APR22' as e_asset, 'LPePyvcrv3crypto-29APR22' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*tricrypto2_lp_prices.tricrypto2_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x6Dd0F7c8F4793ed2531c0df4fEA8633a21fDcFf4' --LPePyvcrv3crypto-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join tricrypto2_lp_prices
on tricrypto2_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x6dd0f7c8f4793ed2531c0df4fea8633a21fdcff40002000000000000000000b7'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'eYyvcrv3crypto-29APR22' as e_asset, 'LPeYyvcrv3crypto-29APR22' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*tricrypto2_lp_prices.tricrypto2_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x4aBB6FD289fA70056CFcB58ceBab8689921eB922' --LPeYyvcrv3crypto-29APR22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join tricrypto2_lp_prices
on tricrypto2_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x4abb6fd289fa70056cfcb58cebab8689921eb9220002000000000000000000b6'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvcrv3crypto-16SEP22' as e_asset, 'LPePyvcrv3crypto-16SEP22' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*tricrypto2_lp_prices.tricrypto2_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\xc80fef22fccc277ac0ffea84d111888a767f717e' --LPePyvcrv3crypto-16SEP22
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b'
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000')
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join tricrypto2_lp_prices
on tricrypto2_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\xc80fef22fccc277ac0ffea84d111888a767f717e0002000000000000000001b1'
and deltas[2] <> 0
union all
select evt_block_time, evt_block_number, et."from" as liquidity_provider, bv."liquidityProvider" as liquidityProvider, 'ePyvBalancer-BoostedAaveUSD-04MAY23' as e_asset, 'LPePyvBalancer-BoostedAaveUSD-04MAY23' as lp_token,
deltas[2]/10^18 as deposit_size_base, deltas[2]/10^18*bbausd_lp_prices.bbausd_lp_token as deposit_size_base_usd, deltas[1]/10^18 as deposit_size_e_asset,
case when el."topic2" = '\x0000000000000000000000000000000000000000000000000000000000000000' then bytea2numericpy(substring(el.data FROM 1 FOR 32))/10^18 else bytea2numericpy(substring(el.data FROM 1 FOR 32))*-1/10^18 end as lp_tokens_acquired,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18 else (et.gas_price*et.gas_used)/10^18 end as tx_fee_eth,
case when et."type" = 'DynamicFee' then (eb.base_fee_per_gas+et.max_priority_fee_per_gas)*et.gas_used/10^18*peth.price else (et.gas_price*et.gas_used)/10^18*peth.price end as tx_fee_usd,
evt_tx_hash, et.nonce, bv.evt_index as "index", et.index as "tx_index"
from balancer_v2."Vault_evt_PoolBalanceChanged" bv
left join (
select * from ethereum.logs
where contract_address = '\x28b0379d98fb80da460c190c95f97c74302214b1' --LPePyvBalancer-BoostedAaveUSD-04MAY23
and topic1 = '\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
and bytea2numericpy(substring(data FROM 1 FOR 32)) > 0
and topic3 <> '\x000000000000000000000000654be0b5556f8eadbc2d140505445fa32715ef2b' -- excludes transfers TO the element deployer address
and (topic3 <> '\x0000000000000000000000000000000000000000000000000000000000000000' or topic2 <> '\x0000000000000000000000000000000000000000000000000000000000000000') -- ensure either to or from is not null
) el
on el.tx_hash = bv.evt_tx_hash
left join ethereum.transactions et
on et.hash = bv.evt_tx_hash
left join ethereum.blocks eb
on et.block_number = eb."number"
left join bbausd_lp_prices
on bbausd_lp_prices.date_trunc = date_trunc('day', evt_block_time)
left join (select * from prices.usd where symbol = 'WETH') peth
on date_trunc('minute', evt_block_time) = peth.minute
where "poolId" = '\x28b0379d98fb80da460c190c95f97c74302214b10002000000000000000003c0'
and deltas[2] <> 0
order by evt_block_time desc
)

select evt_block_time,evt_block_number,liquidity_provider::varchar,liquidityProvider::varchar,e_asset,lp_token,deposit_size_base,deposit_size_base_usd,deposit_size_e_asset,lp_tokens_acquired,tx_fee_eth,tx_fee_usd,evt_tx_hash::varchar,nonce,"index",tx_index from liquidity_data
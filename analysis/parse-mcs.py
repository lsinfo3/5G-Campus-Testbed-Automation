import argparse
import pandas as pd
import numpy as np
import os
import sys
import importlib
import itertools
import multiprocessing as mp

from IPython import embed

import logging
log = logging.getLogger()
log.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
log.addHandler(handler)

parsecsv = importlib.import_module('parse-csvs')


## WARNING: double check this for ping runs!!!



ex_run_dir = '/storage/test_mcs_splits/4433/4433__iperfDl_srsRAN_TDD10-2_Bw40__000_da8524c3'
# ex_run_dir = '/storage/test_mcs_splits/4433/4433__scaping_srsRAN_TDD10-1_Bw20__000_4ff24c93'
# ex_run_dir = '/storage/test_mcs_splits/4433/4433__idle---_OAI_TDD5-2_Bw40__003_24a30848'
# ex_run_dir = '/storage/test_mcs_splits/4433/4433__iperfDl_OAI_TDD20-1_Bw40__000_f4443267' # broken
# ex_run_dir = '/storage/full/5GHAT/4433/4433__iperfDl_OAI_TDD5-1_Bw20__004_3566b650'
# ex_run_dir = '/storage/full/5GHAT/4433/4433__iperfDl_srsRAN_TDD5-2_Bw20__002_33e26ccb'
# ex_run_dir = '/storage/full/5GHAT/4433/4433__iperfDl_srsRAN_TDD20-4_Bw20__000_a7115b32'
# ex_run_dir = '/storage/full/5GHAT/4433/4433__iperfDl_srsRAN_TDD10-4_Bw20__005_4f5604fe'
# ex_run_dir = '/storage/full/5GHAT/4433/4433__iperfDl_OAI_TDD5-1_Bw20__002_a591b41d'
ex_run_dir = '/storage/full/5GHAT/4433/4433__iperfUl_OAI_TDD5-1_Bw40__004_50317d1e'
ex_run_dir = '/storage/full/5GHAT/4433/4433__iperfDl_OAI_TDD10-2_Bw40__003_06d8800e'

def merge_based_on_following_mcs(df_input, df_channel):
    return pd.merge_asof(df_input,
                           df_channel[['TIMESTAMP','mcs_idx']],
                           on='TIMESTAMP',
                           direction='forward'
                           )

def merge_based_on_mcs_proximity(df_input, df_channel, cutoff=0.1):
    assert(cutoff >0)
    assert(cutoff <0.5)
    df_channel['next_ts'] = df_channel['TIMESTAMP'].shift(-1)
    df_channel['dt'] = df_channel['next_ts'] - df_channel['TIMESTAMP']
    df_channel['early_end'] = df_channel['TIMESTAMP'] + cutoff * df_channel['dt']
    df_channel['late_start'] = df_channel['TIMESTAMP'].shift(1) + (1-cutoff) * df_channel['dt'].shift(1)
    df_pkts_prev = pd.merge_asof(df_input,
                                   df_channel[['TIMESTAMP','early_end','mcs_idx' ]],
                                   on='TIMESTAMP',
                                   direction='backward'
                                   )
    df_pkts_prev = df_pkts_prev[ df_pkts_prev['TIMESTAMP']<=df_pkts_prev['early_end'] ]
    df_pkts_next = pd.merge_asof(df_input,
                                   df_channel[['TIMESTAMP','late_start','mcs_idx' ]],
                                   on='TIMESTAMP',
                                   direction='forward'
                                   )
    df_pkts_next = df_pkts_next[ df_pkts_next['TIMESTAMP']>=df_pkts_next['late_start'] ]
    return pd.concat([df_pkts_prev,df_pkts_next],ignore_index=True).sort_values('TIMESTAMP') #.drop(['late_start','early_end'], axis=1)


def create_mcs_based_splits(run_dir: str):
    parsed_run = f'{run_dir}/combined.csv.gz'
    if not os.path.isfile(parsed_run) or os.path.getsize(parsed_run) < 1000:
        log.error(f'[01]{run_dir}')
        return False
    df_packets = pd.read_csv(parsed_run).rename({'Timestamp':'TIMESTAMP'}, axis=1).sort_values(by=['TIMESTAMP'])
    assert((df_packets['trafficflow']=='ingress').all())
    first_pkt_timestamp = df_packets['TIMESTAMP'].min()
    last_pkt_timestamp = df_packets['TIMESTAMP'].max()
    df_channel_gnb = pd.read_csv(f'{run_dir}/gnb_snr.csv').query(f'TIMESTAMP>{first_pkt_timestamp} and TIMESTAMP<{last_pkt_timestamp}').reset_index(drop=True)
    for cm in df_channel_gnb.columns:
        df_channel_gnb[cm] = pd.to_numeric(df_channel_gnb[cm], errors='coerce')
    df_channel_gnb = df_channel_gnb.sort_values('TIMESTAMP', ascending=True)
    # logging delay gives different metrics for the same logging intervall different timestamps, we have to group these
    df_channel_gnb['diff'] = df_channel_gnb['TIMESTAMP'].diff()
    df_channel_gnb['logging_event'] = (df_channel_gnb['diff']>0.001).cumsum()
    df_channel_gnb = df_channel_gnb.groupby('logging_event').agg(lambda x: x.dropna().iloc[0] if x.notna().any() else np.nan)
    df_channel_gnb = df_channel_gnb.drop(['diff'], axis=1)
    df_channel_gnb['mcs_idx'] = df_channel_gnb.index
    # TODO: ue channel metrics
    df_channel_ue = pd.read_csv(f'{run_dir}/modem-snr.csv').query(f'TIMESTAMP>{first_pkt_timestamp} and TIMESTAMP<{last_pkt_timestamp}').reset_index(drop=True)
    power_sdr_path = f'{run_dir}/power_sdr.csv'
    if not os.path.isfile(power_sdr_path) or os.path.getsize(power_sdr_path) < 1000:
        log.error(f'[02]{run_dir}')
        return False
    df_energy_sdr = pd.read_csv(power_sdr_path).query(f'TIME>{first_pkt_timestamp} and TIME<{last_pkt_timestamp}').reset_index(drop=True).rename({'TIME':'TIMESTAMP'}, axis=1)
    power_ue_path = f'{run_dir}/power_ue.csv'
    if not os.path.isfile(power_ue_path) or os.path.getsize(power_ue_path) < 1000:
        log.error(f'[03]{run_dir}')
        return False
    df_energy_ue = pd.read_csv(power_ue_path).query(f'TIME>{first_pkt_timestamp} and TIME<{last_pkt_timestamp}').reset_index(drop=True).rename({'TIME':'TIMESTAMP'}, axis=1)
    df_perf_counter_gnb = parsecsv._get_perf_counters(run_dir, start=first_pkt_timestamp, end=last_pkt_timestamp, raw_values=True).rename({'Timestamp':'TIMESTAMP'}, axis=1)

    # print(df_packets)
    # print(df_channel_gnb)
    # print(df_energy_sdr)
    # print(df_perf_counter_gnb)
    # print('---')
    # print('---')
    # print('---')

    # # map packets to the next logging event based on their timestamp
    # df_pkts_mapped = merge_based_on_following_mcs(df_packets, df_channel_gnb)

    # only map packets in direct temporal relation to the mcs log event
    df_pkts_mapped = merge_based_on_mcs_proximity(df_packets, df_channel_gnb, cutoff=0.1)
    df_pkts_mapped = df_pkts_mapped.join(df_channel_gnb, on='mcs_idx', rsuffix='_mcs')
    df_pkts_mapped = df_pkts_mapped.drop(['trafficflow','SourceIPOuter','DestinationIPOuter','SourceIPInner','DestinationIPInner','mcs_idx_mcs'],axis=1)
    df_pkts_mapped_grp = df_pkts_mapped.groupby(['mcs_idx','location'])
    log.debug(f'len df_pkts_mapped: {len(df_pkts_mapped)}')
    # df_pkts_mapped.columns = list(map(lambda x: ''.join(filter(None,x)), df_pkts_mapped.columns.values))
    df_pkts_mapped = df_pkts_mapped_grp.mean()


    df_energy_sdr_mapped = merge_based_on_mcs_proximity(df_energy_sdr, df_channel_gnb, cutoff=0.1)
    df_energy_sdr_mapped = df_energy_sdr_mapped.join(df_channel_gnb, on='mcs_idx', rsuffix='_mcs')
    df_energy_sdr_mapped['voltage'] = df_energy_sdr_mapped['VAL'].where(df_energy_sdr_mapped['TYPE'].eq('voltage'), np.nan)
    df_energy_sdr_mapped['current'] = df_energy_sdr_mapped['VAL'].where(df_energy_sdr_mapped['TYPE'].eq('current'), np.nan)
    df_energy_sdr_mapped['power'] = df_energy_sdr_mapped['VAL'].where(df_energy_sdr_mapped['TYPE'].eq('power'), np.nan)
    df_energy_sdr_mapped = df_energy_sdr_mapped.drop(['TYPE','VAL','mcs_idx_mcs'],axis=1)
    df_energy_sdr_mapped_grp = df_energy_sdr_mapped.groupby('mcs_idx')
    df_energy_sdr_mapped = df_energy_sdr_mapped_grp.mean()


    df_energy_ue_mapped = merge_based_on_mcs_proximity(df_energy_ue, df_channel_gnb, cutoff=0.1)
    df_energy_ue_mapped = df_energy_ue_mapped.join(df_channel_gnb, on='mcs_idx', rsuffix='_mcs')
    df_energy_ue_mapped['voltage'] = df_energy_ue_mapped['VAL'].where(df_energy_ue_mapped['TYPE'].eq('voltage'), np.nan)
    df_energy_ue_mapped['current'] = df_energy_ue_mapped['VAL'].where(df_energy_ue_mapped['TYPE'].eq('current'), np.nan)
    df_energy_ue_mapped['power'] = df_energy_ue_mapped['VAL'].where(df_energy_ue_mapped['TYPE'].eq('power'), np.nan)
    df_energy_ue_mapped = df_energy_ue_mapped.drop(['TYPE','VAL','mcs_idx_mcs'],axis=1)
    df_energy_ue_mapped_grp = df_energy_ue_mapped.groupby('mcs_idx')
    df_energy_ue_mapped = df_energy_ue_mapped_grp.mean()


    df_perf_counter_mapped = merge_based_on_mcs_proximity(df_perf_counter_gnb, df_channel_gnb, cutoff=0.1)
    df_perf_counter_mapped = df_perf_counter_mapped.join(df_channel_gnb, on='mcs_idx', rsuffix='_mcs')
    df_perf_counter_mapped = df_perf_counter_mapped.drop(['mcs_idx_mcs'],axis=1)
    df_perf_counter_mapped_grp = df_perf_counter_mapped.groupby('mcs_idx')
    df_perf_counter_mapped = df_perf_counter_mapped_grp.mean()


    df_mcs = df_pkts_mapped.loc[:,['TIMESTAMP_mcs']]
    df_mcs = df_mcs.rename({'TIMESTAMP_mcs':'TS_end'},axis=1)
    df_mcs['TS_start']=df_mcs['TS_end'].shift(1)
    df_mcs.loc[0,'TS_start']=first_pkt_timestamp
    df_mcs['TS_start']=df_mcs['TS_start']-first_pkt_timestamp
    df_mcs['TS_end']=df_mcs['TS_end']-first_pkt_timestamp
    #df_mcs['duration']=df_mcs['TS_end']-df_mcs['TS_start']
    df_mcs = df_mcs.reset_index()


    df_mcs['duration'] = df_pkts_mapped_grp['TIMESTAMP'].agg('max').reset_index()['TIMESTAMP'] - df_pkts_mapped_grp['TIMESTAMP'].agg('min').reset_index()['TIMESTAMP']
    df_mcs['pkts'] = df_pkts_mapped_grp['PacketSize'].count().reset_index()['PacketSize']
    df_mcs['pkt_size'] = df_pkts_mapped_grp['PacketSize'].mean().reset_index()['PacketSize']
    df_mcs['delay'] = df_pkts_mapped_grp['delay'].mean().reset_index()['delay']

    df_mcs = df_mcs.merge(df_energy_ue_mapped_grp['current'].mean(), on='mcs_idx', how='left')
    df_mcs = df_mcs.merge(df_energy_ue_mapped_grp['voltage'].mean(), on='mcs_idx', how='left')
    df_mcs = df_mcs.merge(df_energy_ue_mapped_grp['power'].mean(), on='mcs_idx', how='left')
    df_mcs = df_mcs.rename({'current':'ue_current', 'voltage':'ue_voltage', 'power':'ue_power'},axis=1)

    df_mcs = df_mcs.merge(df_energy_sdr_mapped_grp['current'].mean(), on='mcs_idx', how='left')
    df_mcs = df_mcs.merge(df_energy_sdr_mapped_grp['voltage'].mean(), on='mcs_idx', how='left')
    df_mcs = df_mcs.merge(df_energy_sdr_mapped_grp['power'].mean(), on='mcs_idx', how='left')
    df_mcs = df_mcs.rename({'current':'sdr_current', 'voltage':'sdr_voltage', 'power':'sdr_power'},axis=1)

    df_mcs = df_mcs.merge(df_pkts_mapped[['CQI','SNR','RSRP','MCS_DL','MCS_UL']].reset_index(), on=['mcs_idx','location'], how='left')
    df_mcs = df_mcs.merge(df_perf_counter_mapped[['cache-misses','cycles','dTLB-load-misses','instructions']].reset_index(), on=['mcs_idx'], how='left')
    df_mcs['identifier'] = os.path.basename(run_dir)
    df_mcs = df_mcs.iloc[:, [len(df_mcs.columns)-1,*list(range(len(df_mcs.columns)-1))] ]
    # df_mcs.index = df_mcs.index.astype(int)
    df_mcs['mcs_idx'] = df_mcs['mcs_idx'].astype(int)
    # df_mcs['MCS_DL'] = df_mcs['MCS_DL'].astype(int)
    # df_mcs['MCS_UL'] = df_mcs['MCS_UL'].astype(int)
    df_mcs = df_mcs.rename({'location':'direction'}, axis=1)
    df_mcs['direction'] = df_mcs['direction'].apply(lambda x : 'DL' if x == 'ue' else 'UL')
    df_mcs.index = df_mcs.index.astype(int)

    return df_mcs[ ~(df_mcs['MCS_DL'].isna())&~(df_mcs['MCS_UL'].isna()) ]

    # TODO: check iperf
    # TODO: error checks: Nans, failed runs, etc


def create_mcs_based_splits_wrapper(run_dir: str):
    try:
        return create_mcs_based_splits(run_dir)
    except BaseException as e:
        print(f'Failed at:\n{run_dir}')
        print('===')
        import traceback
        print(traceback.format_exc())
        print('===')
        raise e


def main(basedir, skip):
    # test_configurations = [e.path for e in os.scandir(basedir) if e.is_dir() and not os.path.basename(e).startswith(".")]
    # runs = [r.path for t in test_configurations for r in os.scandir(t) if r.is_dir() and not os.path.basename(r).startswith(".")]
    # runs = [ r for r in runs if not os.path.isfile(f"{r}/FAILED") ]
    # print('\n'.join(runs))

    existing_df = pd.read_parquet(f'{basedir}/all_runs.parquet')
    get_mcs_runs = existing_df.loc[ ~(existing_df['failed_run'])&(existing_df['traffic_config__traffic_type']!='idle'), 'identifier' ]
    _subfolders = [ f.path for f in os.scandir(basedir) if f.is_dir() ]
    print(_subfolders)
    runs = [ f"{base}/{id}" for base,id in itertools.product(_subfolders, set(get_mcs_runs)) if os.path.isdir(f"{base}/{id}") ]
    print(runs)
    log.info(f"Handling {len(runs)} runs")

    with mp.Pool(8) as pool:
        returns = pool.map(create_mcs_based_splits_wrapper, runs)
    failed_runs = [ r for r in returns if not isinstance(r,pd.DataFrame) ]
    returns = [ r for r in returns if isinstance(r,pd.DataFrame) ]
    log.info(f"Successfully parsed {len(returns)} runs")
    df = pd.concat(returns, axis=0).reset_index()
    df = df.drop(['index'], axis=1)
    df.to_parquet(f'{basedir}/mcs_splits.parquet')
    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Split msm into discrete chunks, along the mcs reporting intervalls"
            )
    parser.add_argument("--skip", action='store_true')
    parser.add_argument("filename")
    args = parser.parse_args()

    df = main(args.filename, args.skip)

    # df = create_mcs_based_splits(ex_run_dir)
    print(df)


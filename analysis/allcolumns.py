agg_percentiles = [
    'ue_power',
    'sdr_power',
    'delay__mean',
    'delay__std',
    'throughput__mean',
    'throughput__std',
    'throughputin__mean',
    'iat__mean',
    'iat__std',
    'sent_pkts',
] + [ "modem_snr", "modem_sinr", "modem_rsrp", "modem_rsrq", "gnb_snr", "gnb_cqi", "gnb_rsrp", "gnb_mcs_dl", "gnb_mcs_ul" ] \
+ ['perf_instructions', 'perf_dTLB-load-misses', 'perf_cycles', 'perf_cache-misses']

agg_min = [
    'ue_power',
    'sdr_power',
    'delay__min',
    'missing_pkts',
    'sent_pkts',
    'throughput__mean',
    'throughput__min',
    'iat__min',
]
agg_max = [
    'ue_power',
    'sdr_power',
    'delay__max',
    'missing_pkts',
    'sent_pkts',
    'throughput__mean',
    'throughput__max',
    'iat__max',
]


agg_mean = [
    'ue_power',
    'sdr_power',
    'ue_power_failed',
    'sdr_power_failed',
    'actualduration',
    'failed_run',
    'missing_pkts',
    'sent_pkts',
    'delay__min',
    'delay__max',
    'delay__mean',
    'delay__std',
    'delay__5%',
    'delay__25%',
    'delay__50%',
    'delay__75%',
    'delay__95%',
    'throughput__min',
    'throughput__max',
    'throughput__mean',
    'throughput__std',
    'throughput__5%',
    'throughput__25%',
    'throughput__50%',
    'throughput__75%',
    'throughput__95%',
    'throughputin__mean',
    'iat__max',
    'iat__mean',
    'iat__std',
    'iat__5%',
    'iat__25%',
    'iat__50%',
    'iat__75%',
    'iat__95%',
] + [ "modem_snr", "modem_sinr", "modem_rsrp", "modem_rsrq", "gnb_snr", "gnb_cqi", "gnb_rsrp", "gnb_mcs_dl", "gnb_mcs_ul" ]\
+ ['perf_instructions', 'perf_dTLB-load-misses', 'perf_cycles', 'perf_cache-misses']


all_msm_columns = [
'ue_power',
'sdr_power',
'ue_current',
'ue_voltage',
'sdr_current',
'sdr_voltage',
'ue_power_failed',
'sdr_power_failed',
'actualduration',
'failed_run',
'missing_pkts',
'sent_pkts',
'delay__min',
'delay__max',
'delay__mean',
'delay__std',
'delay__5%',
'delay__25%',
'delay__50%',
'delay__75%',
'delay__95%',
'throughput__min',
'throughput__max',
'throughput__mean',
'throughput__std',
'throughput__5%',
'throughput__25%',
'throughput__50%',
'throughput__75%',
'throughput__95%',
'throughputin__mean',
'iat__min',
'iat__max',
'iat__mean',
'iat__std',
'iat__5%',
'iat__25%',
'iat__50%',
'iat__75%',
'iat__95%',
] + [ "modem_snr", "modem_sinr", "modem_rsrp", "modem_rsrq", "gnb_snr", "gnb_cqi", "gnb_rsrp", "gnb_mcs_dl", "gnb_mcs_ul" ] \
+ ['perf_instructions', 'perf_dTLB-load-misses', 'perf_cycles', 'perf_cache-misses'] \
+ [f'IBT_ue_{i:02d}' for i in range(0,101)] \
  + [f'IBT_gnb_{i:02d}' for i in range(0,101)] \
  + [f'BT_ue_{i:02d}' for i in range(0,101)] \
  + [f'BT_gnb_{i:02d}' for i in range(0,101)] \
  + [f'BD_ue_{i:02d}' for i in range(0,101)] \
  + [f'BD_gnb_{i:02d}' for i in range(0,101)]

all_columns = [
'ue_power',
'sdr_power',
'ue_current',
'ue_voltage',
'sdr_current',
'sdr_voltage',
'ue_power_failed',
'sdr_power_failed',
'actualduration',
'direction',
'failed_run',
'missing_pkts',
'sent_pkts',
'pkt_size',
'delay__min',
'delay__max',
'delay__mean',
'delay__std',
'delay__5%',
'delay__25%',
'delay__50%',
'delay__75%',
'delay__95%',
'throughput__min',
'throughput__max',
'throughput__mean',
'throughput__std',
'throughput__5%',
'throughput__25%',
'throughput__50%',
'throughput__75%',
'throughput__95%',
'throughputin__mean',
'iat__min',
'iat__max',
'iat__mean',
'iat__std',
'iat__5%',
'iat__25%',
'iat__50%',
'iat__75%',
'iat__95%',
'distance_horizontal_in_m',
'distance_vertical_in_m',
'gnb_antenna_inclanation_in_degree',
'gnb_antenna_rotation_in_degree',
'ue_antenna_inclanation_in_degree',
'ue_antenna_rotation_in_degree',
'modem',
'interface_ue',
'interface_gnb',
'jammer',                               # DEFAULT: false
'dockerization',                        # DEFAULT: false
'performance_tuning',                   # DEFAULT: false
'distance_floor',                       # DEFAULT: 0.2
'distance_nearest_wall',                # DEFAULT: 0.2
'location',                             # DEFAULT: A202?
'sdr',
'identifier',
'run',
'rx_gain',
'tx_gain',
'gnb_version__type',
'gnb_version__uhd_version',
'gnb_version__version',
'gnb_version__commit',
'traffic_config__traffic_type',
'traffic_config__direction',
'traffic_config__traffic_duration',
'traffic_config__proto',
'traffic_config__dist',
'traffic_config__iat',
'traffic_config__rate',
'traffic_config__size',
'traffic_config__count',
'traffic_config__target_ip',
'traffic_config__target_port',
'traffic_config__burst',
'tdd_config__tdd_ratio',
# 'tdd_config__tdd_flex_slots',
'tdd_config__tdd_period',
'tdd_config__tdd_dl_slots',
'tdd_config__tdd_dl_symbols',
'tdd_config__tdd_ul_slots',
'tdd_config__tdd_ul_symbols',
'gnb_version__combined'
] + [ "modem_snr", "modem_sinr", "modem_rsrp", "modem_rsrq", "gnb_snr", "gnb_cqi", "gnb_rsrp", "gnb_mcs_dl", "gnb_mcs_ul" ] \
+ ['perf_instructions', 'perf_dTLB-load-misses', 'perf_cycles', 'perf_cache-misses'] \
+ [f'IBT_ue_{i:02d}' for i in range(0,101)] \
  + [f'IBT_gnb_{i:02d}' for i in range(0,101)] \
  + [f'BT_ue_{i:02d}' for i in range(0,101)] \
  + [f'BT_gnb_{i:02d}' for i in range(0,101)] \
  + [f'BD_ue_{i:02d}' for i in range(0,101)] \
  + [f'BD_gnb_{i:02d}' for i in range(0,101)]


columns_to_group_by = list( set(all_columns).difference(set(all_msm_columns)).difference(set(["identifier"])) )

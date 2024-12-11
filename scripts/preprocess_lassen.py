#!/usr/bin/env python3
from pathlib import Path
import pandas as pd
import sys

path = Path(sys.argv[1])

node_history_df = pd.read_csv(path / 'final_csm_allocation_node_history.csv')
allocation_history_df = pd.read_csv(path / 'final_csm_allocation_history_hashed.csv', usecols=['allocation_id', 'begin_time'])

allocation_to_time = {r.allocation_id: r.begin_time for r in allocation_history_df.itertuples()}

node_history_df['job_begin_time'] = node_history_df['allocation_id'].apply(lambda id: allocation_to_time[id])

node_history_df.to_csv(path / 'final_csm_allocation_node_history_with_time.csv')

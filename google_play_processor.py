import pandas as pd
import numpy as np
import os
import shutil
import re
from shutil import unpack_archive, move
from datetime import date
from pathlib import Path
from typing import Optional, List

class GooglePlayProcessor:
  source_directory_path: Path
  processed_data_path: Path

  country_impressions_df: Optional[pd.DataFrame]
  country_downloads_df: Optional[pd.DataFrame]
  channel_impressions_df: Optional[pd.DataFrame]
  channel_downloads_df: Optional[pd.DataFrame]

  def __init__(self, source_directory_path: Path, processed_data_path: Optional[Path]=None):
    self.source_directory_path = source_directory_path
    self.processed_data_path = processed_data_path if processed_data_path else source_directory_path / Path('processed')

    self.country_impressions_df = pd.DataFrame()
    self.country_downloads_df = pd.DataFrame()
    self.channel_impressions_df = pd.DataFrame()
    self.channel_downloads_df = pd.DataFrame()

  @property
  def source_dir(self) -> str:
    return str(self.source_directory_path.absolute()) + '/'

  @property
  def processed_data_frames(self) -> List[pd.DataFrame]:
    data_frames = [
      self.country_impressions_df,
      self.country_downloads_df,
      self.channel_impressions_df,
      self.channel_downloads_df
    ]

    return [df for df in data_frames if not df.empty]
  
  @property
  def min_processed_date(self) -> Optional[date]:
    if self.processed_data_frames:
      return min([d.date.min() for d in self.processed_data_frames]).date()
    else:
      return None
  
  @property
  def max_processed_date(self) -> Optional[date]:
    if self.processed_data_frames:
      return max([d.date.max() for d in self.processed_data_frames]).date()

  def process(self):
    #List of all files in inputs
    full_item_list = [e for e in os.listdir(self.source_dir)]
    zip_files = [x for x in full_item_list if re.compile('zip').findall(x)]

    for z in zip_files:
      unpack_archive(self.source_dir+z, self.source_dir)

    #Re-set full item list
    full_item_list = [x for x in os.listdir(self.source_dir) if re.compile('csv').findall(x)]
    gpc_scrape = [x for x in full_item_list if re.compile('GooglePlayScraper').findall(x)][0]

    df_pc = pd.DataFrame()
    df_c = pd.DataFrame()
    df_ch = pd.DataFrame()

    for i in [x for x in full_item_list if re.compile('play_country.csv').findall(x)]:
        df = pd.read_csv(self.source_dir+i, encoding = 'utf-16', na_filter = False).astype({'Date': 'datetime64[ns]', 'Store Listing Visitors': 'float', 'Installers': 'float'})
        df_pc = df.append(df_pc)

    for i in [x for x in full_item_list if re.compile('country.csv').findall(x) and x not in [x for x in full_item_list if re.compile('play_country.csv').findall(x)]]:
        df = pd.read_csv(self.source_dir+i, encoding = 'utf-16', na_filter = False).astype({'Date': 'datetime64[ns]', 'Store Listing Visitors': 'float', 'Installers': 'float'})
        df_c = df.append(df_c)

    for i in [x for x in full_item_list if re.compile('channel.csv').findall(x)]:
        df = pd.read_csv(self.source_dir+i, encoding = 'utf-16', na_filter = False).astype({'Date': 'datetime64[ns]', 'Store Listing Visitors': 'float', 'Installers': 'float'})
        df_ch = df.append(df_ch)


    #--------Country--------------------------------------------------------------------------------------------
    df_c = df_c.loc[:, 'Date':'Installers'] #take columns from Date up until (and including) Installers
    df_c = df_c.rename(columns={'Date':'date', 'Package Name': 'app_name',  'Country': 'country_code', 'Store Listing Visitors': 'total_impressions', 'Installers': 'total_downloads'})

    df_c['country_code'] = np.where(df_c['country_code']=='', 'XX', df_c['country_code'])

    aggregations = { 'total_impressions':'sum', 'total_downloads': 'sum'}
    df_c = df_c.groupby(['date', 'app_name', 'country_code'], as_index=False).agg(aggregations)


    df_pc = df_pc.loc[:, 'Date':'Installers'] #take columns from Date up until (and including) Installers
    df_pc = df_pc.rename(columns={'Date':'date', 'Package Name': 'app_name', 'Country (Play Store)': 'country_code', 'Store Listing Visitors': 'organic_impressions', 'Installers': 'organic_downloads'})

    df_pc['country_code'] = np.where(df_pc['country_code']=='', 'XX', df_pc['country_code'])


    aggregations = { 'organic_impressions':'sum', 'organic_downloads': 'sum'}
    df_pc = df_pc.groupby(['date', 'app_name', 'country_code'], as_index=False).agg(aggregations)

    master_c = df_c.merge(df_pc, on = ['date', 'app_name', 'country_code'], how = 'left') .fillna({'organic_impressions': 0,'organic_downloads': 0})

    master_c['inorganic_impressions'] = master_c['total_impressions'] - master_c['organic_impressions']
    master_c['inorganic_downloads'] = master_c['total_downloads'] - master_c['organic_downloads']

    master_c['inorganic_impressions'] = np.where(master_c['inorganic_impressions']<0, 0, master_c['inorganic_impressions'])
    master_c['inorganic_downloads'] = np.where(master_c['inorganic_downloads']<0, 0, master_c['inorganic_downloads'])

    master_c['platform_id'] = 2

    country_impressions = pd.DataFrame()
    country_downloads = pd.DataFrame()
    for j in [x for x in master_c.columns.to_list() if x not in ('date', 'app_name', 'country_code', 'platform_id')]:
        if j.split('_')[0] == 'total':
            source = 'Total Play Store'
        elif j.split('_')[0] == 'organic':
            source = 'Play Store (organic)'
        else:
            source = 'Inorganic'
        calc_type = j.split('_')[1]

        df = master_c[['date', 'app_name', 'country_code', 'platform_id']]    
        df[calc_type] = master_c[j]
        df['source'] = source

        if calc_type  == 'impressions':
            country_impressions = country_impressions.append(df, sort=True)    
        if calc_type  == 'downloads':
            country_downloads = country_downloads.append(df, sort=True)

    country_impressions = country_impressions[country_impressions['source'] != 'Total Play Store']
    country_downloads = country_downloads[country_downloads['source'] != 'Total Play Store']

    self.country_impressions_df = country_impressions[['date', 'impressions', 'platform_id', 'source', 'app_name', 'country_code']]
    self.country_downloads_df = country_downloads[['date', 'downloads', 'platform_id', 'source', 'app_name', 'country_code']]

    #--------Channel--------------------------------------------------------------------------------------------
    df_ch = df_ch.loc[:, 'Date':'Installers'] #take columns from Date up until (and including) Installers
    df_ch = df_ch.rename(columns={'Date':'date', 'Package Name': 'app_name', 'Acquisition Channel': 'source', 'Store Listing Visitors': 'impressions', 'Installers': 'downloads'})
    try:
        df_gpc = pd.read_csv(self.source_dir+gpc_scrape, na_filter = False).astype({'date': 'datetime64[ns]', 'store_listing_visitors': 'float', 'first_time_installers': 'float'})
        df_gpc = df_gpc.rename(columns={'date':'date',   'app_id': 'app_name', 'acquisition_channel': 'source',   'store_listing_visitors': 'impressions',   'first_time_installers': 'downloads'})
        df_gpc = df_gpc[df_gpc.date.isin(set(df_ch.date.unique()))] #filter GPC data to exported data
        df_gpc = df_gpc[df_ch.columns.to_list()]

        df_ch = df_ch[df_ch.date.isin(set(df_gpc.date.unique()))] #filter exported data to GPC
        df_ch = df_ch[df_ch['source'] != 'Play Store (organic)']

        df_ch = df_ch.append(df_gpc)
    except:
        print('GPC data not loaded or does not exist')
        pass

    df_ch['platform_id'] = 2

    self.channel_impressions_df = df_ch[['date', 'impressions', 'platform_id', 'source', 'app_name']]
    self.channel_downloads_df = df_ch[['date', 'downloads', 'platform_id', 'source', 'app_name']]
  
  def save(self):
    if len(self.processed_data_frames) != 4:
      raise ValueError('Data is not fully processed.')

    if not self.processed_data_path.exists():
      self.processed_data_path.mkdir()
    else:
      raise ValueError('Proccessed data path already exists.', self.processed_data_path)
    
    self.country_impressions_df.to_csv(f'{self.processed_data_path.absolute()}/country-impressions.csv', index=None)
    self.country_downloads_df.to_csv(f'{self.processed_data_path.absolute()}/country-downloads.csv', index=None)
    self.channel_impressions_df.to_csv(f'{self.processed_data_path.absolute()}/channel-impressions.csv', index=None)
    self.channel_downloads_df.to_csv(f'{self.processed_data_path.absolute()}/channel-downloads.csv', index=None)
##

import pandas as pd
from githubdata import GithubData
from mirutil import funcs as mf
from mirutil.funcs import read_data_according_to_type as rdata


ifp = 'list.xlsx'

id2t_rp_url = 'https://github.com/imahdimir/d-TSETMC-ID-2-Ticker-map'

cur_rp_url = 'https://github.com/imahdimir/search-a-list-in-TSETMC'

tic = 'Ticker'
tid = 'TSETMC_ID'
ticn = 'TickerN'
srch = 'srch'
idcols = {
    'ID-1' : 1 ,
    'ID-2' : 2 ,
    'ID-3' : 3 ,
    'ID-4' : 4 ,
    }

def main() :
  pass

  ##


  df = mf.read_data_according_to_type(ifp)
  df[0] = df[df.columns[0]]
  df = df[[0]]

  ##
  dfs = pd.DataFrame()

  ##
  for _ , row in df.iterrows() :
    _sdf = mf.search_tsetmc(row[0])
    _sdf[srch] = row[0]
    dfs = pd.concat([dfs , _sdf])

  ##
  mf.save_as_prq_wo_index(dfs , 'temp.prq')

  ##
  dfs = rdata('temp.prq')

  ##
  idf = pd.DataFrame()

  for ky , vl in idcols.items() :
    _idf = dfs[dfs.columns.difference(set(idcols.keys()) - set(ky))]

    _idf[tid] = dfs[[ky]]
    _idf[ticn] = dfs[tic] + str(vl)

    idf = pd.concat([idf , _idf])

  ##
  id2t_rp = GithubData(id2t_rp_url)
  id2t_rp.clone()

  ##
  dfmp = id2t_rp.data_filepath
  dfm = mf.read_data_according_to_type(dfmp)
  dfm = dfm.reset_index()

  ##
  idf1 = idf[[tid , ticn]]
  idf1 = idf1.rename(columns = {
      ticn : tic
      })

  ##
  dfm = pd.concat([dfm , idf1])
  dfm = dfm.drop_duplicates()
  assert dfm[tid].is_unique

  ##
  dfm = dfm.set_index(tid)

  ##
  dfm.to_parquet(dfmp)

  ##
  msg = 'added by searching a list'
  msg += 'by: ' + cur_rp_url

  id2t_rp.commit_push(msg)

  ##

  id2t_rp.rmdir()


##
##
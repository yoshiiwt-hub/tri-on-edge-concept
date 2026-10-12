# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
#################### FIX NOTE (2026.08.09  IWATA,Y.) ##########
# tri_edge_23_eda_interactive.py 経由での実行時、以下の既存バグを確認・修正した：
#  ①gen_interact_linechart() 内 def_button(): y_min_/y_max_ 計算で
#    d_df['_idx'][[y_+'_ss']]（二重角括弧=DataFrame）に .min()/.max() をかけていたため、
#    int()変換でSeriesを渡すことになりTypeErrorになっていた。d_df['_idx'][y_+'_ss']
#    （単一角括弧=Series）に修正（AnalyzeFunctionsVZ_DT.py で見つけた同系統のバグと同じパターン）。
################################################################
#################### IMPORT LIBRARY ##########
import copy             #リストのコピー
import datetime as dt   #時間、時刻
import itertools as itr #組み合わせ
import numpy as np
np.set_printoptions(precision=3, suppress=True)
np.set_printoptions(threshold=20)
np.set_printoptions(edgeitems=5)
import os
import pandas as pd
pd.options.display.precision    = 3
pd.options.display.float_format = '{:.3f}'.format
pd.options.display.max_rows     = None ##32
pd.options.display.max_columns  = None ##16
pd.options.display.show_dimensions = True
import plotly
import plotly.express       as px
import plotly.graph_objects as go
from   sklearn import preprocessing as pp
########################################

############### プロットの構成を定義 ############
## v01c.20230613
def def_plot_const(df, l_ql_, l_qn_, tar_):
    print("_____ def_plot_const _____")
    print(dt.datetime.now(),'\n')
    
    df = df.sort_index()
    is_wave_ = ('WVID' in list(df.columns))
    
    if (tar_ in list(l_qn_)):
        l_col_qn = list(l_qn_)
        l_col_qn.remove(tar_)
        l_col_qn = [tar_] + l_col_qn
        l_col_ql = list(l_ql_)
    elif (tar_ in list(l_ql_)):
        l_col_ql = list(l_ql_)
        l_col_ql.remove(tar_)
        l_col_ql = [tar_] + l_col_ql
        l_col_qn = list(l_qn_)
    else:
        tar_ = list(l_qn_)[0]
        l_col_ql = list(l_ql_)
        l_col_qn = list(l_qn_)
    ##:if
    
    ## Index
    idx_is_date = (df.index.dtype=='datetime64[ns]')
    if (idx_is_date): df['IDX_'] = df.index.strftime('%Y/%m/%d %H:%M:%S.%f').str[:-3]
    else:             df['IDX_'] = df.index.values.astype('str')
    
    ## QL (date cat)
    if False:####(idx_is_date): 
        df['Dt_Month'] = df.index.strftime('%Y%b')
        df['Dt_Weak']  = df.index.strftime('%YW%W')
        df['Dt_Day']   = df.index.strftime('%m%d')
        l_col_ql = l_col_ql + ['Dt_Month', 'Dt_Weak', 'Dt_Day']
    ##:if (idx_is_date)
    
    ## QL (refference column)
    df[l_col_ql] = df[l_col_ql].astype('str')
    df['QL_'] = ''
    for ql in l_col_ql[:5]: df['QL_'] = df['QL_'].str.cat('"'+df[ql].str[:16]+'", ', sep='')
    df['QL_'] = [s[:-2] for s in df['QL_']]
    
    ## QL (label encode)
    l_col_ql_lb = []
    d_ql_cl     = {}
    for ql_ in l_col_ql:
        lb_ = ql_ + '_lb'
        df[lb_], label_ = pd.factorize(df[ql_])
        d_ql_cl[ql_] = list(label_)
        l_col_ql_lb.append(lb_)
    ##:for
    
    l_cl_ = []
    for k in list(d_ql_cl.keys()):
        for v in d_ql_cl[k]: l_cl_.append(v)
    ##:for
    
    ## QN (refference column)
    df['QN_'] = ''
    for qn in l_col_qn[:5]: df['QN_'] = df['QN_'].str.cat(df[qn].apply(lambda v: '{:.2f}'.format(v))+', ', sep='')
    df['QN_'] = [s[:-2] for s in df['QN_']]
    
    ## Scaling
    l_col_num = l_col_ql_lb + l_col_qn
    for c in l_col_num:
        df[c+'_ss'] = pp.StandardScaler().fit_transform(df[[c]])
        df[c+'_mm'] = pp.MinMaxScaler().fit_transform(df[[c]])
    ##:for
    for i in range(len(l_col_num)): df[l_col_num[i]+'_mm'] = df[l_col_num[i]+'_mm'] + i*1.0
    
    ## WaveID, WaveDF per Ql*Cl
    d_ql_cl_wvid, d_ql_cl_wvdf = {}, {}
    if (is_wave_):
        for ql_ in l_col_ql:
            for cl_ in d_ql_cl[ql_]:
                ql_cl_ = ql_+'_'+cl_
                qry_ = '{} in {} and WVID != -1'.format(ql_, str([cl_]))
                d_ql_cl_wvid[ql_cl_] = list(np.sort(df.query(qry_)['WVID'].unique()))
                d_ql_cl_wvdf[ql_cl_] = [df.query('WVID=={}'.format(s)) for s in d_ql_cl_wvid[ql_cl_]]
            ##:for
        ##:for
    ##:if
    
    ## Qn*Ql*Cl
    l_qn_ql_cl_ = []
    for qn_, ql_ in itr.product(l_col_qn, l_col_ql):
        for cl_ in d_ql_cl[ql_]:
            ql_cl_ = ql_+'_'+cl_
            
            if (is_wave_):
                for i_ in range(len(d_ql_cl_wvdf[ql_cl_])): l_qn_ql_cl_.append((qn_, ql_, cl_, i_))
                ##:for
            else:
                l_qn_ql_cl_.append((qn_, ql_, cl_))
            ##:if
        ##:for
    ##:if
    
    ## View Construction
    print('df.columns', len(df.columns))
    print('{}*{}'.format(len(l_col_ql), 4).ljust(4), 'l_col_ql * (org, _lb, _mm, _ss)')
    print('{}*{}'.format(len(l_col_qn), 3).ljust(4), 'l_col_qn * (org, _mm, _ss)')
    print('3'.ljust(4), "['QL_', 'QN_', 'IDX_']")
    if (is_wave_): print('3'.ljust(4), "['WVID', 'WVPT', 'COUT']")
    print()
    
    print("df['QL_'] :", list(df['QL_'][:1]) ,'..')
    print("df['QN_'] :", list(df['QN_'][:1]) ,'..')
    print("df['IDX_']:", df['IDX_'].iloc[0],'~', df['IDX_'].iloc[-1])
    print()
    
    print('d_ql_cl', sum([len(v) for v in d_ql_cl.values()]), '(l_col_ql * class)')
    for k, v in d_ql_cl.items(): print(k, len(v),'\t', "['{}'~'{}']".format(v[0], v[-1]))
    print()
    
    if not(is_wave_):
        print('l_qn_ql_cl_', len(l_qn_ql_cl_), '(l_col_qn * l_col_ql * class)')
        print(l_qn_ql_cl_[:3], '~', l_qn_ql_cl_[-3:],'\n')
    elif (is_wave_):
        print('d_ql_cl_wvdf', len(d_ql_cl_wvdf), [(k, len(v)) for k, v in d_ql_cl_wvdf.items()])
    ##:if
    
    print()
    print(dt.datetime.now(),'\n')
    return df, l_col_ql, l_col_qn, l_qn_ql_cl_, d_ql_cl, d_ql_cl_wvdf
########################################

############### 対話的な折れ線 ###############
## v02a.20230830
def gen_interact_linechart(df, l_ql_, l_qn_, tar_, d_cfg_):
    print("_____ gen_interact_linechart _____")
    print(dt.datetime.now(),'\n')
    
    title     = d_cfg_["title"]
    is_mobile = (__file__[:9] in ["/storage/"])
    
    ## Define Plot Construction
    df, l_col_ql, l_col_qn, _,_,_ = def_plot_const(df, l_ql_, l_qn_, tar_)
    df = df.sort_index()
    
    n_rec      = len(df.index)
    n_ql, n_qn = len(l_col_ql), len(l_col_qn)
    
    ## Scaling
    l_col_ql_lb    = [s+'_lb' for s in l_col_ql]
    l_col_ql_lb_ss = [s+'_ss' for s in l_col_ql_lb]
    l_col_ql_lb_mm = [s+'_mm' for s in l_col_ql_lb]
    
    l_col_qn_ss = [s+'_ss' for s in l_col_qn]
    l_col_qn_mm = [s+'_mm' for s in l_col_qn]
    
    l_col_ss = l_col_qn_ss + l_col_ql_lb_ss ####
    l_col_mm = l_col_qn_mm + l_col_ql_lb_mm ####
    
    for c_ in l_col_qn + l_col_ql_lb:
        df[c_+'_ss'] = pp.StandardScaler().fit_transform(df[[c_]])
        df[c_+'_mm'] = pp.MinMaxScaler().fit_transform(df[[c_]])
    ##:for
    
    ## Column Name, Num
    l_col_base = l_col_qn + l_col_ql_lb ####
    l_col_base = [tar_] + [s for s in l_col_base if (s.replace('_lb','') != tar_)] ####
    n_col_base = len(l_col_base)
    
    #### PLOT
    yaxis_domain  = [0.25, 1.0] if (is_mobile) else [0.0, 0.9]
    button_loc_y  = 0.125 if (is_mobile) else 1.075
    button_direc  = 'up' if (is_mobile) else 'down'
    l_t_slider_loc_ = [(0, 0.09, 1), (0, 0.05, 1)] if (is_mobile) else [(0.2, 1.10, 0.75), (0.2, 1.05, 0.75)]
    rgslider_len  = 0.05 if (is_mobile) else 0.05
    fontsize_base = 40   if (is_mobile) else 20
    
    title_text    = '<b>{}</b>  Rec.({})  Col.(QL:{}  QN:{})'.format(title, n_rec, n_ql, n_qn)
    
    #### TRACES
    print("_____ def traces _____")
    def def_trace(df_, y_, psfx_):
        
        width_ = 2.5 ##if (y_nm_==tar_) else 1.5
        msize_ = 5.5 ##if (y_nm_==tar_) else 8.5
        
        ####ind_is_time_ = (str(df_.index.dtype)[:8]=='datetime')
        
        trace_ = go.Scattergl(
            name='{}'.format(y_[:-3])+str(psfx_)
          , x=list(range(len(df_)))####df.index.strftime('%m.%d<br>%H:%M<br>%S.%f').str[:-5]####
          , y=df_[y_]
          , mode='markers+lines'
          , line={"width": width_}
          , marker={"size": msize_, "symbol": "circle-open-dot"}
          , opacity=0.8
          , hovertemplate=' '\
          + 'Var: '+y_[:-3]+' ,  '+'%{hovertext}'
          , hovertext=\
              'Val: '  +df_[y_[:-3]].astype('str')+' <br> '\
            + 'Index: '+df_['IDX_'] +' <br> '\
            + 'QL: '   +df_['QL_']  +' <br> '\
            + 'QN: '   +df_['QN_']  +' '
          , hoverlabel={"font": {"size": fontsize_base}, "namelength":0}
        )
        
        return trace_
    ##:def
    
    d_df = {}
    d_df["_idx"] = df
    d_df["_tar"] = df.sort_values([tar_]).reset_index(drop=True)
    
    ## l_traces[:] -> ql+qn > mm/ss > idx/tar sort
    l_traces = []
    t_all_mm_mm, t_all_ss_mm = (0, 0), (0, 0)####
    for i in range(n_col_base):
        ## mm
        scale_ = '_mm'
        for sort_ in ['_idx', '_tar']:
            d_df[sort_][l_col_mm[i]] = d_df[sort_][l_col_mm[i]] + i*1.1
            l_traces.append(def_trace(d_df[sort_], l_col_mm[i], scale_+sort_))
            
            t_all_mm_mm = ( min(t_all_mm_mm[0], d_df[sort_][l_col_mm[i]].min()), max(t_all_mm_mm[1], d_df[sort_][l_col_mm[i]].max()) )####
        ##:for sort_
        
        ## ss
        scale_ = '_ss'
        for sort_ in ['_idx', '_tar']:
            l_traces.append(def_trace(d_df[sort_], l_col_ss[i], scale_+sort_))
            
            t_all_ss_mm = ( min(t_all_ss_mm[0], d_df[sort_][l_col_ss[i]].min()), max(t_all_ss_mm[1], d_df[sort_][l_col_ss[i]].max()) )####
        ##:for sort_
    ##:for i
    
    l_traces_name = [l_traces[s]["name"] for s in range(len(l_traces))]
    
    for i in range(len(l_traces_name)): print('{:>2}/{:>2} col_{:0>2}_{:0>2}'.format(
    i, 
    len(l_traces_name), 
    i%n_col_base, n_col_base
    ), 
    l_traces_name[i][-6:-4],
    l_traces_name[i][-3:],
    l_traces_name[i] if not(l_traces_name[i][:-7].replace('_lb','')==tar_) else '* '+l_traces_name[i],
    )##
    print()
    
    #### Viz
    print("_____ def visibles _____")
    d_l_viz = {}
    for col_ in ['All']:
        for scale_ in ['_mm', '_ss']:
            for sort_ in ['_idx', '_tar']:
                d_l_viz[col_+scale_+sort_] = [(scale_+sort_==s[-7:]) for s in l_traces_name]
            ##:for sort_
        ##:for scale_
    ##:for scale_
    
    for col_ in l_col_base:
        for col2_ in l_col_base:
            for sort_ in ['_idx', '_tar']:
                d_l_viz['_'.join([col_, col2_])+'_ss'+sort_] = [
                   (col_ +'_ss'+sort_==s[:])
                or (col2_+'_ss'+sort_==s[:])
                or (tar_ +'_ss'+sort_==s[:].replace('_lb',''))
                for s in l_traces_name]
            ##:for sort_
        ##:for col2_
    ##:for col_
    
    for k, v in d_l_viz.items(): print(k, len(v), v[:].count(True))##
    print()
    
    #### STEPS of SLIDERS
    print("_____ def steps of sliders _____")
    def def_step(l_anno_, l_l_vis_, l_label_):
        step_ = []
        for i in range(len(l_l_vis_)):
            step_.append({
                'label' : l_label_[i] if (len(l_label_[i])<5) else '{}..'.format(l_label_[i][:4]),
                'method': 'update', ## 'restyle' ##
                'args': [
                    {'visible': l_l_vis_[i]},
                    {'annotations': [{
                        'text': l_anno_[i],
                        'x': 0.995, 'xanchor': 'right', 'xref': 'paper',
                        'y': yaxis_domain[1]-0.015, 'yanchor': 'top', 'yref': 'paper',
                        'align': "left",
                        'bgcolor': 'None', 'bordercolor': 'royalblue',
                        'borderpad': {'l': 15, 'r': 15, 't': 2, 'b': 10},
                        'font': {'color': 'royalblue'},
                        'showarrow': False,
                    }]}
                ],
            })
        ##:for
        return step_
    ##:def
    
    d_steps = {}
    for col_ in ['All']:
        for scale_ in ['_mm', '_ss']:
            d_steps[col_+scale_] = def_step(
                ['  "{}"  <br>   Target: "{}"  <br>   Sorted:  {}  '.format(col_+scale_, tar_, s) for s in ['Index', 'Target']],
                [d_l_viz[col_+scale_+'_idx'], d_l_viz[col_+scale_+'_tar']],
                ['Idx', 'Tar']
            )
        ##:for scale_
    ##:for col_
    
    for col_ in l_col_base:
        for scale_ in ['_ss']:
            d_steps[col_+'_idx'] = def_step(
                ['  "{}"  <br>  "{}"  <br>   Target: "{}"  <br>   Sorted:  Index  '.format(col_, s, tar_) for s in l_col_base],
                [d_l_viz['_'.join([col_, col2_])+scale_+'_idx'] for col2_ in l_col_base],
                ['']*len(l_col_base)
            )
            d_steps[col_+'_tar'] = def_step(
                ['  "{}"  <br>  "{}"  <br>   Target: "{}"  <br>   Sorted:  Target '.format(col_, s, tar_) for s in l_col_base],
                [d_l_viz['_'.join([col_, col2_])+scale_+'_tar'] for col2_ in l_col_base],
                l_col_base
            )
        ##:for scale_
    ##:for col_
    
    for k, v in d_steps.items(): print(k, [s['label'] for s in v], len(v[0]['args'][0]['visible']))##
    print()
    
    #### SLIDERS
    print("_____ def sliders _____")
    def def_slider(sl1_step_, sl2_step_, l_loc_):
        slider_ = [
        { 'steps': sl1_step_
        , 'x': l_loc_[0][0], 'xanchor': 'left'
        , 'y': l_loc_[0][1], 'yanchor': 'top'
        , 'len': l_loc_[0][2]
        , 'minorticklen': fontsize_base*0.5
        , 'active': 0
        , 'currentvalue': {"visible": False}#### True, 'xanchor': 'left', 'offset': 0, 'prefix': 'Sort by Index: ', 'suffix': ''}####
        , 'bgcolor': "blue"
        , 'activebgcolor': "snow"
        , 'borderwidth': 2.5
        , 'transition': go.layout.slider.Transition(duration=350)
        },
        { 'steps': sl2_step_
        , 'x': l_loc_[1][0], 'xanchor': 'left'
        , 'y': l_loc_[1][1], 'yanchor': 'top'
        , 'len': l_loc_[1][2]
        , 'minorticklen': fontsize_base*0.5
        , 'active': 0
        , 'currentvalue': {"visible": False}
        , 'bgcolor':"blue"
        , 'activebgcolor':"snow"
        , 'borderwidth': 2.5
        , 'transition': go.layout.slider.Transition(duration=350)
        }
        ]
        return slider_
    ##:def
    
    d_sliders = {}
    for col_ in ['All']:
        for scale_ in ['_mm', '_ss']:
            d_sliders[col_+scale_+'_sort'] = def_slider(
              d_steps[col_+scale_]
            , []
            , [(0.5, button_loc_y+0.005, 0.35), (None, None, None)]
            )
        ##:for scale_
    ##:for col_
    
    for col_ in l_col_base:
        d_sliders[col_] = def_slider(
          d_steps[col_+'_idx']
        , d_steps[col_+'_tar']
        , l_t_slider_loc_
        )
    ##:for col_
    
    for k, v in d_sliders.items(): print(k, list(v[0].keys()), list(v[1].keys()))##
    print()
    
    #### BUTTONS
    print("_____ def buttons _____")
    def def_button(y_, l_viz_, l_sl_):
        y_lbl = y_.replace('_lb','')
        if (y_lbl==tar_): y_lbl = '* ' + y_lbl
        y_lbl = y_lbl.replace('_mm','_MM').replace('_ss','_SS')
        
        if (y_[:3]=='All'):
            if (y_[4:6]=='mm'): y_range_ = [int(t_all_mm_mm[0] - 1.5), int(t_all_mm_mm[1] + 1.5)]####
            if (y_[4:6]=='ss'): y_range_ = [int(t_all_ss_mm[0] - 1.5), int(t_all_ss_mm[1] + 1.5)]####
        else:
            y_min_ = d_df['_idx'][y_+'_ss'].min() if (y_ in l_col_qn) else d_df['_idx'][y_.replace('_lb','')+'_lb_ss'].min()
            y_max_ = d_df['_idx'][y_+'_ss'].max() if (y_ in l_col_qn) else d_df['_idx'][y_.replace('_lb','')+'_lb_ss'].max()
            y_range_ = [int(y_min_ - 1.5), int(y_max_ + 1.5)]
        ##:if
        
        button_ = {
          'name' : '{0}'.format(y_)
        , 'label': '  {0}    '.format(y_lbl if len(y_lbl)<=16 else y_lbl[:12]+".."+y_lbl[-4:])
        , 'method': 'update'
        , 'args': [
            { 'visible': l_viz_}
          , { 'title': title_text
            , 'sliders': l_sl_
            , 'rangeslider': dict(visible=True, thickness=rgslider_len)
            , 'yaxis.range': y_range_
            , 'annotations': [{
                'text': '  "{}"  <br>   Target: "{}"  <br>   Sorted:  Index  '.format(y_, tar_),
                'x': 0.995, 'xanchor': 'right', 'xref': 'paper',
                'y': yaxis_domain[1]-0.015, 'yanchor': 'top', 'yref': 'paper',
                'align': "left",
                'bgcolor': 'None', 'bordercolor': 'royalblue',
                'borderpad': {'l': 15, 'r': 15, 't': 2, 'b': 10},
                'font': {'color': 'royalblue'},
                'showarrow': False,
            }]
            }
          ]
        }
        return button_
    ##:def
    
    l_buttons = []
    for col_ in ['All']:
        for scale_ in ['_mm', '_ss']:
            l_buttons.append(def_button(
              col_+scale_
            , d_l_viz[col_+scale_+'_idx']
            , d_sliders[col_+scale_+'_sort']))
        ##:for scale_
    ##:for col_
    
    for col_ in l_col_base:
        l_buttons.append(def_button(
          col_
        , d_l_viz['_'.join([col_, col_])+'_ss'+'_idx']
        , d_sliders[col_]))
    ##:for col_
    
    for b in l_buttons: print(b['name'], b['label'], b['method'], 'args', [list(d.keys()) for d in b['args']])##
    print()
    
    #### LAYOUT
    if (is_mobile): template_ = 'plotly'##['ggplot2', 'seaborn', 'simple_white', 'plotly', 'plotly_white', 'plotly_dark', 'presentation', 'xgridoff', 'ygridoff', 'gridon', 'none'][int(dt.datetime.now().strftime('%S'))//11]#### テンプレートをランダムで変化
    else: template_ = 'plotly'
    print(template_)##
    
    layout = go.Layout(
        template=template_
    ,   title=dict(
            text=template_
        ,   x=0.015, y=0.985
        ,   xanchor='left', yanchor='top'
        )
    ,   xaxis=dict(title=''
        ,   domain=[0.0, 1.0]
        ,   rangeslider=dict(visible=True, thickness=rgslider_len)
        ####, ticks='', tickmode='array', tickvals=list(range(len(df)))####
        )
    ,   yaxis=dict(title=''
        ,   domain=yaxis_domain
        )
    ,   font=dict(size=fontsize_base*0.75)
    ,   sliders=d_sliders["All_mm_sort"]
    ,   updatemenus=[dict(
            buttons=l_buttons
        ,   x=0.0, xanchor='left'
        ,   y=button_loc_y,   yanchor='top'
        ,   direction=button_direc
        ,   active=-1
        ,   showactive=True)]
    ,   showlegend=False
    )
    # print(layout,'\n')##
    
    ## プロットを作成
    fig_ = go.Figure(dict(data=l_traces, layout=layout))
    
    #print("fig.data"  , fig_.data  ,'\n')##
    #print("fig.layout", fig_.layout,'\n')##
    #print("fig.frames", fig_.frames,'\n')##
    
    print()
    print(dt.datetime.now())
    print("_______________",'\n')
    return fig_
##:def
########################################

############### 対話的な散布図 ###############
## v02c.20230903
def gen_interact_scatter(df, l_ql_, l_qn_, tar_, d_cfg_):
    print("_____ gen_interact_linechart _____")
    print(dt.datetime.now(),'\n')
    
    title     = d_cfg_["title"]
    is_mobile = (__file__[:9] in ["/storage/"])
    
    ## Define Plot Construction
    df, l_col_ql, l_col_qn,_, d_ql_cl,_ = def_plot_const(df, l_ql_, l_qn_, tar_)
    df = df.sort_index()
    
    #### dev*
    df['ind_no'] = df.reset_index().index.values.astype(int)
    l_col_qn = ['ind_no'] + l_col_qn
    print(df['ind_no'].describe())##
    print(l_col_qn)##
    #### *dev
    
    ## Target is Ql/Qn
    tar_is_ql  = (tar_ in l_col_ql)
    
    n_rec      = len(df.index)
    n_ql, n_qn = len(l_col_ql), len(l_col_qn)
    
    #### PLOT
    yaxis_domain  = [0.25, 1.0] if (is_mobile) else [0.0, 0.9]
    button_loc_y  = 0.155 if (is_mobile) else 1.075
    button_direc  = 'up' if (is_mobile) else 'down'
    l_t_slider_loc_ = [(0, 0.12, 1), (0, 0.05, 1)] if (is_mobile) else [(0.2, 1.115, 0.75), (0.2, 1.015, 0.75)]
    fontsize_base = 40 if (is_mobile) else 20
    
    title_text    = '<b>{}</b>  Rec.({})  Col.(QL:{}  QN:{})'.format(title, n_rec, n_ql, n_qn)
    
    #### TRACES
    print("_____ def traces _____")
    def def_trace(df_, x_, c_):
        
        if (tar_is_ql):
            #### dev*
            ####y_ = tar_+"_lb"
            y_ = 'ind_no'
            #### *dev
            arr_fact_ql, l_cl_ = pd.factorize(df_[tar_+'_lb'])
            clrscale_ = px.colors.qualitative.Vivid
            marker_line_ = {
                "width": [8.5 if (is_mobile) else 5.5 for s in arr_fact_ql],
                "color": [clrscale_[s] for s in arr_fact_ql]
            } if (c_ != tar_) else {"width": 1.5, "color": 'white'}
        else:
            y_ = tar_
            marker_line_ = {"width": 1.5, "color": 'white'}
        ##:if (tar_is_ql)
        
        ## color is ql/qn
        if (c_ in l_col_ql):
            arr_fact_ql, l_cl_ = pd.factorize(df_[c_+'_lb'])
            l_cl_, n_cl_ = list(l_cl_), len(l_cl_)
            l_clrscale_ql = [px.colors.qualitative.Vivid, px.colors.qualitative.Plotly] # Vivid Plotly D3 Dark2 Set2 Dark24 Bold # px.colors.sequential.Rainbow #
            if   (n_cl_<=11):    clrscale_ = l_clrscale_ql[0]
            elif (11<n_cl_<=21): clrscale_ = l_clrscale_ql[0] + l_clrscale_ql[1]
            else: clrscale_ = l_clrscale_ql[0] + l_clrscale_ql[1] + l_clrscale_ql[0]*int((n_cl_-21)/11+1)
            
            marker_color_ = {
                  "color": [clrscale_[s] for s in arr_fact_ql]
            }
            hover_bg_ = marker_color_["color"]
        ##:if (c_ in l_col_ql)
        
        if (c_ in l_col_qn):
            marker_color_ = {
                  "color": df_[c_]
                , "colorscale": "Blues"
                , "showscale": False
            }
            hover_bg_ = 'royalblue'
        ##:if (c_ in l_col_qn)
        
        c_hover_ = c_ if (c_ not in [x_, y_]) else l_col_ql[0]
        if (df_[c_hover_].dtype != 'object'): df_['c_str_'] = df_[c_hover_].map('{:.2f}'.format)
        else: df_['c_str_'] = df_[c_hover_].map('\"{}\"'.format)
        
        trace_ = go.Scattergl(##go.Scatter(
            name='{}_{}'.format(x_, c_)
          , x=df_[x_], y=df_[y_]
          , mode='markers'
          , opacity=0.65
          , marker={**marker_color_
            , "line" : marker_line_
            , "size" : fontsize_base*1.0 if (is_mobile) else fontsize_base*1.25
          }
          , hovertemplate=' x: %{x:.2f}, y: %{y:.2f}, c: %{hovertext}'
          , hovertext=df_['c_str_'] +'<br> '\
            + 'index: ' + df_['IDX_'] +'<br> '\
            + 'QL: '    + df_['QL_']  +'<br> '\
            + 'QN: '    + df_['QN_']  +' '
          , hoverlabel={
              "font":{"size": fontsize_base}
            , "namelength": 0
            , "bgcolor": hover_bg_
            }
        )
        
        return trace_
    ##:def
    
    ## l_traces[:] -> ql+qn > mm/ss > idx/tar sort
    l_traces = []
    for qn_ in l_col_qn:
        for qlqn_ in l_col_ql + l_col_qn:
            l_traces.append(def_trace(df, qn_, qlqn_))
        ##:for qnql_
    ##:for c1_
    
    l_traces_name = [l_traces[s]["name"] for s in range(len(l_traces))]
    
    for i in range(len(l_traces_name)): print(i, l_traces_name[i] if not(l_traces_name[i][:len(tar_)]==tar_) else '* '+l_traces_name[i])##
    print()##
    
    #### Viz
    print("_____ def visibles _____")
    d_l_viz = {}
    for qn_ in l_col_qn:
        for qlqn_ in l_col_ql + l_col_qn:
            d_l_viz['_'.join([qn_, qlqn_])] = ['_'.join([qn_, qlqn_])==s[:] for s in l_traces_name]
        ##:for qnql_
    ##:for col_
    
    for k, v in d_l_viz.items(): print(k, len(v), v[:].count(True))##
    print()##
    
    #### STEPS of SLIDERS
    print("_____ def steps of sliders _____")
    def def_step(l_l_vis_, l_step_):
        step_ = []
        for i in range(len(l_l_vis_)):
            ## stats (annotation)
            d_ql_cl_n = {}
            for k, l in d_ql_cl.items():
                for v in l:
                     ql_cl = k+'_'+str(v)
                     q_ = '{} in {}'.format(k, [v])
                     d_ql_cl_n[ql_cl] = len(df.query(q_))
                 ##:for
            ##:for
            
            c_ = l_step_[i]
            if (c_ in l_col_ql):
                if (len(d_ql_cl[c_])<=8):##
                        label_ = [' "{}" (n={}, {:.1f}%) '.format(v[:16], d_ql_cl_n[c_+'_'+v], (d_ql_cl_n[c_+'_'+v]/n_rec)*100) for v in d_ql_cl[c_]]
                        label_text_ = ' {} ({}) <br> {} '.format(c_, len(d_ql_cl[c_]), " <br> ".join(label_))
                else:
                    label_ = [' "{}" (n={}, {:.1f}%) '.format(v[:16], d_ql_cl_n[c_+'_'+v], (d_ql_cl_n[c_+'_'+v]/n_rec)*100) for v in d_ql_cl[c_][:8]]
                    label_text_ = ' {} ({}) <br> {} '.format(c_, len(d_ql_cl[c_]), " <br> ".join(label_)+"<br> ... ")
                ##:if
            else:
                label_ = ['{}  {:>6.2f}'.format(s[0], s[1]) for s in [('avr', df[c_].mean()), ('std', df[c_].std()), ('min', df[c_].min()), ('25%', df[c_].quantile(0.25)), ('med', df[c_].median()), ('75%', df[c_].quantile(0.75)), ('max', df[c_].max())]]
                label_text_ = ' {} <br>  {} '.format(c_, " <br>  ".join(label_))
            ##:if
            
            step_.append({
                'label' : l_step_[i] if (len(l_step_[i])<5) else '{}..'.format(l_step_[i][:4]),
                'method': 'update', ## 'restyle' ##
                'args': [
                    {'visible': l_l_vis_[i]},
                    {'annotations': [{
                        'text': label_text_,
                        'x': 0.995, 'xanchor': 'right', 'xref': 'paper',
                        'y': yaxis_domain[1]-0.015, 'yanchor': 'top', 'yref': 'paper',
                        'align': "left",
                        'bgcolor': 'None', 'bordercolor': 'royalblue',
                        'borderpad': {'l': 15, 'r': 15, 't': 2, 'b': 10},
                        'font': {'color': 'royalblue'},
                        'showarrow': False,
                    }]}
                ],
            })
        ##:for
        return step_
    ##:def
    
    d_steps = {}
    for qn1_ in l_col_qn:
        d_steps[qn1_+'_ql'] = def_step(
            [d_l_viz['_'.join([qn1_, ql_])] for ql_ in l_col_ql],
            l_col_ql
        )
        d_steps[qn1_+'_qn'] = def_step(
            [d_l_viz['_'.join([
                qn1_, 
                qn2_ if (qn2_ not in (qn1_, tar_, l_col_qn[0])) else l_col_ql[0]]
            )] for qn2_ in l_col_qn],
            l_col_qn
        )
    ##:for col_
    
    for k, v in d_steps.items(): print(k, [s['label'] for s in v], len(v[0]['args'][0]['visible']))##
    print()##
    
    #### SLIDERS
    print("_____ def sliders _____")
    def def_slider(sl1_step_, sl2_step_, l_loc_):
        slider_ = [
        { 'steps': sl1_step_
        , 'x': l_loc_[0][0], 'xanchor': 'left'
        , 'y': l_loc_[0][1], 'yanchor': 'top'
        , 'len': l_loc_[0][2]
        , 'minorticklen': fontsize_base*0.5
        , 'active': 0
        , 'currentvalue': {"visible": False}
        , 'bgcolor': "blue"
        , 'activebgcolor': "snow"
        , 'borderwidth': 2.5
        , 'transition': go.layout.slider.Transition(duration=350)
        },
        { 'steps': sl2_step_
        , 'x': l_loc_[1][0], 'xanchor': 'left'
        , 'y': l_loc_[1][1], 'yanchor': 'top'
        , 'len': l_loc_[1][2]
        , 'minorticklen': fontsize_base*0.5
        , 'active': 0
        , 'currentvalue': {"visible": False}
        , 'bgcolor':"blue"
        , 'activebgcolor':"snow"
        , 'borderwidth': 2.5
        , 'transition': go.layout.slider.Transition(duration=350)
        }
        ]
        return slider_
    ##:def
    
    d_sliders = {}
    for c1_ in l_col_qn:
        d_sliders[c1_] = def_slider(
          d_steps[c1_+'_ql']
        , d_steps[c1_+'_qn']
        , l_t_slider_loc_
        )
    ##:for col_
    
    for k, v in d_sliders.items(): print(k, list(v[0].keys()), list(v[1].keys()))##
    print()##
    
    #### BUTTONS
    print("_____ def buttons _____")
    def def_button(x_, l_viz_, l_sl_):
        #### dev*
        ####y_ = tar_+'_lb' if (tar_is_ql) else tar_
        y_ = 'ind_no' if (tar_is_ql) else tar_
        #### *dev
        x_lbl = x_.replace('_lb','')
        if (x_lbl==tar_): x_lbl = '* ' + x_lbl
        
        button_ = {
          'name' : '{0}'.format(x_)
        , 'label': '  {0}    '.format(x_lbl if len(x_lbl)<=16 else x_lbl[:12]+".."+x_lbl[-4:])
        , 'method': 'update'
        , 'args': [
            { 'visible': l_viz_}
          , { 'title': title_text
            , 'xaxis': {'title': x_}
            , 'yaxis': {'title': y_, 'domain': yaxis_domain}
            , 'sliders': l_sl_
            , 'annotations': [{
                'text': '  {}  '.format(l_col_ql[0]),
                'x': 0.995, 'xanchor': 'right', 'xref': 'paper',
                'y': yaxis_domain[1]-0.015, 'yanchor': 'top', 'yref': 'paper',
                'align': "left",
                'bgcolor': 'None', 'bordercolor': 'royalblue',
                'borderpad': {'l': 15, 'r': 15, 't': 2, 'b': 10},
                'font': {'color': 'royalblue'},
                'showarrow': False,
            }]
            }
          ]
        }
        return button_
    ##:def
    
    l_buttons = []
    for qn_ in l_col_qn:
        l_buttons.append(def_button(
          qn_
        , d_l_viz['_'.join([qn_, l_col_ql[0]])]
        , d_sliders[qn_]
        ))
    ##:for col_
    
    for b in l_buttons: print(b['name'], b['label'], b['method'], 'args', [list(d.keys()) for d in b['args']])##
    print()##
    
    #### LAYOUT
    if (is_mobile): template_ = 'plotly'##['ggplot2', 'seaborn', 'simple_white', 'plotly', 'plotly_white', 'plotly_dark', 'presentation', 'xgridoff', 'ygridoff', 'gridon', 'none'][int(dt.datetime.now().strftime('%S'))//11]#### テンプレートをランダムで変化
    else: template_ = 'plotly'
    print(template_)##
    
    layout = go.Layout(
        template=template_
    ,   title=dict(
            text=template_
        ,   x=0.015, y=0.985
        ,   xanchor='left', yanchor='top'
        )
    ,   xaxis=dict(title=l_col_qn[0]
        ,   domain=[0.0, 1.0]
        )
    #### dev*
    ####,   yaxis=dict(title=tar_+"_lb" if (tar_is_ql) else tar_
    ,   yaxis=dict(title='ind_no' if (tar_is_ql) else tar_
    #### *dev
        ,   domain=yaxis_domain
        )
    ,   font=dict(size=fontsize_base*0.75)
    ,   sliders=d_sliders[l_col_qn[0]]
    ,   updatemenus=[dict(
            buttons=l_buttons
        ,   x=0.0, xanchor='left'
        ,   y=button_loc_y,   yanchor='top'
        ,   direction=button_direc
        ,   active=-1
        ,   showactive=True)]
    ,   showlegend=False
    )
    # print(layout,'\n')##
    
    ## プロットを作成
    fig_ = go.Figure(dict(data=l_traces, layout=layout))
    
    #print("fig.data"  , fig_.data  ,'\n')##
    #print("fig.layout", fig_.layout,'\n')##
    #print("fig.frames", fig_.frames,'\n')##
    
    print()
    print(dt.datetime.now())
    print("_______________",'\n')
    return fig_
##:def
########################################

############### 対話的な箱ひげ、度数分布 ###############
## v01b.20230709
def gen_interact_dist(df, l_ql_, l_qn_, tar_, d_cfg_):
    print("_____ gen_interact_distribution_chart _____")
    print(dt.datetime.now(),'\n')
    
    title     = d_cfg_["title"]
    is_mobile = (__file__[:9] in ["/storage/"])
    
    ## Define Plot Construction
    df, l_col_ql, l_col_qn,_, d_ql_cl,_ = def_plot_const(df, l_ql_, l_qn_, tar_)
    
    n_rec      = len(df.index)
    n_ql, n_qn = len(l_col_ql), len(l_col_qn)
    boxpoints_ = d_cfg_.get("boxpoints", 'all')
    
    ## Target is Ql/Qn
    tar_is_ql  = (tar_ in l_col_ql)
    if (tar_is_ql): qn_y = l_col_qn[0]
    else:           qn_y = tar_
    
    d_ql_cl_n = {}
    for k, l in d_ql_cl.items():
         for v in l:
             ql_cl = k+'_'+str(v)
             q_ = '{} in {}'.format(k,[v])
             d_ql_cl_n[ql_cl] = len(df.query(q_))
         ##:for
    ##:for
    
    #### PLOT
    title_text    = '<b>{}</b>  Rec.({})  Col.(QL:{}  QN:{})'.format(title, n_rec, n_ql, n_qn)
    
    yaxis_domain  = [0.15, 1.0] if (is_mobile) else [0.0, 0.975] # [0.25, 1.0]
    button_loc_y  = 0.075 if (is_mobile) else 1.075 # 0.155
    button_direc  = 'up' if (is_mobile) else 'down'
    l_t_slider_loc_ = [(0, 0.035, 1)] if (is_mobile) else [(0.2, 1.105, 0.75)] # (0, 0.12, 1)
    fontsize_base = 40 if (is_mobile) else 20
    
    #### TRACES
    print("_____ def traces _____")
    def def_trace_vio(df_, qn_, ql_):
        trace_ = go.Violin(##side='positive',
          y=df_[qn_]
        , x=df_[ql_] if (ql_ != '') else [qn_]*len(df_)
        , fillcolor=None
        , line_color=None
        , points='all' ## 'outliers'
        , box={
          "visible": True,
          "fillcolor": None,
          "line":{"width": 2.5, "color": None},
          "width": 0.45
          }
        , hovertemplate=' '\
          + 'Val: %{y:.3f}' + '    '\
          + 'Cat: %{x}'     + ' <br>'\
          + '%{hovertext}'
        , hovertext=' '\
          + 'index: ' + df['IDX_'] + '<br> '\
          + 'QL: '    + df['QL_']  + '<br> '\
          + 'QN: '    + df['QN_']  + ' '
        , hoverlabel={"font": {"size": fontsize_base}, "namelength": 0}
        )
        return trace_
    ##:def
    
    def def_trace_box(df_, qn_, ql_):
        trace_ = go.Box(
          x=df_[ql_], y=df_[qn_]
        , boxmean=True
        , boxpoints=boxpoints_
        , whiskerwidth=1.0
        )
        return trace_
    ##:def
    
    def def_trace_hist(df_, qn_, ql_):
        trace_ = go.Histogram(x=df_[qn_])
        return trace_
    ##:def
    
    l_traces = []
    for qn_x in l_col_qn:
        for ql in ['']+l_col_ql:
            l_traces.append(def_trace_vio(df, qn_x, ql))
        ##:for
    ##:for
    
    #### STEPS of SLIDERS
    print("_____ def steps of sliders, visibles _____")
    def def_step(x_, l_ql_):
        step_ = []
        for ql_ in l_ql_:
            sl_lbl = '* '+ql_ if (ql_==tar_) else ql_
            
            if (ql_ != ''):
                if (len(d_ql_cl[ql_])<=8):##
                    label_      = ['"{}" (n={}, {:.1f}%)'.format(v, d_ql_cl_n[ql_+'_'+v], (d_ql_cl_n[ql_+'_'+v]/n_rec)*100) for v in d_ql_cl[ql_]]
                    label_text_ = ' {} ({}) <br> {}'.format(ql_, len(d_ql_cl[ql_]), " <br> ".join(label_))
                else:
                    label_      = ['"{}" (n={}, {:.1f}%)'.format(v, d_ql_cl_n[ql_+'_'+v], (d_ql_cl_n[ql_+'_'+v]/n_rec)*100) for v in d_ql_cl[ql_][:8]]
                    label_text_ = ' {} ({}) <br> {}'.format(ql_, len(d_ql_cl[ql_]), " <br> ".join(label_)+"<br> ... ")
                ##:if
            ##:if
            
            annotations_ = [dict(
              text=label_text_ if (ql_ != '') else ql_
            , xref='paper', x=0.995
            , yref='paper', y=yaxis_domain[1]-0.015, yanchor='top'
            , bgcolor='None', bordercolor='royalblue', borderpad=dict(l=15, r=15, t=2, b=10)
            , font={"color": 'royalblue'}
            , align='left'
            , showarrow=False)]
            
            step_.append({ 'label': sl_lbl
            , 'method': 'update'
            , 'args': [
                {'visible': [(k+'_'+j==x_+'_'+ql_) for k in l_col_qn for j in ['']+l_col_ql]}
              , { 'xaxis': {'title': ql_, "domain": [0.0,1.0]
                , "tickmode": 'array'
                , "tickvals": None
                , "ticktext": None
                }
                , 'annotations': annotations_
                  }
              , {'title': ql_}
              ]
            })
        ##:for
        return step_
    ##:def
    
    d_steps = {}
    for qn_x in l_col_qn: d_steps[qn_x] = def_step(qn_x, ['']+l_col_ql)
    
    #### SLIDERS
    print("_____ def sliders _____")
    def def_slider(x_, l_loc_):
        slider_ = [{
          'x': l_loc_[0][0], 'xanchor': 'left'
        , 'y': l_loc_[0][1], 'yanchor': 'top'
        , 'len': l_loc_[0][2]
        , 'minorticklen': fontsize_base*0.5
        , 'active': 0, 'currentvalue': {"visible": False}
        , 'bgcolor':"blue", 'activebgcolor':"snow"
        , 'borderwidth': 2.5
        , 'transition': go.layout.slider.Transition(duration=350)
        , 'steps': d_steps[x_]
        }]
        return slider_
    ##:def
    
    d_sliders = {}
    for qn_x in l_col_qn: d_sliders[qn_x] = def_slider(qn_x, l_t_slider_loc_)
    
    #### BUTTONS
    print("_____ def buttons _____")
    def def_button(y_, ql_):
        y_lbl = y_.replace('_lb','')
        if (y_lbl==tar_): y_lbl = '* ' + y_lbl
        
        if (len(d_ql_cl[ql_])<=8):##
            label_      = ['"{}" (n={}, {:.1f}%)'.format(v, d_ql_cl_n[ql_+'_'+v], (d_ql_cl_n[ql_+'_'+v]/n_rec)*100) for v in d_ql_cl[ql_]]
            label_text_ = ' {} ({}) <br> {}'.format(ql_, len(d_ql_cl[ql_]), " <br> ".join(label_))
        else:
            label_      = ['"{}" (n={}, {:.1f}%)'.format(v, d_ql_cl_n[ql_+'_'+v], (d_ql_cl_n[ql_+'_'+v]/n_rec)*100) for v in d_ql_cl[ql_][:8]]
            label_text_ = ' {} ({}) <br> {}'.format(ql_, len(d_ql_cl[ql_]), " <br> ".join(label_)+"<br> ... ")
        ##:if
        
        annotations_ = [dict(
          text=label_text_
        , xref='paper', x=0.995
        , yref='paper', y=yaxis_domain[1]-0.015, yanchor='top'
        , bgcolor='None', bordercolor='royalblue', borderpad=dict(l=15, r=15, t=2, b=10)
        , font={"color":'royalblue'}
        , align='left'
        , showarrow=False)]
        
        button_ = {
          'name' : '{0}'.format(y_)
        , 'label': '  {0}    '.format(y_lbl if len(y_lbl)<=16 else y_lbl[:12]+".."+y_lbl[-4:])
        , 'method': 'update'
        , 'args': [
            { 'visible': [(k+'_'+j==y_+'_'+'') for k in l_col_qn for j in ['']+l_col_ql]}
          , { 'title': title_text
            , 'xaxis': {'title': ql_, "domain": [0.0,1.0]
              , "tickmode": 'array'
              , "tickvals": None
              , "ticktext": None
              }
            , 'yaxis': {'title': y_ , "domain": yaxis_domain}
            , 'annotations': annotations_
            , 'sliders': [d_sliders[y_][0]]
            }
          ]
        }
        return button_
    ##:def
    
    l_buttons = []
    for qn in l_col_qn: l_buttons.append(def_button(qn, l_col_ql[0]))
    
    #### LAYOUT
    if (is_mobile): template_ = 'plotly'##['ggplot2', 'seaborn', 'simple_white', 'plotly', 'plotly_white', 'plotly_dark', 'presentation', 'xgridoff', 'ygridoff', 'gridon', 'none'][int(dt.datetime.now().strftime('%S'))//11]#### テンプレートをランダムで変化
    else: template_ = 'plotly'
    
    layout = go.Layout(
        template=template_
    ,   title=dict(
            text=template_
        ,   x=0.015, y=0.985
        ,   xanchor='left', yanchor='top')
    ,   xaxis=dict(title=''  , domain=[0.0,1.0])
    ,   yaxis=dict(title=qn_y, domain=yaxis_domain)
    ,   font=dict(size=fontsize_base*0.75)
    ,   sliders=None
    ,   updatemenus=[dict(
            buttons=l_buttons
        ,   x=0.0, xanchor='left'
        ,   y=button_loc_y, yanchor='top'
        ,   direction=button_direc
        ,   active=-1
        ,   showactive=True)]
    ,   showlegend=False
    )
    
    ## プロットを作成
    fig_ = go.Figure(dict(data=l_traces, layout=layout))
    #print("fig.data"  , fig_.data  ,'\n')##
    #print("fig.layout", fig_.layout,'\n')##
    #print("fig.frames", fig_.frames,'\n')##
    
    print()
    print(dt.datetime.now())
    print("_______________",'\n')
    return fig_
##:def
########################################

############### 対話的な重ね合わせ波形 ############
def gen_interact_layered_wave(df, l_col_ql, l_col_qn, tar_, d_cfg_):
    
    title = d_cfg_["title"]
    is_mobile = (__file__[:9] in ["/storage/"])
    
    ql_to_v_cl    = d_cfg_.get("ql_to_v_cl", [''])
    ql_to_v_cl_is = (ql_to_v_cl[0] not in [None, ''])
    
    ## Define Plot Construction
    df, l_col_ql, l_col_qn, l_qn_ql_cl_, d_ql_cl, d_ql_cl_wvdf = def_plot_const(df, l_col_ql, l_col_qn, tar_)
    
    n_rec      = len(df.index)
    n_ql, n_qn = len(l_col_ql), len(l_col_qn)
    n_wave     = int( sum([len(v) for k,v in d_ql_cl_wvdf.items()]) / n_ql )
    avr_wv_len = int( sum([len(d_ql_cl_wvdf[k][0]) if (0<len(d_ql_cl_wvdf[k])) else 0 for k in list(d_ql_cl_wvdf.keys())]) )
    print(title, n_wave, avr_wv_len, n_rec, n_ql, n_qn, tar_, '\n')##
    
    d_qn_range = {}
    if (ql_to_v_cl_is):
        def calc_range_(d_, pct_):
            min_ = d_.min()
            max_ = d_.max()
            rng_ = abs(max_ - min_)
            min_, max_ = min_ - rng_*pct_, max_ + rng_*pct_
            return (min_, max_) 
        ##:der
        
        d_qn_range['x'] = calc_range_(df.query('WVPT !=-1')['WVPT'], 0.05)
        
        for qn_ in l_col_qn:
            d_qn_range[qn_+'_y'] = calc_range_(df[qn_], 0.05)
        ##:for
    ##:if
    
    #### PLOT
    yaxis_domain  = [0.25, 1.0] if (is_mobile) else [0.0, 0.9]
    button_loc_y  = 0.155 if (is_mobile) else 1.075
    button_direc  = 'up' if (is_mobile) else 'down'
    l_t_slider_loc_ = [(0, 0.12, 1), (0, 0.05, 1)] if (is_mobile) else [(0.2, 1.115, 0.75), (0.2, 1.015, 0.75)]
    fontsize_base = 40   if (is_mobile) else 20
    
    title_text    = '<b>{}</b>  Wave({})  Col.(QL:{}  QN:{})'.format(title, n_wave, n_ql, n_qn)
    print(title_text)##
    
    #### TRACES, Viz ####
    print("_____ def traces, visibles _____")
    def def_trace(df_, var_, t_cl_):
        df_ = df_.reset_index()
        
        width_ = 1.5 ##if (var_==tar_) else 1.5
        msize_ = 5.5 ##if (var_==tar_) else 8.5
        
        trace_ = go.Scattergl(####go.Scatter(
            name='{}'.format(var_)
          , x=df_.index
          , y=df_[var_]
          , mode='markers+lines'
          , line={"color": t_cl_[1], "width": width_}
          , marker={"size": msize_, "symbol": "circle-open-dot"}
          , hovertemplate=' '\
            + 'Value: x: %{x},  y: %{y},  ' + '%{hovertext}'
          , hovertext='c: ' + df_[t_cl_[0]] + ' <br> '\
            + 'Index: ' + df_['IDX_'] + ' <br> '\
            + 'QL: '    + df_['QL_']  + ' <br> '\
            + 'QN: '    + df_['QN_']  + ' '
          , hoverlabel={"font": {"size":fontsize_base}, "namelength": 0, "bgcolor": t_cl_[1]}
        )
        return trace_
    ##:def
    
    d_l_viz  = {}
    for qn_, ql_ in itr.product(l_col_qn, l_col_ql):
        qn_ql_ = qn_+'_'+ql_
        d_l_viz[qn_ql_] = [(qn_,ql_)==t[:2] for t in l_qn_ql_cl_]
        
        for cl_ in d_ql_cl[ql_]:
            qn_ql_cl_ = qn_+'_'+ql_+'_'+ cl_
            d_l_viz[qn_ql_cl_] = [(qn_,ql_,cl_)==t[:3] for t in l_qn_ql_cl_]
        ##:for
    ##:for
    #for k, v in d_l_viz.items(): print(k, len([s for s in v if (s)]))##
    
    c_scale_qual = px.colors.qualitative.D3 # D3 Plotly Vivid Dark2 Set2 Dark24 Bold
    c_scale_seq  = px.colors.sequential.Rainbow #
    
    l_traces = []
    for qn_, ql_ in itr.product(l_col_qn, l_col_ql):
        qn_ql_ = qn_+'_'+ql_
        
        for i_cl_ in range(len(d_ql_cl[ql_])):
            cl_    = d_ql_cl[ql_][i_cl_]
            n_cl_  = len(d_ql_cl[ql_])
            qn_ql_cl_ = qn_+'_'+ql_+'_'+cl_
            ql_cl_    = ql_+'_'+cl_
            if (n_cl_ <= len(c_scale_qual)): cl_color = c_scale_qual[i_cl_]
            else: cl_color = c_scale_seq[int((i_cl_/n_cl_)*(len(c_scale_seq)-1))]
            
            for d_ in d_ql_cl_wvdf[ql_cl_]: l_traces.append(def_trace(d_, qn_, (ql_, cl_color)))
        ##:for
    ##:for
    
    #### STEPS of SLIDERS
    print("_____ def steps of sliders _____")
    def def_step(qn_, l_ql_, set_cl_):
        step_ = []
        l_ = ql_to_v_cl if (ql_to_v_cl_is) else l_ql_
        
        for ql_ in l_:
            if not(set_cl_):
                l_ql_v_ = ['']
                class_ = ['"{}"'.format(cl_) for cl_ in d_ql_cl[ql_]]
                anno_text_ = ' {} ({}): {} '.format(ql_, len(class_), " / ".join(class_))
            else:
                l_ql_v_ = d_ql_cl[ql_]
            ##:if not(set_cl_)
            
            for cl_ in l_ql_v_:
                if (cl_ not in d_ql_cl[ql_]):
                    ql_cl_    = ql_
                    sl_label  = ql_[:8]
                else:
                    ql_cl_    = ql_+'_'+cl_
                    sl_label  = ql_[:2]+'_'+str(cl_)[:2]
                ##:if
                qn_ql_cl_ = qn_+'_'+ql_cl_
                
                if (set_cl_):
                    if (0 < len(d_ql_cl_wvdf[ql_cl_])):
                        srs_  = pd.concat(d_ql_cl_wvdf[ql_cl_])[qn_]
                        stat_ = ['{}  {:>6.2f}'.format(s[0], s[1]) for s in [
                            ('avr', srs_.mean()),
                            ('std', srs_.std()),
                            ('min', srs_.min()),
                            ('25%', srs_.quantile(0.25)),
                            ('med', srs_.median()),
                            ('75%', srs_.quantile(0.75)),
                            ('max', srs_.max())]
                        ]
                        anno_text_ = ' {}  "{}" <br>  {} '.format(ql_, cl_, " <br>  ".join(stat_))
                    else:
                        anno_text_ = ' {}  "{}" '.format(ql_, cl_)
                    ##:if (0<len(srs_))
                ##:if (set_cl_)
                
                y_min_ = df.query('WVID != -1')[qn_].min()
                y_max_ = df.query('WVID != -1')[qn_].max()
                y_rng_ = abs(y_max_ - y_min_)
                y_range_ = [int(y_min_ - y_rng_*0.05), int(y_max_ + y_rng_*0.15)]
                
                step_.append({
                  'label' : sl_label
                , 'method': 'update'
                , 'args': [
                    {'visible': d_l_viz[qn_ql_cl_]}
                  , {'xaxis': {'title': ''}
                    , 'yaxis.range': y_range_
                    , 'annotations': [{
                        'text': anno_text_
                      , 'x': 0.995, 'xanchor': 'right', 'xref': 'paper'
                      , 'y': yaxis_domain[1]-0.015, 'yanchor': 'top', 'yref': 'paper'
                      , 'align': "left"
                      , 'bgcolor': 'None', 'bordercolor': 'royalblue'
                      , 'borderpad': {'l': 15, 'r': 15, 't': 2, 'b': 10}
                      , 'font': {"color": 'royalblue'}
                      , 'showarrow': False
                      }]
                    }
                  , {'title': ql_+'_'+cl_}
                  ]
                })
            ##:for cl_
        ##:for ql_
        return step_
    ##:def
    
    d_steps = {}
    for qn_, ql_ in itr.product(l_col_qn, l_col_ql):
        d_steps['_'.join([qn_, ql_, '__'])] = def_step(qn_, l_col_ql[:2], False)
        d_steps['_'.join([qn_, ql_, 'CL'])] = def_step(qn_, l_col_ql[:2], True)
    ##:for qn_, ql_
    
    #### SLIDERS
    print("_____ def sliders _____")
    def def_slider(sl1_step_, sl2_step_, l_loc_):
        slider_ = [
        { 'steps': sl1_step_
        , 'x': l_loc_[0][0], 'xanchor': 'left'
        , 'y': l_loc_[0][1], 'yanchor': 'top'
        , 'len': l_loc_[0][2] if (5<=len(sl1_step_)) else 0.1*len(sl1_step_)
        , 'minorticklen': fontsize_base*0.5
        , 'active': 0
        , 'currentvalue': {"visible": False}
        , 'bgcolor': "blue"
        , 'activebgcolor': "snow"
        , 'borderwidth': 2.5
        , 'transition': go.layout.slider.Transition(duration=350)
        },
        { 'steps': sl2_step_
        , 'x': l_loc_[1][0], 'xanchor': 'left'
        , 'y': l_loc_[1][1], 'yanchor': 'top'
        , 'len': l_loc_[1][2] if (5<=len(sl2_step_)) else 0.1*len(sl2_step_)
        , 'minorticklen': fontsize_base*0.5
        , 'active': 0
        , 'currentvalue': {"visible": False}
        , 'bgcolor':"blue"
        , 'activebgcolor':"snow"
        , 'borderwidth': 2.5
        , 'transition': go.layout.slider.Transition(duration=350)
        }
        ]
        return slider_
    ##:def
    
    d_sliders = {}
    for c1_ in l_col_qn:
        for c2_ in l_col_ql:
            d_sliders[c1_+'_'+c2_] = def_slider(
              d_steps['_'.join([c1_, c2_, '__'])]
            , d_steps['_'.join([c1_, c2_, 'CL'])]
            , l_t_slider_loc_
            )
        ##:for c2_
    ##:for c1_
    
    #### BUTTONS
    print("_____ def buttons _____")
    def def_button(qn_, l_viz_, l_sl_):
        y_lbl = qn_.replace('_lb','')
        if (y_lbl==tar_): y_lbl = '* ' + y_lbl
        
        button_ = {
          'name' : '{0}'.format(qn_)
        , 'label': '  {0}      '.format(y_lbl)
        , 'method': 'update'
        , 'args': [
            { 'visible': l_viz_}
          , { 'title': title_text
            , 'xaxis': {'title': ''}
            , 'yaxis': {'title': '', 'domain': yaxis_domain}
            , 'sliders': d_sliders[qn_ql_]
            }
          ]
        }
        return button_
    ##:def
    
    l_buttons = []
    for qn_, ql_ in itr.product(l_col_qn, l_col_ql[0:1]):
        qn_ql_ = qn_+'_'+ql_
        l_buttons.append(def_button(qn_, d_l_viz[qn_ql_], d_sliders[qn_ql_]))
    ##:for qn_, ql_
    
    #### LAYOUT
    if (is_mobile): template_ = 'plotly'##['ggplot2', 'seaborn', 'simple_white', 'plotly', 'plotly_white', 'plotly_dark', 'presentation', 'xgridoff', 'ygridoff', 'gridon', 'none'][int(dt.datetime.now().strftime('%S'))//11]#### テンプレートをランダムで変化
    else: template_ = 'plotly'
    
    qn_ql_ = l_col_qn[0]+'_'+l_col_ql[0]
    layout = go.Layout(
        template=template_
    ,   title=dict(
            text=template_
        ,   x=0.015, y=0.985
        ,   xanchor='left', yanchor='top')
    ,   xaxis=dict(title='', domain=[0.00, 1.0])
    ,   yaxis=dict(title=l_col_qn[0], domain=yaxis_domain)
    ,   font=dict(size=fontsize_base*0.75)
    ,   sliders=d_sliders[qn_ql_]
    ,   updatemenus=[dict(
            buttons=l_buttons
        ,   x=0.0, xanchor='left'
        ,   y=button_loc_y,   yanchor='top'
        ,   direction=button_direc
        ,   active=-1
        ,   showactive=True)]
    ,   showlegend=False
    )
    
    ## プロットを作成
    fig_ = go.Figure(dict(data=l_traces, layout=layout))
    
    #print("fig.data"  , fig_.data  ,'\n')##
    #print("fig.layout", fig_.layout,'\n')##
    #print("fig.frames", fig_.frames,'\n')##
    
    print()
    print(dt.datetime.now())
    print("_______________",'\n')
    return fig_
##:def
########################################

############### データ準備 ###############
## v01a.20230616
## df, targetname, featurename, IsQualittv, IsQuanttv = prepare_wave_data(title_)
def prepare_wave_data(TITLE_):
    import AnalyzeFunctionsWV    as AFPW
    print(dt.datetime.now(), "[START] PREPARE DATA", '\n')
    
    ## df_, l_tar_, l_fea_, l_ql_, l_qn_
    if (TITLE_ in ["Tb_wv"]):
        df_wv,_,_,_ = AFPW.Tb_CUTOUT({"rule":"UpTo","ThLw":20,"offset":0},{"rule":"DwTo","ThUp":20,"offset":20},20)#{"rule":"UpTo","ThLw":100,"offset":10} {"rule":"DwTo","ThUp":60,"offset":20}
        df_wv['Val2'] = abs(df_wv['Val'] - 110)
        df_wv['CAT'], df_wv['CAT2'] = df_wv['WVID'] % 5, df_wv['WVID'] % 3
        l_tar_, l_fea_ = ['CL'], ['Val','Val2','CAT','CAT2']
        l_ql_, l_qn_   = ['CL','CAT','CAT2'], ['Val','Val2']
        
    elif (TITLE_ in ["Sensor_wv"]):
        df_wv,_,_,_ = AFPW.Sensor_CUTOUT('','','')
        l_tar_, l_fea_ = ['anomaly'], ['sendt1','lot']
        l_ql_, l_qn_   = ['anomaly','lot'], ['sendt1']
        
    elif (TITLE_ in ["Sin_wv"]):
        df_wv,_,_,_ = AFPW.Sin_CUTOUT({"rule":'FLG',"offset":25},{"rule":'FLG',"offset":50},'')
        
        df_wv['CAT']  = df_wv['WVID'] % 5
        l_tar_, l_fea_ = ['CL'], ['Sin','Cos','Tan',] # 'LOT'
        l_ql_, l_qn_   = ['CL','CAT'], ['Sin','Cos','Tan']
    ##:if
    
    print()
    print(dt.datetime.now(),"[END] PREPARE DATA",'\n')
    return df_wv, l_tar_, l_fea_, l_ql_, l_qn_
########################################

########################################
if (__name__ == "__main__"):
    import AnalyzeFunctionsVZ_UI as AFVU
    FILENAME = os.path.basename(__file__)
    DIRNAME  = os.path.dirname(__file__)
    OUTDIR = DIRNAME+"/output/" ## "/DIO/"+title+"/output/"
    if not(os.path.exists(OUTDIR)): os.makedirs(OUTDIR)
##:if

## データ準備
if (__name__ == "__main__"):
    print(dt.datetime.now(),'\n')
    import AnalyzeFunctionsDT as AFD
    
    title   = "MPG" ## Iris Wine MPG Ames AWB ATTRI STL TTN GLSS FrFr BNT Sensor Tb Tg Sin DataXn
    is_wave = (title[-3:]=='_wv') ## Tb_wv Sensor_wv Sin_wv
    
    gen_layer = (is_wave)    ## True False
    gen_dist  = False##not(is_wave) ## 
    gen_line  = False##not(is_wave) ##
    gen_scat  = not(is_wave) ## False##
    plot_show = "layer" if (is_wave) else "scat" ## layer dist line scat
    save_plot = True ## True False
    
    mabiki = 1 ##
    exe_pca, n_com = False, 6 ## True False
    exe_mt     = False ## True False
    calc_stats = True ## True False
    importance = True ## True False
    
    if not(title[-3:]=='_wv'): fea, tar, df, Ql, Qn,_ = AFD.PrepareData(title)
    if    (title[-3:]=='_wv'): df, tar, fea, Ql, Qn  = prepare_wave_data(title)
    
    df = df[::mabiki]
    
    if   (tar[0] in Ql):
        Ql.remove(tar[0])
        Ql = tar + Ql[:64]##
        Qn = Qn[:64]##
    elif (tar[0] in Qn):
        Qn.remove(tar[0])
        Qn = tar + Qn[:64]##
        Ql = Ql[:64]##
    ##:if
    
    print("df:", len(df), ",", len(df.columns))##
    print("target :", tar)##
    print("feature:", len(fea), fea)##
    print("IsQualittv:", len(Ql), Ql)##
    print("IsQuanttv :", len(Qn), Qn)##
    
    print()
    print(dt.datetime.now(),'\n')
##:if

## 前処理
if (__name__ == "__main__"):
    print(dt.datetime.now(),'\n')
    import AnalyzeFunctionsUS as AFU
    
    if (exe_pca): ## PCA
        title = title + "(pca)"
        fea_ql, fea_qn = copy.deepcopy(Ql), copy.deepcopy(Qn)
        if (tar[0] in fea_ql): fea_ql.remove(tar[0])
        if (tar[0] in fea_qn): fea_qn.remove(tar[0])
        
        _,df_pca,_ = AFU.PrimaryComponentAnalysis(df[fea_qn], n_com)
        
        if ('WVID' in df.columns):
            df = pd.concat([df[['WVID', 'WVPT']], df[fea_ql], df_pca, df[tar]], axis=1)
        else:
            df = pd.concat([df[fea_ql], df_pca, df[tar]], axis=1)
        ##:if
        
        if (tar[0] in Qn):
            Qn  = copy.deepcopy(list(df_pca.columns) + tar)
            fea = fea_ql + Qn[:-1]
        else:
            Qn  = copy.deepcopy(list(df_pca.columns))
            fea = fea_ql + Qn
        ##:if
    ##:if
    
    if (exe_mt): ## MT
        title = title + "(mt)"
        Ql_fea = [s for s in Ql if (s != tar[0])]
        Qn_fea = [s for s in Qn if (s != tar[0])]
        
        dic_us  = AFU.mahalanobis_unitspace(df[Qn_fea])
        _,df_sn = AFU.mahalanobis_distance(dic_us, df[Qn_fea])
        
        for c in Qn_fea+['sn_max_val']: df_sn[c].mask(df_sn[c] < 0, 0, inplace=True)##
        for c in Qn_fea: df_sn.rename(columns={c: c+'_sn'}, inplace=True)
        
        if ('WVID' in df.columns):
            df = pd.concat([df[['WVID', 'WVPT']], df[tar+Ql_fea], df_sn], axis=1)
        else:
            df = pd.concat([df[tar+Ql_fea+Qn_fea], df_sn], axis=1)
        ##:if
        
        Ql.append('sn_max_col')
        Qn.insert(0, 'm_dis')
        
        df['m_dis_outlier'] = [0 if (s <= df['m_dis'].quantile(0.75)) else 1 for s in df['m_dis']]##
        Ql.append('m_dis_outlier')##
        ##df = df.query('m_dis_outlier==0')####
        ##title = title + "(rm_outlier)"####
    ##:if
    
    print()
    print(dt.datetime.now(),'\n')
##:if

## 統計量・Importance
if (__name__ == "__main__") and (calc_stats):
    print(dt.datetime.now(),'\n')
    import AnalyzeFunctionsPP as AFP
    
    AFP.CALC_STATS(df.dropna(subset=Qn), Ql, Qn, {"outdir": OUTDIR, "show": True, "save": True, "title": title, "pca": True, "importance": True})
    
    print()
    print(dt.datetime.now(),'\n')
##:if

## 可視化（波形重ね合わせ）
if (__name__ == "__main__") and (gen_layer):
    print(dt.datetime.now(),'\n')
    
    fig = gen_interact_layered_wave(df, Ql, Qn, tar[0], {"title": title
    , "ql_to_v_cl": Ql[:2]})## クラスごとにプロット
    
    if (save_plot): plotly.offline.plot(fig, auto_open=False, filename=OUTDIR+title+'_Layered.html')
    if (plot_show=='layer'): fig.show()
    
    print()
    print(dt.datetime.now(),'\n')
##:if

## 可視化（箱ひげ→分布）
if (__name__ == "__main__") and (gen_dist):
    print(dt.datetime.now(),'\n')
    
    Ql_bx = ['WVID']+Ql if ('WVID' in df.columns) else Ql
    df_bx = df.query('WVID != -1') if ('WVID' in df.columns) else df
    
    fig = AFVU.gen_interact_dist(df_bx, Ql_bx, Qn, tar[0], {"title": title})
    
    if (save_plot): plotly.offline.plot(fig, auto_open=False, filename=OUTDIR+title+'_Dist.html')
    if (plot_show=='dist'): fig.show()
    
    print()
    print(dt.datetime.now(),'\n')
##:if

## 可視化（散布図→変数間の関係性）
if (__name__ == "__main__") and (gen_scat):
    print(dt.datetime.now(),'\n')
    
    fig = AFVU.gen_interact_scatter(df, Ql, Qn, tar[0], {"title": title})
    
    if (save_plot): plotly.offline.plot(fig, auto_open=False, filename=OUTDIR+title+'_Scatter.html')
    if (plot_show=='scat'): fig.show()
    
    print()
    print(dt.datetime.now(),'\n')
##:if

## 可視化（折れ線→時系列・値順の傾向）
if (__name__ == "__main__") and (gen_line):
    print(dt.datetime.now(),'\n')
    
    fig = AFVU.gen_interact_linechart(df, Ql, Qn, tar[0], {"title": title})
    
    if (save_plot): plotly.offline.plot(fig, auto_open=False, filename=OUTDIR+title+'_Line.html')
    if (plot_show=='line'): fig.show()
    
    print()
    print(dt.datetime.now(),'\n')
##:if
########################################
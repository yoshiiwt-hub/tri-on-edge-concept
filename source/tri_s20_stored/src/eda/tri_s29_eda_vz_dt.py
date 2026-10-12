# SPDX-License-Identifier: MIT
# Copyright (c) 2026 IWATA,Yoshitaka
# https://github.com/yoshiiwt-hub/tri-on-edge-concept
#################### FIX NOTE (2026.08.09  IWATA,Y.) ##########
# tri_edge_22_eda_visualizer.py 経由での実行時、以下の既存バグ・改善を確認・修正した：
#  ①visualiseData() の 'hist' 分岐: ymin_/ymax_ 計算で DF[LstColumn[:1]]（スライス=DataFrame）
#    に .min()/.max() をかけていたため、float()変換でSeriesを渡すことになりTypeErrorになっていた。
#    DF[LstColumn[0]]（単一列名=Series）に修正（231行目付近の書き方と統一）。
#  ②gen_viz_report() 内 heatmap_corr(): AFP.CALC_STATS(df[...], ...) の df が未定義変数
#    （NameError）だった。gen_viz_report() の引数 df_ の参照漏れ（タイポ）。df_ に修正。
#  ③gen_viz_report(): 従来はグラフ生成のどれか1つが例外を投げると、そこで処理全体が停止し、
#    最後に実行されるExcel出力（image_to_excel）まで一切到達できなかった（画像は生成されて
#    いてもExcelレポートが0件になる）。各グラフ生成呼び出しを _safe() ラッパーで保護し、
#    失敗したものはログを出してスキップ・Noneとして扱い、成功した分だけで後続処理・
#    Excel出力を続行するようにした。Excel出力ループ自体（グループ単位の image_to_excel
#    呼び出し）も同様にtry/exceptで保護し、1グループの埋め込み失敗が他グループの
#    出力を妨げないようにした。
#  ④gen_viz_report() 冒頭で AnalyzeFunctionsVZ_DT(AFVD)・AnalyzeFunctionsPP(AFP) はimport
#    されていたが、viz_pca_vec() が使う AFU.PrimaryComponentAnalysis() の AnalyzeFunctionsUS
#    （AFU）がimportされておらず NameError になっていた。import AnalyzeFunctionsUS as AFU を追加。
#  ⑤gen_viz_report(): 目的変数(tar_)が未設定（None）の場合、tar_を使うグラフ生成が失敗するが、
#    _safe() のエラーメッセージが元の例外内容（"None"等）だけでは原因が分かりにくかったため、
#    tar_ is None の場合は「目的変数(objective)が未設定なため、この処理は実行できなかった
#    可能性があります」というヒントをメッセージに追加するようにした。
################################################################
#################### IMPORT LIBRARY ##########
##im##port collections   #カウント
##from   contextlib im##port redirect_stdout
import copy             #リストのコピー
import traceback        #エラー時のスタックトレース表示（gen_viz_report内の_safe()ラッパー用）
import datetime as dt   #時間、時刻
import gc               #メモリクリア
import itertools as itr #組み合わせ
import numpy as np
np.set_printoptions(precision=3, suppress=True)
np.set_printoptions(threshold=20)
np.set_printoptions(edgeitems=5)
import math
import os
import pandas as pd
pd.options.display.precision    = 3
pd.options.display.float_format = '{:.2f}'.format
pd.options.display.max_rows     = None
pd.options.display.max_columns  = None
pd.options.display.show_dimensions = True
from   scipy.stats import gaussian_kde
from   sklearn import preprocessing as pp
##from   sklearn.tree im##port export_graphviz, plot_tree
##from   sklearn.metrics im##port accuracy_score, mean_absolute_error, mean_squared_error, r2_score

import matplotlib.pyplot as plt #可視化
from   matplotlib.colors import ListedColormap, LinearSegmentedColormap
import matplotlib.font_manager as fm
##from   matplotlib im##port rcParams
##im##port matplotlib.dates as mdates, DateFormatter
##im##port matplotlib.patches as patches
##from   mpl_toolkits.mplot3d im##port Axes3D
if (__file__[:9] not in ["/storage/"]): plt.rcParams['font.family'] = 'Meiryo' # 'sans-serif' # 
else: import japanize_matplotlib
##fm._rebuild()
##print('font フォント:',fm.findSystemFonts())##
##plt.rcParams['font.sans-serif'] = ['Noto Sans CJK JP', 'Yu Gothic', 'Meiryo', 'Hiragino Maru Gothic Pro', 'Meirio', 'Takao', 'IPAexGothic', 'IPAPGothic', 'VL PGothic']
##print([(param, value) for param, value in plt.rcParams.items() if 'font.family' in param])##
import seaborn as sns
##sns.set_style("whitegrid",{'grid.linestyle':':'}) # darkgrid whitegrid dark white ticks
##sns.set_palette('muted', n_colors=None, desat=None, color_codes=False) # deep muted pastel bright dark colorblind
########################################


#################### 可視化 ####################
## v09e.20230101
## savefile = visualiseData(DF, graph, LstColumn, catColumn, EXEC_CFG)
##  DF        :DF to plot
##  graph     :STR of graph type ('pair' 'box' 'hist' 'scatter' 'linemm' 'liness' else 'line')
##  LstColumn :LIST of column to plot
##  catColumn :STR of category column
##  EXEC_CFG  :DICT of exec config
##    title   :STR of graph title
##    show    :BOOL (1: show graph)
##    save    :BOOL (1: save graph)
##    outdir  :STR for output directry
##    figsize :LIST of size [x,y,dpi,line,font]
##  savefile  :STR of saved image filename
def visualiseData(DF, graph, LstColumn, catColumn, EXEC_CFG):
    print("_____ VISUALIZE DATA _____")
    print(dt.datetime.now(),'\n')
    
    if (len(DF[LstColumn[0]].dropna())==0): return "ERROR: df empty"
    
    qry_ = EXEC_CFG.get("query", None)
    if (qry_ not in [None,'']):
        DF = DF.query(qry_)
        EXEC_CFG.update({"title":EXEC_CFG["title"]+"(qry)"})
    ##:if
    
    CAT = bool(catColumn not in ['',None])
    CAT_UNQ_LST = list(DF[catColumn].sort_values(ascending=True).unique()) if (CAT) else []
    CAT_UNQ_NUM = len(CAT_UNQ_LST) if (CAT) else 0
    DAT_NUM = len(DF[LstColumn[0]])
    #print(CAT_UNQ_LST, CAT_UNQ_NUM, DAT_NUM)##
    
    TITLE = EXEC_CFG.get("title","Viz")
    SHOW  = EXEC_CFG.get("show",True)
    SAVE  = EXEC_CFG.get("save",True)
    SAVEDIR = EXEC_CFG.get("outdir","output/")
    if not(os.path.exists(SAVEDIR)): os.makedirs(SAVEDIR)
    
    IS_MOBILE = bool(__file__[:9] in ["/storage/"])
    
    if (IS_MOBILE): FIGSIZE = EXEC_CFG.get("figsize", [4.2,6.5,300,0.5,10])
    else:           FIGSIZE = EXEC_CFG.get("figsize", [3000/300,2000/300,300,1.0,10]) #[1280/300,720/300,300,0.5,5])
    
    print(TITLE, graph, '{0} {1}'.format(LstColumn, len(DF[LstColumn[0]])))##
    if (CAT): print('{0} {1} {2}'.format(catColumn, CAT_UNQ_NUM, CAT_UNQ_LST))##
    
    ##プロット設定
    FC = EXEC_CFG.get("FC","k") # "darkslategray"
    EC = EXEC_CFG.get("EC","w") # "steelblue"
    ## k dimgrey darkgrey silver snow ghostwhite w
    ## darkslategray darkcyan lightslategrey 
    ## midnightblue steelblue aliceblue
    
    if (len(FIGSIZE)==0):
        fig = plt.figure(facecolor=FC, edgecolor=EC)
        LW, FS = 1, 10
    else:
        fig = plt.figure(figsize=(FIGSIZE[0], FIGSIZE[1]), dpi=FIGSIZE[2], facecolor=FC, edgecolor=EC)
        LW, FS = FIGSIZE[3], FIGSIZE[4]
    ##:if
    
    if (FC[:4] in ["dark","k"]): MY_CMP = EXEC_CFG.get("MY_CMP",["dodgerblue","orange","lime","violet","aqua","gold","rosybrown",'y','c','m'])
    else: MY_CMP = EXEC_CFG.get("MY_CMP",["royalblue","darkorange","forestgreen","fuchsia","mediumblue","gold","darkviolet",'y','c','m'])
    
    my_listed_cmap = ListedColormap(MY_CMP, name='MY_CMP', N=10)
    my_liner_cmap  = LinearSegmentedColormap.from_list('MY_CMP', MY_CMP, len(MY_CMP))
    #print(my_cmap)##
    
    CMP_1 = my_listed_cmap # plt.get_cmap('tab10')  ## 'tab10' 10 'Pastel2' 'Paired' 'Set1' 'Set2' 'tab20'
    CMP_2 = plt.get_cmap('tab10')  ## 'Accent' 8
    CMP_3 = plt.get_cmap('Accent') ## 'Dark2' 8
    CMP = [CMP_1(i) for i in range(10)]
    if (10 < CAT_UNQ_NUM): CMP.extend([CMP_2(i) for i in range(8)])
    if (18  < CAT_UNQ_NUM): CMP.extend([CMP_3(i) for i in range(8)])
    if (26 < CAT_UNQ_NUM): CMP.extend([CMP_1(i) for i in range(CAT_UNQ_NUM-(26))])
    
    #print([(param, value) for param, value in plt.rcParams.items() if 'color' in param])## 
    plt.rcParams["axes.edgecolor"]    = EC
    plt.rcParams["axes.facecolor"]    = EC
    plt.rcParams["axes.labelcolor"]   = EC
    plt.rcParams["axes.titlecolor"]   = EC
    plt.rcParams["figure.edgecolor"]  = EC
    plt.rcParams["figure.facecolor"]  = EC
    plt.rcParams["savefig.edgecolor"] = EC
    plt.rcParams["savefig.facecolor"] = EC
    plt.rcParams["text.color"]        = EC
    plt.rcParams["xtick.color"]       = EC
    plt.rcParams["ytick.color"]       = EC
    #plt.rcParams["xtick.labelcolor"]  = EC
    #plt.rcParams["ytick.labelcolor"]  = EC
    plt.rc("axes"   , fc=FC, ec=EC, labelcolor=EC,titlecolor=EC)
    plt.rc("figure" , fc=FC, ec=EC)
    plt.rc("savefig", fc=FC, ec=EC)
    
    ##プロット内容
    ax1 = fig.add_subplot(1, 1, 1)
    
    LOGY      = False
    REGEND_ON = True
    grid_on   = True
    xlabel_90 = False
    tight_lo  = not(IS_MOBILE)
    
    ################################
    ## 箱ひげ図
    ################################
    #   Q1-1.5IQR  Q1  median  Q3  Q3+1.5IQR
    #              |-----|-----|
    #  o  |--------|     |     |--------|   o
    #              |-----|-----|
    #  fliers      <------IQR-->       fliers
    if (graph == 'box'):
        TITLE = TITLE+"_Box_"
        REGEND_ON = False
        
        ##カテゴリ指定あり
        if (CAT):
            TITLE = TITLE+str([LstColumn[0]])[:40]+"_"+catColumn[:16]
            
            xlabel_90 = bool(5<CAT_UNQ_NUM)
            tight_lo  = bool(5<CAT_UNQ_NUM)
            
            df_v = DF[[LstColumn[0], catColumn]].sort_values([catColumn], ascending=True).copy()
            df_v[catColumn] = df_v[catColumn].astype('str')
            unq_list_ = list(df_v[catColumn].sort_values(ascending=True).unique())
            
            for u in unq_list_:
                len_ = len(df_v[df_v[catColumn]==u])
                per_ = (len_ / DAT_NUM) *100
                if (xlabel_90): v_ = '"{0:.20}" ({1})'.format(u, len_)
                else:           v_ = '"{0:.20}"\n({1}, {2:.1f}%)'.format(u, len_, per_)
                df_v[catColumn][df_v[catColumn]==u] = v_
            ##:for
            
            sns.boxplot(data=df_v
            , y=LstColumn[0]
            , x=catColumn
            , linewidth=LW*0.5
            , color=CMP[2] ##
            ##, palette=CMP #tab10 カテゴリごとに色分け
            , boxprops={"edgecolor":EC}
            , medianprops={"color":EC}
            , whiskerprops={"color":EC}
            , capprops={"color":EC}
            , flierprops={"mec":EC, "mew":LW*0.5, "mfc":FC}
            , ax=ax1)
            
        ##カテゴリ指定なし
        else:
            TITLE = TITLE[:-1]+"(Scaled)_"+str(LstColumn)[:40]+"_"
            xlabel_90 = bool(5<len(LstColumn))
            
            sc = pp.StandardScaler()
            df_v = pd.DataFrame(
              sc.fit_transform(DF[LstColumn])
            , index=DF.index
            , columns=LstColumn)
            
            sns.boxplot(data=df_v
            , linewidth=LW*0.5
            , color=CMP[0]
            , boxprops={"edgecolor":EC}
            , medianprops={"color":EC}
            , whiskerprops={"color":EC}
            , capprops={"color":EC}
            , flierprops={"mec":EC, "mew":LW*0.5, "mfc":FC}
            , ax=ax1)
        #:if
    ##:elif
    
    ################################
    ## ヒストグラム
    ################################
    elif graph == 'hist':
        TITLE = TITLE+"_Hist_"+str([LstColumn[0]])[:40]
        
        LOGY      = bool(100<DAT_NUM) ##
        REGEND_ON = False
        
        ##bin数、範囲
        sturges = lambda n:math.ceil(math.log2(n*2))
        hist_bins_ = sturges(len(DF[LstColumn[0]]))*5
        ymin_ = float(DF[LstColumn[0]].dropna().min())
        ymax_ = float(DF[LstColumn[0]].dropna().max())
        hist_range_ = (ymin_, ymax_)
        
        ## stats
        srs_ = DF[LstColumn[0]]
        dsc_ = srs_.describe()
        stats_ = [
          ['mean', dsc_.loc['mean'], '']
        , ['std' , dsc_.loc['std'] , '\n']
        , ['min' , dsc_.loc['min'] , '']
        , [' 1%' , np.percentile(srs_, 1), '']
        , [' 5%' , np.percentile(srs_, 5), '']
        , ['10%' , np.percentile(srs_,10), '']
        , ['25%' , dsc_.loc['25%'], '']
        , ['med.', dsc_.loc['50%'], '']
        , ['75%' , dsc_.loc['75%'], '']
        , ['90%' , np.percentile(srs_,90), '']
        , ['95%' , np.percentile(srs_,95), '']
        , ['99%' , np.percentile(srs_,99), '']
        , ['max' , dsc_.loc['max'], '\n']
        , ['6sg_l', dsc_.loc['mean']-dsc_.loc['std']*3, '']
        , ['6sg_u', dsc_.loc['mean']+dsc_.loc['std']*3, '']
        , ['iqr_l', dsc_.loc['50%']-abs(dsc_.loc['75%']-dsc_.loc['25%'])*1.5, '']
        , ['iqr_u', dsc_.loc['50%']+abs(dsc_.loc['75%']-dsc_.loc['25%'])*1.5, '\n']
        , ['skew', srs_.skew(), ''] # 尖度 skewness
        , ['kurt', srs_.kurt(), ''] # 歪度 kurtosis
        ]
        
        ##カテゴリ指定あり -> 色分けしてプロット
        if (CAT):
            TITLE = TITLE+"_{0}_{1}bins".format(catColumn[:16], hist_bins_)
            
            list_ = [[name, len(group), group] for name, group in DF.groupby(catColumn)]
            list_ = sorted(list_, key=lambda x:(x[1]), reverse=True)
            
            num_  = 0
            TXT_, txt_len = '', 0
            for l_ in list_:
                #print(num_, name)##
                #print(group[[catColumn]].astype('str').describe())##
                
                len_ = l_[1]
                per_ = (len_ / DAT_NUM) *100
                
                ##凡例描画
                TXT_ = '\n' * num_
                TXT_ = TXT_+'■ {0} ({1}, {2:.1f}%) '.format(l_[0], len_, per_)
                
                txt_len = max(txt_len, len('■ {0} ({1}, {2:.1f}%) '.format(l_[0], len_, per_)))
                
                plt.text(0.05, 1-(0.001/FS)-(0.0165*FS/10*1.5)
                , TXT_
                , transform=ax1.transAxes
                , horizontalalignment='left'
                , verticalalignment='top'
                , size=FS*0.65, color=CMP[num_]
                , fontweight='bold'##, family='monospace'
                , bbox={'edgecolor':"None",'linewidth':0, 'facecolor':"None"}
                , zorder=150)
                
                ##ヒストグラム描画
                plt.hist(
                  l_[2].loc[:,LstColumn[0]]
                , range=hist_range_
                , bins=hist_bins_
                , ec=EC, fc=CMP[num_], alpha=0.55
                , lw=0.15)
                
                ##密度推定の描画
                data = l_[2].loc[:,LstColumn[0]].reset_index().iloc[:,1]
                data_max = max(data)
                data_min = min(data)
                dmax = max(data.value_counts(bins=hist_bins_, sort=True).reset_index().iloc[:,1])
                
                if (1<len(data)) and (data_max != data_min):
                    kde = gaussian_kde(data, bw_method=0.6)
                    
                    xlim = ax1.get_xlim()
                    X = np.linspace(xlim[0],xlim[1])
                    
                    estm_y = kde(X)
                    estm_y = estm_y*(dmax)/max(estm_y)
                    
                    ax1.plot(X,estm_y,color='w',lw=1.5,alpha=1)
                    ax1.plot(X,estm_y,color=CMP[num_],lw=2.5,alpha=0.65)
                    ax1.plot(X,estm_y,color='w',lw=0.15,alpha=1)
                ##:if
                
                num_ = num_+1
            ##:for
            
            ##凡例の枠
            plt.text(0.05, 1-(0.001/FS)-(0.0165*FS/10*1.5)
            , '\n'*(num_-1)+' '*(txt_len-1)
            , transform=ax1.transAxes
            , horizontalalignment='left'
            , verticalalignment='top'
            , size=FS*0.65, color='None'
            , fontweight='bold'##, family='monospace'
            , bbox={'edgecolor':EC,'linewidth':0.35,'facecolor':FC,'alpha':0.95}
            , zorder=140)
            
            ## stats
            TXT_ = ''
            for l_ in stats_: TXT_ = TXT_ +'{0} {1:>9.2f}'.format(l_[0].ljust(5), l_[1])+'\n'+l_[2]
            
            plt.text(0.965, 0.975
            , TXT_[:-1]
            , transform=ax1.transAxes
            , horizontalalignment='right'
            , verticalalignment='top'
            , size=FS*0.55, color='w'
            , fontweight='normal'##, family='monospace'
            , bbox={'edgecolor':EC,'linewidth':0.35,'facecolor':FC,'alpha':0.4}
            , zorder=150)
            
        ##カテゴリ指定なし
        else:
            TITLE = TITLE+"_{0}bins".format(hist_bins_)
            
            ##凡例描画
            TXT_ = '■ {0} ({1})'.format(
              LstColumn[0][:16]
            , len(DF[LstColumn[0]]))
            plt.text(0.05, 1-(0.001/FS)-(0.0165*FS/10*1.5)
            , TXT_
            , transform=ax1.transAxes
            , horizontalalignment='left'
            , verticalalignment='top'
            , size=FS*0.65, color=CMP[0]
            , fontweight='bold'##, family='monospace'
            , bbox={'edgecolor':EC,'linewidth':0.35,'facecolor':FC,'alpha':0.95}
            , zorder=150)
            
            ## stats
            TXT_ = ''
            for l_ in stats_: TXT_ = TXT_ +'{0} {1:>9.2f}'.format(l_[0].ljust(5), l_[1])+'\n'+l_[2]
            
            plt.text(0.965, 0.975
            , TXT_[:-1]
            , transform=ax1.transAxes
            , horizontalalignment='right'
            , verticalalignment='top'
            , size=FS*0.55, color='w'
            , fontweight='normal'##, family='monospace'
            , bbox={'edgecolor':EC,'linewidth':0.35,'facecolor':FC,'alpha':0.4}
            , zorder=150)
            
            ##ヒストグラム描画
            plt.hist(
              DF.loc[:,LstColumn[0]]
            , range=hist_range_
            , bins=hist_bins_
            , ec=EC, fc=CMP[0], alpha=0.95
            , lw=0.25)
            
            ##密度推定の描画
            data = DF.loc[:,LstColumn[0]]
            data_max = max(data)
            data_min = min(data)
            dmax = max(data.value_counts(bins=hist_bins_, sort=True).reset_index().iloc[:,1])
            
            if (1<len(data)) and (data_max != data_min):
                kde = gaussian_kde(data, bw_method=0.6)
                
                xlim = ax1.get_xlim()
                X = np.linspace(xlim[0],xlim[1])
                
                estm_y = kde(X)
                estm_y = estm_y*(dmax/max(estm_y))
                
                ax1.plot(X,estm_y,color='w',lw=1.5,alpha=1)
                ax1.plot(X,estm_y,color=CMP[0],lw=2.5,alpha=0.65)
                ax1.plot(X,estm_y,color='w',lw=0.15,alpha=1)
            ##:if
        ##:if
        
        ## stats vline
        ylim = ax1.get_ylim()
        list_ = [
          ['mean', stats_[0][1] , (0, ylim[1]*1.05), 0.85, 'left' , '-']
        , ['med.', stats_[7][1] , (0, ylim[1]*0.85), 0.85, 'left' , '-']
        , ['25%' , stats_[6][1] , (0, ylim[1]*0.65), 0.6 , 'right', '-']
        , ['75%' , stats_[8][1] , (0, ylim[1]*0.70), 0.6 , 'left' , '-']
        , ['10%' , stats_[5][1] , (0, ylim[1]*0.40), 0.6 , 'right', '-']
        , ['90%' , stats_[9][1] , (0, ylim[1]*0.45), 0.6 , 'left' , '-']
        , [' 5%' , stats_[4][1] , (0, ylim[1]*0.20), 0.6 , 'right', '-']
        , ['95%' , stats_[10][1], (0, ylim[1]*0.25), 0.6 , 'left' , '-']
        , [' 1%' , stats_[3][1] , (0, ylim[1]*0.10), 0.6 , 'right', '-']
        , ['99%' , stats_[11][1], (0, ylim[1]*0.15), 0.6 , 'left' , '-']
        ]
        for l_ in list_:
            plt.vlines(l_[1], l_[2][0], l_[2][1], lw=l_[3], ls=l_[5], color=EC, zorder=150)
            plt.annotate(" "+l_[0], xy=(l_[1],  l_[2][1]), ha=l_[4], size=FS*0.65, color=EC, zorder=160)
        ##:for
        
        ## ylim
        ylim = ax1.get_ylim()
        if (LOGY):
            plt.ylim(10**(-0.1), ylim[1]*10**0.25)
            ax1.set_ylim(10**(-0.1), ylim[1]*10**0.25)
        else:
            plt.ylim(0, ylim[1]*1.1)
            ax1.set_ylim(0, ylim[1]*1.1)
        ##:if
    ##:elif
    
    ################################
    ## 散布図
    ################################
    elif graph == 'scatter':
        TITLE = TITLE+"_Scat_"+str(LstColumn[0:2])[:40]+"_"+catColumn[:16]
        
        x_col = LstColumn[0]
        y_col = LstColumn[1]
        
        LW_ = 0.2 if (DAT_NUM<1000) else 0.15
        MS_ = FS  if (DAT_NUM<1000) else FS*0.35
        AL_ = 1.0 if (DAT_NUM<1000) else 0.75
        
        REGEND_ON = False
        cbar_on   = EXEC_CFG.get("cbar", False)
        
        ##カテゴリ指定あり
        if (CAT):
            ## カテゴリ数多 -> colorbar
            if (cbar_on) or ((20<CAT_UNQ_NUM) and (DF[catColumn].dtype !='object')):
                cmp_ = 'Blues' # 'Blues' 'bwr' 'coolwarm' 'seismic' 'PuOr' 'PRGn' 'cool'
                cbar_pos_ = 'vertical' if FIGSIZE[0]/FIGSIZE[1]>=1 else 'horizontal'
                
                plt.scatter(
                  DF[x_col].values
                , DF[y_col].values
                , c=DF[catColumn].values
                , cmap=cmp_
                , marker="o", s=MS_*7.5, lw=LW_
                , ec=EC, alpha=0.95*AL_)
                
                cbar = plt.colorbar(orientation=cbar_pos_, aspect=40, label=catColumn)
                cbar.ax.tick_params(labelsize=FS*0.6)
                cbar.ax.tick_params(axis='x', colors=EC)
                cbar.ax.xaxis.label.set_fontsize(FS*0.9)
                cbar.ax.xaxis.label.set_color(EC)
                cbar.ax.tick_params(axis='y', colors=EC)
                cbar.ax.yaxis.label.set_fontsize(FS*0.9)
                cbar.ax.yaxis.label.set_color(EC)
                
            ## カテゴリ数少 -> 色分け
            else:
                list_ = [[name, len(group), group] for name, group in DF.groupby(catColumn)] # , group
                list_ = sorted(list_, key=lambda x:(x[1]), reverse=True)
                
                num_ = 0
                for l_ in list_:
                    len_ = l_[1]
                    per_ = (len_ / DAT_NUM) *100
                    v_   = '"{0:.20}" ({1}, {2:.1f}%)'.format(str(l_[0]), len_, per_)
                    
                    ax1.plot(
                      l_[2][x_col], l_[2][y_col]
                    , marker='o', ms=MS_*max((1+(0.5-num_/10)),0.5), mew=LW_*1.5#/1.5
                    , mfc=CMP[num_]
                    , mec=EC, alpha=0.85*AL_
                    , linestyle=''
                    , label=v_
                    , zorder=10)
                    
                    num_ = num_+1
                ##:for
                
                REGEND_ON = True # False # 
            #:if
            
        ##カテゴリ指定なし
        else:
            plt.scatter(
              DF[x_col].values
            , DF[y_col].values
            , marker="o", s=MS_*7.5, lw=LW_
            , c=CMP[0], ec=EC
            , alpha=0.75*AL_)
        #:if
        
        ax1.legend(fontsize=FS*0.6)
        plt.xlabel(LstColumn[0], fontsize=FS)
        plt.ylabel(LstColumn[1], fontsize=FS)
    ##:elif
    
    ################################
    ## ヒートマップ
    ################################
    elif graph in ['heat','heatmm','heatrt']:
        grid_on   = False
        xlabel_90 = bool(5<len(LstColumn))
        tight_lo  = False
        
        if (graph=='heatrt'):
            x_lbl_ = DF.columns.name
            y_lbl_ = DF.index.name
            TITLE = TITLE+"_HeatMrx_"+str([x_lbl_,y_lbl_])
            xlabel_90 = bool(5<len(DF.columns))
        else:
            TITLE = TITLE+"_Heat_"+str(LstColumn)[:40]
        ##:if
        
        ## ColorBar # 'Blues' 'bwr' 'coolwarm' 'seismic' 'PuOr' 'PRGn' 'cool'
        if   (graph=='heatmm'): cmp_ = 'coolwarm'
        elif (graph=='heatrt'): cmp_ = 'Blues'
        else                  : cmp_ = 'Oranges'
        
        if (FIGSIZE[0]/FIGSIZE[1]>=1): cbar_pos_ = 'right'
        else: cbar_pos_ = 'bottom'
        
        if   (graph=='heatmm'): cbar_rng_ = (-1,1)
        elif (graph=='heatrt'): cbar_rng_ = ( 0,1)
        else: cbar_rng_ = (None,None)
        
        ## MAX 16 Col. 27 Rec.
        if (16<len(DF.columns)): LstColumn = LstColumn[:15]+[LstColumn[-1]]
        INDEX_ = list(DF.index.values[:27])
        DF_V_  = DF.loc[INDEX_, LstColumn].copy()
        
        ## label
        data   = DF_V_.astype('float').values
        if (xlabel_90): DF_V_.columns = [s.replace('\n',' ') for s in list(DF_V_.columns)]
        xlabel = [s if (len(str(s))<=16) else str(s)[:16]+'\n'+str(s)[16:32] if (16<len(str(s))<=32) else str(s)[:16]+'\n'+str(s)[16:32]+'\n'+str(s)[32:46]+".." for s in list(DF_V_.columns)]
        ylabel = [s if (len(str(s))<=8)
          else '\n'.join([str(s)[:8], str(s)[8:16]]) if (8<len(str(s))<=16)
          else '\n'.join([str(s)[:8], str(s)[8:16], str(s)[16:24], ".."])
        for s in list(DF_V_.index.values)]
        
        REGEND_ON = False
        
        ##plt.imshow(data, cmap='coolwarm', interpolation='nearest')
        sns.heatmap(data
        , vmin=cbar_rng_[0], vmax=cbar_rng_[1]
        , annot=True, fmt='.2f', annot_kws={"size":FS*0.4}
        , cmap=cmp_, linewidths=LW*0.5
        , cbar_kws = dict(use_gridspec=False,location=cbar_pos_,shrink=0.95,label=''))
        
        cax = fig.axes[-1]
        cax.tick_params(which='major',width=0,labelsize=FS*0.5)
        cax.tick_params(axis='x', colors=EC)
        cax.xaxis.label.set_fontsize(FS*0.9)
        cax.xaxis.label.set_color(EC)
        cax.tick_params(axis='y', colors=EC)
        cax.yaxis.label.set_fontsize(FS*0.9)
        cax.yaxis.label.set_color(EC)
        
        ax1.set_xticklabels(xlabel,minor=False,rotation=90)
        ax1.set_yticklabels(ylabel,minor=False,rotation=0)
        
        if (graph=='heatrt'):
            plt.xlabel(x_lbl_)
            plt.ylabel(y_lbl_)
        ##:if
    ##:elif
    
    ################################
    ## 折れ線グラフ
    ################################
    else:
        COLOR = ListedColormap(MY_CMP[:len(LstColumn)+1], name='MY_CMP', N=len(LstColumn)) # my_liner_cmap # Accent Set1 winter ocean viridis gist_rainbow brg terrain rainbow tab20
        
        if (CAT):
            TITLE_ = TITLE+"_Line_"+str([s[:10] for s in LstColumn[:5]])+"_"+catColumn[:16]
            
            if (catColumn in LstColumn):LstColumn.remove(catColumn)
            DFtmp = DF[LstColumn+[catColumn]].copy()
            
            ax2 = ax1.twinx()
        else:
            TITLE_ = TITLE+"_Line_"+str([s[:10] for s in LstColumn[:5]])
            DFtmp = DF[LstColumn].copy()
        ##:if
        REGEND_ON = True
        
        ##時系列ラベル
        if (DFtmp.index.dtype=='datetime64[ns]'):
            DFtmp.index = DFtmp.index.strftime('%m/%d\n%H:%M\n%S.%f').str[:-3]
        ##:if
        
        N_  = len(DFtmp) * len(LstColumn)
        LW_ = LW*1.5  if N_<=500 else LW
        MS_ = FS*0.25 if N_<=500 else 0
        AL_ = 1       if N_<=500 else 1
        
        #正規化(最大1、最小0)
        if graph == 'linemm':
            TITLE_ = TITLE_.replace("_Line_","_LineMM_")
            
            sc = pp.MinMaxScaler()
            DFtmp1 = pd.DataFrame(
                 sc.fit_transform(DFtmp[LstColumn])
                ,index=DFtmp.index
                ,columns=LstColumn)
            ax1.tick_params(labelleft=False, left=False)
            
            N_ = 0.85
        ##:if
        
        #標準化(平均0、標準偏差1)
        if graph == 'liness':
            TITLE_ = TITLE_.replace("_Line_","_LineSS_")
            
            sc = pp.StandardScaler()
            DFtmp1 = pd.DataFrame(
                 sc.fit_transform(DFtmp[LstColumn])
                ,index=DFtmp.index
                ,columns=LstColumn)
            ax1.tick_params(labelleft=False, left=False)
            
            N_ = 0 #3.5
        ##:if
        
        if graph in ['linemm']:
            ##変数ごとにy軸をずらす
            for i in range(len(LstColumn)): DFtmp1[LstColumn[i]] = DFtmp1[LstColumn[i]] + i*N_
            
            DFtmp = pd.concat([DFtmp1,DFtmp[[catColumn]]], axis=1) if (CAT) else DFtmp1
            #print(DFtmp.dtypes)##
            
            yrange = (DFtmp1[LstColumn[0]].min(), DFtmp1[LstColumn[-1]].max())
            ylimlow = yrange[0] - abs(yrange[0])*0.05 - 0.05
            ylimup  = yrange[1] + abs(yrange[1])*0.05 + 0.15
            if (CAT): ax2.set_ylim([ylimlow, ylimup])
            else:     ax1.set_ylim([ylimlow, ylimup])
        ##:if
        
        if graph in ['liness']:
            
            DFtmp = pd.concat([DFtmp1,DFtmp[[catColumn]]], axis=1) if (CAT) else DFtmp1
            #print(DFtmp.dtypes)##
            
            yrange = (DFtmp[LstColumn].min(axis=1).min(), DFtmp[LstColumn].max(axis=1).max())
            #print(yrange)##
            
            ylimlow = yrange[0] - abs(yrange[0])*0.05 - 0.05
            ylimup  = yrange[1] + abs(yrange[1])*0.05 + 0.15
            if (CAT): ax2.set_ylim([ylimlow, ylimup])
            else:     ax1.set_ylim([ylimlow, ylimup])
        ##:if
        
        #折れ線描画
        if (CAT): ##ターゲット有り
            mark_on  = EXEC_CFG.get("mark_on", False)
            mark_qry = EXEC_CFG.get("mark_query", 'index==index')
            mark_col = EXEC_CFG.get("mark_col", catColumn)
            if (mark_on):## [ｘ]マーク有り
                 DFtmp.query(mark_qry)[mark_col].plot(grid=False
                , lw=0, color=EC, alpha=0.95
                , marker='X', ms=FS, mew=0.5
                , mfc='orangered', mec=EC
                , zorder=3
                , ax=ax2)####
            else:
                DFtmp[catColumn].plot(grid=False
                , lw=LW_*1.75, color='w', alpha=0.95
                , zorder=5
                , ax=ax1)
                DFtmp[catColumn].plot(grid=False
                , lw=LW_*1.75+1, color='orangered', alpha=0.95
                , zorder=3
                , ax=ax1)
            ##:if
        ##:if
        
        AX_ = ax2 if (CAT) else ax1
        
        DFtmp[LstColumn].plot(grid=False
        , legend=True
        , lw=LW_,colormap=COLOR,alpha=AL_
        , marker='s',ms=1.5,mew=0.15,mfc="None"
        , zorder=10
        , ax=AX_)
        
        if (15<len(LstColumn)):
            if (ax1.get_legend() is not None): ax1.get_legend().remove()
            REGEND_ON = False
        ##:if
        
        TITLE = TITLE_
    ##:if
    
    ##プロット設定
    #print(plt.get_fignums(), plt.gcf())##
    #print(fig.get_axes(), plt.gca())##
    
    plt.title(TITLE[:40]+'\n'+TITLE[40:], loc='center', y=1, fontsize=FS)
    
    if (LOGY):
        plt.yscale("log")
        ax1.set_yscale("log")
    ##:if
    
    plt.tick_params(labelsize=FS*0.6)
    
    ax1.tick_params(
     top=False, labeltop=False
    ,left=False, labelleft=True#False
    ,right=False, labelright=False
    ,bottom=False, labelbottom=True)
    
    if (graph[:4]=='line') and (CAT): ax2.tick_params(
     top=False, labeltop=False
    ,left=False, labelleft=False
    ,right=False, labelright=False#True
    ,bottom=False, labelbottom=False)
    
    ax1.xaxis.label.set_color(EC)
    ax1.yaxis.label.set_color(EC)
    ax1.tick_params(axis='x', colors=EC)
    ax1.tick_params(axis='y', colors=EC)
    
    if (grid_on): ax1.grid(which='major', ls='dotted', lw=0.25, color=EC, alpha=0.85)
    
    if (IS_MOBILE):
        plt.legend(fontsize=FS*0.6)
    else:
        l = plt.legend(fontsize=FS*0.6)
        l.set_zorder(10)
    ##:if
    if not(REGEND_ON): plt.legend([],[],frameon=False)
    
    if (xlabel_90): RT_ = 90
    else: RT_ = 0
    plt.setp(ax1.get_xticklabels(), rotation=RT_)
    
    if (tight_lo): fig.tight_layout()
    
    savefile = ''
    if (SAVE):
        savefile = SAVEDIR+TITLE+".png"
        plt.savefig(savefile, facecolor=FC, edgecolor=EC)
        print("SAVE TO", savefile)##
    ##:if
    
    if (SHOW): plt.show()
    plt.close('all')
    gc.collect()
    
    print()
    print(dt.datetime.now())
    print("_______________",'\n')
    return '{}'.format(savefile).replace("\\","/")
########################################

########################################
def image_to_excel(l_img, d_param):
    import openpyxl
    import cv2
    import re
    
    ## ワークブック設定
    wb_name = d_param["title"]+'_img.xlsx'
    ws_name = d_param["title"]
    cel_h , cel_w  = 16     , 12
    cel_h_, cel_w_ = 16*0.75, 12*0.13
    
    if (os.path.exists(d_param["outdir"]+wb_name)):
        wb = openpyxl.load_workbook(d_param["outdir"]+wb_name)
        if (ws_name not in wb.sheetnames): wb.create_sheet(title=ws_name)
    else:
        wb = openpyxl.Workbook()
        wb.worksheets[0].title = ws_name
    ##:if
    ws = wb[ws_name]
    
    for i in range(9999): ws.row_dimensions[i].height = cel_h
    for a1,a2 in [(a1,a2) for a1 in ['','A','B','C','D'] for a2 in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ']: ws.column_dimensions[a1+a2].width = cel_w
    
    ## シートに画像貼り付け
    ## (画像のサイズ)
    ####
    buf = np.fromfile(l_img[0], np.uint8)
    size_img = cv2.imdecode(buf, cv2.IMREAD_UNCHANGED)
    ####size_img = cv2.imread(l_img[0])
    ####
    img_height, img_width = size_img.shape[:2]
    
    ## (セルのサイズ)
    h_ = int(img_height*0.5 / cel_h*0.75) + 1
    w_ = int(img_width *0.5 / cel_w*0.13) + 1
    h_img_row = int(img_height*0.5 / cel_h*0.75 + 1)##
    w_img_row = int(img_width *0.5 / cel_w*0.13 + 1)##
    
    ## (画像を貼り付け)
    def add_img_to_ws(lis_, dic_):
        inter_r, inter_c = 4, 1
        
        for i_ in range(len(lis_)):
            if (dic_["array"]=="v"): row_,col_ = dic_["start"][0]+(h_img_row+inter_r)*i_, dic_["start"][1]
            if (dic_["array"]=="h"): row_,col_ = dic_["start"][0], dic_["start"][1]+(w_img_row+inter_c)*i_
            
            str_1 = re.sub('.+/','', l_img[i_])[:-4]
            str_1 = re.sub(".+\['", "['", str_1)
            str_1 = re.sub("'\].+", "']", str_1)
            str_2 = re.sub('.+/','', l_img[i_])[:-4]
            str_2 = re.sub(".+\['", "['", str_2)
            str_2 = re.sub(".+'\]_", "" , str_2)
            str_2 = "" if (str_2==str_1) else str_2
            str_1 = str_1+".." if (str_1[-1] != "]") else str_1
            if (str_2.rfind("bins")!=-1): str_2 = str_2[:str_2.rfind("_")]
            ws.cell(row=row_,column=col_).value = '{}    {}'.format(str_1, str_2)
            
            img_ = openpyxl.drawing.image.Image(l_img[i_])
            img_.width  = img_.width  *0.5
            img_.height = img_.height *0.5
            img_.anchor = ws.cell(row=row_+1, column=col_).coordinate
            ws.add_image(img_)
        ##:for
        return 3+row_+h_img_row, 1+col_+w_img_row
    ##:def
    
    r_,c_ = add_img_to_ws(l_img, d_param)
    wb.save(d_param["outdir"]+wb_name)
    
    return r_, c_
##:def
########################################

########################################
def gen_viz_report(fea_, tar_, tar_is_, df_, ql_, qn_, cfg_):
    import AnalyzeFunctionsVZ_DT as AFVD
    import AnalyzeFunctionsPP    as AFP
    import AnalyzeFunctionsUS    as AFU  # viz_pca_vec() の AFU.PrimaryComponentAnalysis() で使用（import漏れを修正）
    print("_____ GEN VISUALIZE REPORT _____")
    print(dt.datetime.now(),'\n')
    
    print('df_col ({}): {}'.format(len(df_.columns),list(df_.columns)[:20]),"..")##
    print('df_idx ({}): {}'.format(len(df_.index),list(df_.index)[:20]),"..",'\n')##
    
    cfg_.update(show=False, save=True)##
    print(cfg_,'\n')##
    
    def def_var(tar_, tar_is_, ql_, qn_):
        fea_ql_     = [s for s in ql_ if (s != tar_)]
        fea_ql_num_ = [s for s in fea_ql_ if (df_[s].dtype != 'object')]
        fea_ql_obj_ = [s for s in fea_ql_ if (df_[s].dtype == 'object')]
        fea_qn_     = [s for s in qn_ if (s != tar_)]
        
        print('Target: {}, is "{}"'.format(tar_,tar_is_))##
        print('Feature_QL ({}): {}'.format(len(fea_ql_),fea_ql_))##
        print('    Number ({}): {}'.format(len(fea_ql_num_),fea_ql_num_))##
        print('    Object ({}): {}'.format(len(fea_ql_obj_),fea_ql_obj_))##
        print('Feature_QN ({}): {}'.format(len(fea_qn_),fea_qn_),'\n')##
        
        return fea_ql_,fea_ql_num_,fea_ql_obj_,fea_qn_
    ##:def
    
    ## 箱ひげ図 (分布の概要) [QN, QN*QL]
    def box_plot(tar_, tar_is_, ql_, qn_):
        fea_ql_,_,_,fea_qn_ = def_var(tar_, tar_is_, ql_, qn_)
        
        l_imf = []
        if (tar_is_=='ql') and (1<=len(fea_qn_)):
            for c_ in fea_qn_: l_imf.append(AFVD.visualiseData(df_, 'box', [c_], tar_, cfg_))
            l_imf.append(AFVD.visualiseData(df_, 'box', fea_qn_, '', cfg_))
        ##:if
        
        if (tar_is_=='qn') and (1<=len(fea_ql_)):
            for c_ in fea_ql_: l_imf.append(AFVD.visualiseData(df_, 'box', [tar_], c_, cfg_))
            l_imf.append(AFVD.visualiseData(df_, 'box', [tar_]+fea_qn_, '', cfg_))
        ##:if
       
        print(l_imf,len(l_imf))##
        if (None in l_imf): l_imf.remove(None)
        return l_imf if (1<=len(l_imf)) else None
    ##:def
    
    ## ヒストグラム (分布の詳細) [QN, QN*QL]
    def histgram(tar_, tar_is_, ql_, qn_):
        fea_ql_,_,_,fea_qn_ = def_var(tar_, tar_is_, ql_, qn_)
        l_imf = []
        if (tar_is_=='ql') and (1<=len(fea_qn_)):
            for c_ in fea_qn_: l_imf.append(AFVD.visualiseData(df_, 'hist', [c_], tar_, cfg_))
            l_imf.append(AFVD.visualiseData(df_, 'hist', fea_qn_, '', cfg_))
        ##:if
        
        if (tar_is_=='qn') and (1<=len(fea_ql_)):
            for c_ in fea_ql_: l_imf.append(AFVD.visualiseData(df_, 'hist', [tar_], c_, cfg_))
            l_imf.append(AFVD.visualiseData(df_, 'hist', [tar_]+fea_qn_, '', cfg_))
        ##:if
        
        if (tar_ in ['',None]) and (1<=len(fea_qn_)):
            for c_ in fea_qn_: l_imf.append(AFVD.visualiseData(df_, 'hist', [c_], '', cfg_))
            l_imf.append(AFVD.visualiseData(df_, 'hist', fea_qn_, '', cfg_))
        ##:if
        
        print(l_imf,len(l_imf))##
        if (None in l_imf): l_imf.remove(None)
        return l_imf if (1<=len(l_imf)) else None
    ##:def
    
    ## 散布図 (変数同士の関係性) [QN, QN*QL]
    def scatter_tar_plot(tar_, tar_is_, ql_, qn_):
        fea_ql_,_,_,fea_qn_ = def_var(tar_, tar_is_, ql_, qn_)
        
        l_imf = []
        if (tar_is_=='ql') and (2<=len(fea_qn_)):
            for c1, c2 in itr.combinations(fea_qn_, 2): l_imf.append(AFVD.visualiseData(df_, 'scatter', [c1, c2], tar_, cfg_))
        ##:if
        
        if (tar_is_=='qn') and (1<=len(fea_qn_)):
            cat_ = fea_ql_[0] if (0<len(fea_ql_)) else tar_
            for c_ in fea_qn_: l_imf.append(AFVD.visualiseData(df_, 'scatter', [c_, tar_], cat_, cfg_))
        ##:if
        
        print(l_imf,len(l_imf))##
        if (None in l_imf): l_imf.remove(None)
        return l_imf if (1<=len(l_imf)) else None
    ##:def
    
    def scatter_fea_plot(tar_, tar_is_, ql_, qn_):
        fea_ql_,_,_,fea_qn_ = def_var(tar_, tar_is_, ql_, qn_)
        
        l_imf = []
        if (2<=len(fea_qn_)):
            cat_ = fea_ql_[0] if (0<len(fea_ql_)) else tar_
            for c1, c2 in itr.combinations(fea_qn_, 2):
                if (tar_is_ != ''): l_imf.append(AFVD.visualiseData(df_, 'scatter', [c1, c2], cat_, cfg_))####
                else              : l_imf.append(AFVD.visualiseData(df_, 'scatter', [c1, c2], ''  , cfg_))
            ##:for
        ##:if
        
        print(l_imf,len(l_imf))##
        if (None in l_imf): l_imf.remove(None)
        return l_imf if (1<=len(l_imf)) else None
    ##:def
    
    ## 折れ線 (時系列／値大小順での推移) # line(mm/ss)
    def line_chart(gtype, N, tar_, tar_is_, ql_, qn_):
        fea_ql_, fea_ql_num_, fea_ql_obj_, fea_qn_ = def_var(tar_, tar_is_, ql_, qn_)

        l_imf = []
        df_v_ = df_.copy()
        print(df_v_.dtypes)##

        obj_col = [tar_]+fea_ql_obj_ if (tar_is_=='ql') else fea_ql_obj_
        if (0<len(obj_col)): df_v_,_ = AFP.encodeData(df_v_, obj_col)

        cols = [tar_]+fea_ql_num_ + fea_ql_obj_ + fea_qn_
        for i_ in range(0, len(cols), N):
            if (len(cols[i_:i_+N])<=1): continue
            
            ## 目的変数でソート
            if (tar_is_ != ''):
                posfx_ = "(sort)"
                df_v_ = df_v_.sort_values([tar_], ascending=True).reset_index(drop=True)##
                l_imf.append(AFVD.visualiseData(df_v_, gtype, cols[i_:i_+N], tar_, {**cfg_,"title":cfg_["title"]+posfx_}))
            else:
                l_imf.append(AFVD.visualiseData(df_v_, gtype, cols[i_:i_+N], tar_, cfg_))####
            ##:if
            
#            ## 平準化
#            roll = int(len(df_v_)//50)
#            for mc in fea_qn: df_v_[mc] = df_v_[[mc]].rolling(roll, min_periods=1).mean()##
#            posfx_ = posfx_+"("+"roll"+str(roll)+")"
        ##:for
        
        print(l_imf,len(l_imf))##
        if (None in l_imf): l_imf.remove(None)
        return l_imf if (1<=len(l_imf)) else None
    ##:def
    
    ## ヒートマップ(相関係数) [QN]
    def heatmap_corr(tar_, tar_is_, ql_, qn_):
        fea_ql_,_,_,fea_qn_ = def_var(tar_, tar_is_, ql_, qn_)
        
        l_imf = []
        
        if (tar_is_=='ql'):
            ql_col = [tar_]+fea_ql_
            qn_col = fea_qn_
        elif (tar_is_=='qn'):
            ql_col = fea_ql_
            qn_col = [tar_]+fea_qn_
        else:
            ql_col = fea_ql_
            qn_col = fea_qn_
        ##:if
        
        _,_,l_df_cr = AFP.CALC_STATS(df_[ql_col+qn_col], ql_col, qn_col, cfg_)
        
        if (0<len(l_df_cr[0][0])):
            print(len(l_df_cr[0][0]))##
            l_df_cr[0][0] = l_df_cr[0][0].reindex(index=qn_col, columns=qn_col)
            l_imf.append(AFVD.visualiseData(l_df_cr[0][0], 'heatmm', qn_col[:16], '', {**cfg_,"title":cfg_["title"]+"_Corr"}))
        ##:if
        
        if (0<len(l_df_cr[1])):
            print(len(l_df_cr[1]))##
            l_df_cr[1] = l_df_cr[1].reindex(index=ql_col, columns=ql_col)
            l_imf.append(AFVD.visualiseData(l_df_cr[1], 'heatmm', ql_col[:16], '', {**cfg_,"title":cfg_["title"]+"_Corr(crmr)"}))
        ##:if
        
        if (0<len(l_df_cr[2])):
            print(len(l_df_cr[2]))##
            l_df_cr[2] = l_df_cr[2].reindex(index=qn_col, columns=ql_col)
            l_imf.append(AFVD.visualiseData(l_df_cr[2], 'heatmm', ql_col[:16], '', {**cfg_,"title":cfg_["title"]+"_Corr(crrt)"}))
        ##:if
        
        print(l_imf,len(l_imf))##
        if (None in l_imf): l_imf.remove(None)
        return l_imf if (1<=len(l_imf)) else None
    ##:def
    
    ## ヒートマップ(カテゴリ・マトリックス) [QL]
    def heatmap_cat(tar_, tar_is_, ql_, qn_):
        fea_ql_,_,_,_ = def_var(tar_, tar_is_, ql_, qn_)
        l_imf = []

        if (2<=len(fea_ql_+[tar_])): # (tar_is_=='ql') and (1<=len(fea_ql_)):
            for tar_, c_ in itr.combinations(fea_ql_+[tar_], 2): # for c_ in fea_ql_:
                print(tar_, c_)####
                
                df_xtb = pd.crosstab(df_[tar_].copy(), df_[c_].copy(), margins=True)
                df_xtb_norm = pd.crosstab(df_[tar_].copy(), df_[c_].copy(), margins=True, normalize='index')
                print(df_xtb)##
                print(df_xtb_norm)##
                
                n_all_ = df_xtb.loc['All','All']
                df_xtb_norm.columns = ['{:<16}({}, {:.2f})'.format('"'+str(s)[:14]+'"', df_xtb.loc['All',s], df_xtb.loc['All',s]/n_all_) for s in list(df_xtb_norm.columns)]
                df_xtb_norm.index   = ['{:<8}({:<6},   {:.2f}) '.format('"'+str(s)[:6]+'"', df_xtb.loc[s,'All'], df_xtb.loc[s,'All']/n_all_) for s in list(df_xtb_norm.index)]
                df_xtb_norm.columns.name = c_
                df_xtb_norm.index.name = tar_
                #print(df_xtb_norm)##
                
                l_imf.append(AFVD.visualiseData(df_xtb_norm, 'heatrt', list(df_xtb_norm.columns), '', cfg_))
            ##:for
        ##:if
        
        print(l_imf,len(l_imf))##
        if (None in l_imf): l_imf.remove(None)
        return l_imf if (1<=len(l_imf)) else None
    ##:def
    
    ## ヒートマップ(PCA固有ベクトル)
    def viz_pca_vec(tar_, tar_is_, ql_, qn_):
        print('DIMENSIONALITY REDUCTION')
        title_ = cfg_["title"]+"(pca)"
        l_imf = []

        _,_,_,fea_qn_ = def_var(tar_, tar_is_, ql_, qn_)
        
        if (2<=len(fea_qn_)):
            _,_,df_vec = AFU.PrimaryComponentAnalysis(df_[fea_qn_[:16]], 8)
            ##df_vec.T.to_excel(OUTDIR+title_+"_pca_vec.xlsx", index=True, header=True)
            
            df_vec_ = df_vec.T
            cols_ = list(df_vec_.columns)
            for c in cols_: df_vec_[c] = df_vec_[c].abs()
            df_vec_['p_stat'] = df_vec_[cols_].max(axis=1)
            for c in cols_: df_vec_[c+"_r"] = df_vec_[c].round(1)
            
            l_imf.append(AFVD.visualiseData(df_vec_.query('0.1<p_stat').sort_values([c+"_r" for c in cols_[:16]], ascending=False), 'heat', cols_, '', {**cfg_, "title":cfg_["title"]+"_vec", "FC":"darkslategray", "EC":"snow"}))
        ##:if
        
        print(l_imf,len(l_imf))##
        if (None in l_imf): l_imf.remove(None)
        return l_imf if (1<=len(l_imf)) else None
    ##:def
    
    ##変数定義
    tar_, tar_is_ = tar_, tar_is_ # 'ql' 'qn' '' ##
    ql_  = ql_[:]##
    qn_  = qn_[:]##

    ## 1つのグラフ生成が失敗しても、残りのグラフ生成・Excel出力を止めないための保護ラッパー
    ## Mod: 2026.08.09    By: IWATA,Y.
    ## Mod: 2026.08.09    By: IWATA,Y.  tar_ が None（目的変数未設定）の場合、エラーメッセージだけでは
    ##      原因が分かりにくい（例: "TypeError: None"）ため、その旨のヒントを付け加えるようにした
    def _safe(label_, func_, *args_):
        try:
            return func_(*args_)
        except Exception as e_:
            hint_ = "（目的変数(objective)が未設定なため、この処理は実行できなかった可能性があります）" if (tar_ is None) else ""
            print(f"WARNING: '{label_}' の生成中にエラーが発生したためスキップします: {e_}{hint_}")
            traceback.print_exc()
            return None
    ##:def

    ##プロット
    l_l_img = []

    l_l_img.append(_safe('line_chart', line_chart, 'linemm', 5, tar_, tar_is_, ql_, qn_))
    
    l_l_img.append(_safe('box_plot', box_plot, tar_, tar_is_, ql_, qn_))
    
    l_l_img.append(_safe('histgram(all)', histgram, '', '', '', qn_))
    l_l_img.append(_safe('histgram(tar)', histgram, tar_, tar_is_, ql_, qn_))
    for cat in ql_: l_l_img.append(_safe(f'histgram({cat})', histgram, cat, 'ql', ql_, qn_))####
    
    l_l_img.append(_safe('heatmap_cat', heatmap_cat, tar_, tar_is_, ql_, qn_))#ql_[3], 'ql', ql_[:3], qn_))

    l_l_img.append(_safe('scatter_tar_plot(all)', scatter_tar_plot, tar_, tar_is_, ql_, qn_[:]))##
    for cat in ql_: l_l_img.append(_safe(f'scatter_tar_plot({cat})', scatter_tar_plot, tar_, tar_is_, [cat], qn_[:]))

    l_l_img.append(_safe('scatter_fea_plot', scatter_fea_plot, tar_, tar_is_, ql_, qn_[:5]))##

    l_l_img.append(_safe('heatmap_corr', heatmap_corr, tar_, tar_is_, ql_[:10], qn_[:10]))
    
    l_l_img.append(_safe('viz_pca_vec', viz_pca_vec, tar_, tar_is_, ql_, qn_[:10]))
    
    ## image list
    l_l_img = [l for l in l_l_img if (l is not None)]
    print(len(l_l_img), [len(l) for l in l_l_img])##
    print(l_l_img)##
    
    ## Excel Output
    imgxls = cfg_["outdir"]+cfg_["title"]+"_img.xlsx"
    if (os.path.exists(imgxls)): os.remove(imgxls)
    
    row, cel = 3, 2
    for i_ in range(len(l_l_img)):
        if (l_l_img[i_]) is None: continue
        if (None in l_l_img[i_]): l_l_img[i_].remove(None)
        print(i_,l_l_img[i_])##
        try:
            _,cel = image_to_excel(l_l_img[i_],   {"array":"v", "start":[row, cel],  "title":cfg_["title"], "outdir":cfg_["outdir"]})
        except Exception as e_:
            # このグループのExcel埋め込みだけ失敗（壊れた画像ファイル等）でも、
            # 既に書き込み済みの他グループはそのまま残し、後続グループの処理も続行する
            print(f"WARNING: Excelへの画像埋め込み中にエラーが発生したためスキップします（グループ{i_}）: {e_}")
            traceback.print_exc()
    ##:for
    
    print()
    print(dt.datetime.now())
    print("_______________",'\n')
    return l_l_img
##:def
########################################

##savefile = visualiseData(DF, graph, LstColumn, catColumn, EXEC_CFG)

########################################
#v06.20220130
if __name__ == "__main__":
    import AnalyzeFunctionsDT    as AFD
    import AnalyzeFunctionsPP    as AFP
    import AnalyzeFunctionsUS    as AFU
    import AnalyzeFunctionsML    as AFM
    import AnalyzeFunctionsVZ_DT as AFVD
    
    if (__file__[:9] in ["/storage/"]): figsize = [4.2,6.5,300,0.5,10]
    else: figsize = [3000/300,2000/300,300,1.0,10]
    
    FILENAME = os.path.basename(__file__)
    DIRNAME  = os.path.dirname(__file__)
    INDIR    = DIRNAME+"/input/"
    OUTDIR   = DIRNAME+"/output/"
    for d in [INDIR,OUTDIR]: os.makedirs(d) if not(os.path.exists(d)) else None
##:if

## データ準備
if __name__ == "__main__":
    print(dt.datetime.now(),"[START] PREPARATION, PREPROSESSING",'\n')
    
    title_ = "MPG"  # Iris Wine MPG Ames AWB ATTRI STL TTN GLSS FrFr BNT Sensor Tb Tg Sin DataXn ##
    featurename,targetname,df,IsQualittv,IsQuanttv,Judge = AFD.PrepareData(title_)
    
    target_is = \
         'ql' if (targetname[0] in IsQualittv)\
    else 'qn' if (targetname[0] in IsQuanttv)\
    else ''
    
    OUTDIR = DIRNAME+"/DIO/"+title_+"/output/"
    if not(os.path.exists(OUTDIR)): os.makedirs(OUTDIR)
    
    print("df:",len(df),",",len(df.columns))
    print("target :",targetname,target_is)
    print("feature:",len(featurename),featurename)
    print("IsQualittv:",len(IsQualittv),IsQualittv)
    print("IsQuanttv :",len(IsQuanttv),IsQuanttv)
    
    print()
    print(dt.datetime.now(),"[END] PREPARATION, PREPROSESSING",'\n')
##:if

## 可視化レポート
if (__name__ == "__main__"):
    print(dt.datetime.now(),"[START] VISUALIZE",'\n')
    
    cfg = dict(figsize=figsize
    , title=title_
    , outdir=OUTDIR
    , FC="darkslategray", EC="snow")##
    ## k dimgrey darkgrey silver snow ghostwhite w
    ## darkslategray darkcyan lightslategrey 
    ## midnightblue steelblue aliceblue
    
    if (title_ == "MPG"):
        for c in ['carname', 'manu_name']: IsQualittv.remove(c)
    ##:if
    
    if (True): gen_viz_report(featurename, targetname[0], target_is, df.dropna(subset=IsQuanttv), IsQualittv, IsQuanttv, {**cfg, "title": title_+""})
    
    if (False):
        print('DIMENSIONALITY REDUCTION')
        title_ = title_ + "(pca)"
        fea_ql, fea_qn = copy.deepcopy(IsQualittv), copy.deepcopy(IsQuanttv)
        if (targetname[0] in fea_ql): fea_ql.remove(targetname[0])
        if (targetname[0] in fea_qn): fea_qn.remove(targetname[0])

        _, df_pca, df_vec = AFU.PrimaryComponentAnalysis(df[fea_qn], 10)
        df = pd.concat([df[fea_ql], df_pca, df[targetname]], axis=1)

        if (targetname[0] in IsQuanttv):
            IsQuanttv = copy.deepcopy(list(df_pca.columns) + targetname)
            featurename = fea_ql + IsQuanttv[:-1]
        else:
            IsQuanttv = copy.deepcopy(list(df_pca.columns))
            featurename = fea_ql + IsQuanttv
        ##:if
        
        gen_viz_report(featurename, targetname[0], target_is, df, IsQualittv, IsQuanttv, {**cfg, "title":title_})
    ##:if
    
    print()
    print(dt.datetime.now(),"[END] VISUALIZE",'\n')
##:if
########################################
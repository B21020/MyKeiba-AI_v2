import matplotlib.pyplot as plt

def plot_single_threshold(df, N_SAMPLES, label=' '):
    plt.figure(dpi=100)
    # 標準偏差で幅をつけて薄くプロット
    plt.fill_between(
        df.index,
        y1=df['return_rate']-df['std'],
        y2=df['return_rate']+df['std'],
        alpha=0.3
        )
    # 回収率を実線でプロット
    plt.plot(df.index, df['return_rate'], label=label)
    # labelで設定した凡例を表示させる
    plt.legend()
    # グリッドをつける
    plt.grid(True)
    plt.xlabel('threshold')
    plt.ylabel('return_rate')
    plt.show()

def plot_single_threshold_compare(old_returns_df, returns_df, N_SAMPLES, label1='old_tansho', label2='new_tansho'):
    plt.figure(dpi=100)
    # old_returns_dfの標準偏差で幅をつけて薄くプロット
    plt.fill_between(
        old_returns_df.index,
        y1=old_returns_df['return_rate']-old_returns_df['std'],
        y2=old_returns_df['return_rate']+old_returns_df['std'],
        alpha=0.3
        )
    # old_returns_dfの回収率を実線でプロット
    plt.plot(old_returns_df.index, old_returns_df['return_rate'], label=label1)

    # returns_dfの標準偏差で幅をつけて薄くプロット
    plt.fill_between(
        returns_df.index,
        y1=returns_df['return_rate']-returns_df['std'],
        y2=returns_df['return_rate']+returns_df['std'],
        alpha=0.3
        )
    # returns_dfの回収率を実線でプロット
    plt.plot(returns_df.index, returns_df['return_rate'], label=label2)

    # labelで設定した凡例を表示させる
    plt.legend()
    # グリッドをつける
    plt.grid(True)
    plt.xlabel('threshold')
    plt.ylabel('return_rate')
    plt.show()
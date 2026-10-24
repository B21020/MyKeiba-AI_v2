"""オリジナル・2月・現在のモデルを、同じものさし（発走6分前・確定の市場に対する ΔR²）で比べる。

- fold・評価対象・市場は tools/run_stage2_t6.py の --rolling と同じ（評価は 2026年1〜6月の3期間）
- 古いパイプラインのスコアは tools/pipeline_compare/build_and_train.py の出力（scores_orig.csv・scores_feb.csv）
- 現在のモデルは base（過去着順＋通過順＋条件別成績＋騎手成績）と noid（ID系を除く全特徴量）を同じ fold で学習
- すべてのパイプラインのスコアが全頭そろうレースだけを使う（共通の評価対象）
- 各スコアは log を取り、calib で条件付きロジットを推定して評価期間で比べる
  単独：[log s]／6分前の市場との結合：[log s, log q6]／確定の市場との結合：[log s, log q]

判定（計画どおり）:
  どれも上積みが無い（合計 ΔR²（結合−市場、6分前）の区間が0をまたぎ、差が ±0.0005 以内）→ ジリ貧ではない
  古い方が明確に良い（現在より 0.0005 以上高く、区間の下限 > 0）→ 一つずつ戻す切り分けへ

使い方:
  .venv/Scripts/python.exe tools/pipeline_compare/compare.py --scores models/20261024/pipeline_compare --out models/20261024/pipeline_compare
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from modules.evaluation import FINAL_PERIOD_START, ConditionalLogit, paired_delta_r2, pseudo_r2, top1_accuracy  # noqa: E402
from modules.training import align_to_schema  # noqa: E402
from tools.analyze_stage2_ability import define_groups, fit_model, sub_schema  # noqa: E402
from tools.run_stage2_market_baseline import load_inputs  # noqa: E402
from tools.run_stage2_t6 import rolling_splits, seg_mask, t6_odds  # noqa: E402

OLD = ['orig', 'feb']
THRESHOLD = 0.0005


def cl_probs(cal, out, cols):
    cl = ConditionalLogit().fit(cal[cols], cal['race_id'], cal['win'])
    return cl.predict_proba(out[cols], out['race_id']), dict(zip(cols, map(float, cl.coef_)))


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--scores', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    sdir, out_dir = ROOT / a.scores, ROOT / a.out
    out_dir.mkdir(parents=True, exist_ok=True)
    args = SimpleNamespace(featured='data/tmp/featured_data_20261024_stage1.pickle',
                           schema='reproducibility/20261024_stage1/feature_schema.json',
                           race_inclusion='reproducibility/20261024_stage0_audit/race_inclusion.csv',
                           return_tables='data/raw/return_tables.pickle', allow_final=False, drop_feature_prefix=[])
    fd, schema = load_inputs(args)
    assert fd['date'].max() < FINAL_PERIOD_START
    dev_ids = set(fd.loc[(fd['date'] >= pd.Timestamp('2025-11-01')) & fd['is_eval'], 'race_id'])
    o6 = t6_odds(dev_ids)
    d = fd.merge(o6, on=['race_id', '馬番'], how='left')
    ok = d[d['race_id'].isin(dev_ids)].groupby('race_id')['odds_t6'].apply(lambda s: s.notna().all() and (s > 0).all())
    usable = set(ok[ok].index)
    inv = 1.0 / d['odds_t6']
    d['log_q6'] = np.log(inv / inv.groupby(d['race_id']).transform('sum'))

    # 古いパイプラインのスコア。4か月の予測区間が重なるので、同じ馬が2つの fold に出る → fold ごとにその cutoff の分だけ結合する
    old_scores = {p: pd.read_csv(sdir / f'scores_{p}.csv', dtype={'race_id': str}) for p in OLD}
    for p, s in old_scores.items():
        assert not s.duplicated(['race_id', '馬番', 'cutoff']).any()

    groups = define_groups(schema['columns'])
    ids = set(groups['id_horse'] + groups['id_connections'] + groups['peds'])
    current = {'cur_base': groups['form'] + groups['position'] + groups['cond_history'] + groups['jockey_stats'],
               'cur_noid': [c for c in schema['columns'] if c not in ids]}
    pipes = OLD + list(current)
    summary = {'pipelines': pipes, 'threshold': THRESHOLD, 'folds': {}}
    pooled = []
    for sp in rolling_splits():
        C = sp['calib'][0]
        seg = {s: d[seg_mask(d['date'], s, sp)] for s in ('fit', 'select')}
        parts = {s: d[seg_mask(d['date'], s, sp) & d['race_id'].isin(usable)].copy() for s in ('calib', 'outer')}
        for name, cols in current.items():
            sch = sub_schema(schema, cols)
            model = fit_model({s: align_to_schema(seg[s], sch) for s in seg}, seg)
            for s in parts:
                parts[s][f'score_{name}'] = model.predict_proba(align_to_schema(parts[s], sch))[:, 1]
        for s in parts:
            df = parts[s]
            for p in OLD:  # この fold の cutoff で学習したスコアだけ
                sc = old_scores[p]
                sc = sc.loc[sc['cutoff'] == C, ['race_id', '馬番', 'score']].rename(columns={'score': f'score_{p}'})
                df = df.merge(sc, on=['race_id', '馬番'], how='left', validate='one_to_one')
            complete = df.groupby('race_id')[[f'score_{p}' for p in pipes]].apply(lambda x: x.notna().all().all())
            parts[s] = df[df['race_id'].isin(set(complete[complete].index))].copy()
            for p in pipes:
                parts[s][f'log_{p}'] = np.log(np.clip(parts[s][f'score_{p}'], 1e-9, 1))
        cal, out = parts['calib'], parts['outer']
        r, w = out['race_id'], out['win']
        res = {'calib_races': int(cal['race_id'].nunique()), 'outer_races': int(r.nunique()),
               'dropped_outer_races': int(len(set(d.loc[seg_mask(d['date'], 'outer', sp) & d['race_id'].isin(usable), 'race_id'])) - r.nunique()),
               'pipelines': {}}
        p_m6, _ = cl_probs(cal, out, ['log_q6'])
        p_mf, _ = cl_probs(cal, out, ['log_q'])
        store = {'race_id': r.to_numpy(), 'win': w.to_numpy(), 'mkt6': p_m6, 'mktf': p_mf}
        for p in pipes:
            alone, _ = cl_probs(cal, out, [f'log_{p}'])
            c6, coef6 = cl_probs(cal, out, [f'log_{p}', 'log_q6'])
            cf, _ = cl_probs(cal, out, [f'log_{p}', 'log_q'])
            store.update({f'{p}_alone': alone, f'{p}_c6': c6, f'{p}_cf': cf})
            res['pipelines'][p] = {
                'alone_pseudo_r2': pseudo_r2(alone, r, w), 'auc_win': float(roc_auc_score(w, out[f'score_{p}'])),
                'auc_top3': float(roc_auc_score(out['rank'], out[f'score_{p}'])), 'top1': top1_accuracy(alone, r, w),
                'coef_with_t6': coef6,
                'dr2_t6': paired_delta_r2(c6, p_m6, r, w), 'dr2_final': paired_delta_r2(cf, p_mf, r, w)}
        summary['folds'][f'outer_{sp["outer"][0]}'] = res
        pooled.append(pd.DataFrame(store))
        print(f"outer {sp['outer'][0]} ({res['outer_races']} races): " + ' | '.join(
            f"{p} dR2_t6={v['dr2_t6']['delta_r2']:+.5f} alone={v['alone_pseudo_r2']:.4f}" for p, v in res['pipelines'].items()), flush=True)

    P = pd.concat(pooled)
    pool = {}
    for p in pipes:
        pool[p] = {'dr2_t6': paired_delta_r2(P[f'{p}_c6'], P['mkt6'], P['race_id'], P['win']),
                   'dr2_final': paired_delta_r2(P[f'{p}_cf'], P['mktf'], P['race_id'], P['win']),
                   'alone_pseudo_r2': pseudo_r2(P[f'{p}_alone'], P['race_id'], P['win'])}
    pairwise = {f'{a}_minus_{b}': paired_delta_r2(P[f'{a}_c6'], P[f'{b}_c6'], P['race_id'], P['win'])
                for a in OLD for b in current}
    summary['pooled'] = pool
    summary['pairwise_with_t6_market'] = pairwise
    best_current = max(pool[c]['dr2_t6']['delta_r2'] for c in current)
    old_wins = [p for p in OLD if pool[p]['dr2_t6']['delta_r2'] - best_current >= THRESHOLD and pool[p]['dr2_t6']['ci95'][0] > 0]
    any_info = [p for p in pipes if pool[p]['dr2_t6']['ci95'][0] > 0]
    summary['decision'] = {'old_clearly_better': old_wins, 'pipelines_with_ci_above_zero_vs_t6': any_info,
                           'next': 'ablation (stage C)' if old_wins else 'no decline found; stop pipeline tuning'}
    (out_dir / 'results.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=float), encoding='utf-8')
    for p in pipes:
        v = pool[p]
        print(f"pooled {p}: dR2_t6={v['dr2_t6']['delta_r2']:+.5f} {[round(x, 5) for x in v['dr2_t6']['ci95']]} "
              f"dR2_final={v['dr2_final']['delta_r2']:+.5f} alone={v['alone_pseudo_r2']:.4f}")
    for k, v in pairwise.items():
        print(f"{k}: {v['delta_r2']:+.5f} {[round(x, 5) for x in v['ci95']]}")
    print('decision:', summary['decision'])


if __name__ == '__main__':
    main()

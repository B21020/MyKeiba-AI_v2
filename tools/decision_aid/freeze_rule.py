"""意思決定の補助で使う補正と、複勝の紙上の検証の規則を、開発期間のデータだけで固定する。

出力（--out の JSON）:
  win_calibration    勝つ確率 = レース内 softmax(beta · log q6)。[log q6] の条件付きロジットの係数
  place_calibration  複勝の確率 = sigmoid(intercept + slope · logit(Harville の複勝の確率))
  paper_rule         補正した複勝の確率 × 6分前の複勝オッズの下限 ≥ threshold の馬に複勝100円。
                     発走 skip_after_min 分前までに6分前の配信を受け取れなければ見送り
  reference          開発期間の人気別「全部買ったときの回収率」（単勝・複勝は6分前の人気、馬連は6分前の馬連の人気）
  backtest           同じ規則の開発期間の結果（tools/run_place_market.py の H・1.00）
  rule_hash          上の規則と補正の SHA-256（紙上の記録に付ける）
最終確認期間（2026-07-01 以降）は読まない。

使い方:
  .venv/Scripts/python.exe tools/decision_aid/freeze_rule.py --out reproducibility/20261101_decision_aid/frozen_rule.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from modules.evaluation import FINAL_PERIOD_START, ConditionalLogit, pseudo_r2  # noqa: E402
from tools.run_place_market import build  # noqa: E402
from tools.run_stage2_market_baseline import load_inputs  # noqa: E402
from tools.run_stage2_t6 import DECISION_MIN, t6_odds  # noqa: E402

DEV_START, DEV_END = '2025-11-01', '2026-07-01'
THRESHOLD = 1.00
SKIP_AFTER_MIN = 4
BANDS = [0, 1, 2, 3, 5, 8, 12, 18]
LABELS = ['1', '2', '3', '4-5', '6-8', '9-12', '13-18']


def rule_hash(rule: dict) -> str:
    keys = ('win_calibration', 'place_calibration', 'paper_rule')
    return hashlib.sha256(json.dumps({k: rule[k] for k in keys}, sort_keys=True).encode()).hexdigest()


def logit(p):
    p = np.clip(np.asarray(p, float), 1e-6, 1 - 1e-6)
    return np.log(p / (1 - p))


def reference_tables(d: pd.DataFrame) -> dict:
    d = d.copy()
    d['band'] = pd.cut(d.groupby('race_id')['odds_t6'].rank(method='first'), BANDS, labels=LABELS)
    g = d.groupby('band', observed=True)
    tansho = g['tansho_payout'].sum() / (100 * g.size())
    fukusho = g['fukusho_payout'].sum() / (100 * g.size())
    q = json.loads((ROOT / 'models/20261025/quinella_market/results.json').read_text(encoding='utf-8'))
    return {'tansho_by_t6_popularity': {k: round(float(v), 4) for k, v in tansho.items()},
            'fukusho_by_t6_popularity': {k: round(float(v), 4) for k, v in fukusho.items()},
            'umaren_by_quinella_popularity': {r['band']: r['all_bet_roi'] for r in q['descriptive_all_dev']['by_quinella_popularity']},
            'payback_rate': {'単勝': 0.80, '複勝': 0.80, '枠連': 0.775, '馬連': 0.775, 'ワイド': 0.775, '馬単': 0.75,
                             '3連複': 0.75, '3連単': 0.725},
            'note': '開発期間 2025-11〜2026-06、評価対象のレースの全馬（全組）を100円ずつ買った場合の回収率。払戻は確定'}


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    args = SimpleNamespace(featured='data/tmp/featured_data_20261024_stage1.pickle',
                           schema='reproducibility/20261024_stage1/feature_schema.json',
                           race_inclusion='reproducibility/20261024_stage0_audit/race_inclusion.csv',
                           return_tables='data/raw/return_tables.pickle', allow_final=False, drop_feature_prefix=[])
    fd, _ = load_inputs(args)
    fd = fd[['race_id', '馬番', 'date', 'win', 'is_eval', '単勝', 'tansho_payout']]
    assert fd['date'].max() < FINAL_PERIOD_START
    dev = (fd['date'] >= pd.Timestamp(DEV_START)) & (fd['date'] < pd.Timestamp(DEV_END)) & fd['is_eval']
    dev_ids = set(fd.loc[dev, 'race_id'])
    rt = pd.read_pickle(ROOT / args.return_tables)
    rt = rt[rt.index.astype(str).isin(dev_ids)]
    d, info = build(fd[fd['race_id'].isin(dev_ids)], t6_odds(dev_ids), rt)
    assert d['date'].max() < FINAL_PERIOD_START

    cl = ConditionalLogit().fit(d[['log_q6']], d['race_id'], d['win'])
    beta = float(cl.coef_[0])
    d['logit_h'] = logit(d['h'])
    lr = LogisticRegression(C=1e6, max_iter=1000).fit(d[['logit_h']], d['hit'])
    slope, intercept = float(lr.coef_[0][0]), float(lr.intercept_[0])
    p_place = lr.predict_proba(d[['logit_h']])[:, 1]
    picks = p_place * d['place_low'] >= THRESHOLD

    bt = json.loads((ROOT / 'models/20261025/place_market/results.json').read_text(encoding='utf-8'))['pooled']['betting']['H_1.00']
    commit = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    rule = {
        'created_at': datetime.now().astimezone().isoformat(timespec='seconds'),
        'code_commit': commit,
        'data': {'period': [DEV_START, DEV_END], 'races': int(d['race_id'].nunique()), 'horses': len(d),
                 'decision': f'latest 0B41 interim snapshot published at or before post − {DECISION_MIN} min (the 6-min snapshot)',
                 **{k: v for k, v in info.items()}},
        'win_calibration': {'beta_log_q6': round(beta, 6), 'pseudo_r2_in_sample': round(pseudo_r2(cl.predict_proba(d[['log_q6']], d['race_id']),
                                                                                              d['race_id'], d['win']), 6)},
        'place_calibration': {'intercept': round(intercept, 6), 'slope_logit_harville': round(slope, 6)},
        'paper_rule': {'bet': '複勝', 'stake_yen': 100, 'expected_value': 'calibrated place prob × T-6 place odds low',
                       'threshold': THRESHOLD, 'skip_after_min': SKIP_AFTER_MIN, 'snapshot_published_at_or_before_min': DECISION_MIN},
        'in_sample_picks': {'horses': int(picks.sum()), 'races': int(d.loc[picks, 'race_id'].nunique())},
        'backtest': {'source': 'models/20261025/place_market/results.json pooled H_1.00 (rolling folds, Jan–Jun 2026)', **bt},
        'reference': reference_tables(d),
    }
    rule['rule_hash'] = rule_hash(rule)
    out = ROOT / a.out
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(rule, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps({k: rule[k] for k in ('win_calibration', 'place_calibration', 'paper_rule', 'in_sample_picks', 'rule_hash')},
                     ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

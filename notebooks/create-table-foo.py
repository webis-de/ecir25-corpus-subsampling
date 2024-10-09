#!/usr/bin/env python3
from tqdm import tqdm
from glob import glob
import gzip
import json

PERCENTAGE_TOP_SYSTEMS = 0.75
TRUTH_FIELD = 'ground-truth-evaluation-top-10'
MEASURE = 'ndcg@10'

def score_prediction_evaluation():
    from sklearn.metrics import root_mean_squared_error
    import statistics
    ret = {}
    for i in tqdm(glob('../data/processed/evaluation-**.json.gz')):
        display_name = i.split('evaluation-')[1].split('.')[0].split('-')[0]
        ret[display_name] = {}
        with gzip.open(i, 'rt') as f:
            f = json.load(f)
        for team in f.keys():
            for method in f[team].keys():
                if method not in ret[display_name]:
                    ret[display_name][method] = {
                        'y_actual': [],
                        'y_predicted': [],
                        'condensed_y_predicted': [],
                        'actual_minus_predicted': [],
                    }
                for run, run_eval in f[team][method]['runs'].items():
                    if not run_eval['is-in-leave-out-group']:
                        continue
                    
                    actual = run_eval[TRUTH_FIELD][MEASURE]
                    predicted = run_eval['evaluation-without-post-judgments'][MEASURE]
                    predicted_condensed = run_eval['evaluation-with-post-judgments'][MEASURE + '-condensed']

                    ret[display_name][method]['y_actual'] += [actual]
                    ret[display_name][method]['y_predicted'] += [predicted]
                    ret[display_name][method]['condensed_y_predicted'] += [predicted_condensed]
                    ret[display_name][method]['actual_minus_predicted'] += [actual - predicted]

    evaluations = {}

    for dataset in ret.keys():
        evaluations[dataset] = {}
        for method in ret[dataset].keys():
            evaluations[dataset][method] = {
                'Dataset': display_name,
                'Subsampling': method,
                'rmse': root_mean_squared_error(ret[dataset][method]['y_actual'], ret[dataset][method]['y_predicted']),
                'rmse (condensed)': root_mean_squared_error(ret[dataset][method]['y_actual'], ret[dataset][method]['condensed_y_predicted']),
                'avg(actual_minus_predicted)': statistics.mean(ret[dataset][method]['actual_minus_predicted']),
                'stdev(actual_minus_predicted)': statistics.stdev(ret[dataset][method]['actual_minus_predicted']),
        }
    return evaluations

evaluation = score_prediction_evaluation()

def f(dataset, method, field):
    min_score = 10
    for k in evaluation[dataset].keys():
        min_score = min(min_score, abs(evaluation[dataset][k][field]))

    val = evaluation[dataset][method][field]
    style = '\\bfseries' if (min_score + 0.0001) >= abs(val) else ''
    
    return "{" + style + (" {:.3f}".format(val)).replace('0.', '.') + "}"

def table_line(method):
    ret = []
    for dataset in ['clueweb09', 'clueweb12', 'msmarco', 'disks45']:
        ret += [f(dataset, method, 'rmse')]

    for dataset in ['clueweb09', 'clueweb12', 'msmarco', 'disks45']:
        ret += [f(dataset, method, 'rmse (condensed)')]

    for dataset in ['clueweb09', 'clueweb12', 'msmarco', 'disks45']:
        ret += [('\\phantom{-}' if evaluation[dataset][method]['avg(actual_minus_predicted)'] >= 0 else '') + f(dataset, method, 'avg(actual_minus_predicted)') + '$_{\\pm' + ("{:.2f}".format(evaluation[dataset][method]['stdev(actual_minus_predicted)'])).replace('0.', '.') + '}$']

    return ' & '.join(ret)

print("""
\\begin{tabular}{@{}lcccc@{\\hspace{.5em}}cccc@{\\hspace{.5em}}cccc@{}}
\\toprule
\\bfseries Sampling     &   \\multicolumn{4}{c@{\\hspace{1em}}}{\\bfseries RMSE}  & \\multicolumn{4}{c@{\\hspace{1em}}}{\\bfseries RMSE$_{Judged}$}     & \\multicolumn{4}{c@{\\hspace{1em}}}{\\bfseries $\\Delta$}                                              \\\\
\\cmidrule(r@{1em}){2-5}
\\cmidrule(r@{1em}){6-9}
\\cmidrule(){10-13}

& C09 & C12 & MSM & R04 & C09 & C12 & MSM & R04 & C09 & C12 & MSM & R04 \\\\

\\midrule

BM25 & """ + table_line('re-rank-top-1000-bm25') + """ \\\\


\\midrule

LOFT$_{1k}$ & """ + table_line('loft-1000') + """ \\\\
LOFT$_{10k}$ & """ + table_line('loft-10000') + """ \\\\


\\midrule

Pool$_J$ & """ + table_line('top-10-run-pool') + """ \\\\
Pool$_{25}$ & """ + table_line('top-25-run-pool') + """ \\\\
Pool$_{50}$ & """ + table_line('top-50-run-pool') + """ \\\\
Pool$_{100}$ & """ + table_line('top-100-run-pool') + """ \\\\
Pool$_{1000}$ & """ + table_line('top-1000-run-pool') + """ \\\\

\\midrule

Full & """ + table_line('complete-corpus') + """ \\\\

\\bottomrule

\\end{tabular}
""")
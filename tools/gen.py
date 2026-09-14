#!/usr/bin/env python3
"""Synthetic test generator for the local judge (spec format).

Regimes mirror the real tests' reverse-engineered scoring parameters:
  W: pure-wait (#3-like)    w_tp=0, w_c=1, tight SLO1/SLO2, dist_base~1
  T: tp-heavy (#5/#6-like)  w_tp=0.8-0.9, loose waits, tp_UB ~ 2-3x tp_base
  S: single-token (#9/#15)  all L_out=1, w_c~0.95, overloaded arrivals
  M: mixed big-Lout (#10)   w_c~0.85, tiny SLO2 -> tpot-dominated dist
Usage: gen.py <regime> <out.spec> [seed]
"""
import sys, random

def emit(out, K, S, lat, bw, bpt, nl, slo1, slo2, tpUB, tpBase, distBase, wTp, wC,
         rows, arrs):
    with open(out, 'w') as f:
        f.write("%d %.9f %.9f %.9f %d %d\n" % (K, S, lat, bw, bpt, nl))
        f.write("%.9f %.9f %.9f %.9f %.9f %.9f %.9f\n" % (slo1, slo2, tpUB, tpBase, distBase, wTp, wC))
        f.write("%d\n" % len(rows))
        for bs, v in rows:
            f.write("%d " % bs + " ".join("%.9f" % x for x in v) + "\n")
        f.write("%d\n" % len(arrs))
        for rid, lin, lout, at in arrs:
            f.write("%d %d %d %.9f\n" % (rid, lin, lout, at))

BS = [1,2,4,8,16,32,64,128,256,512,1024,2048,4096]

def rows_from(fns):
    # fns: dict col->callable(bs)
    return [(bs, [fns[c](bs) for c in range(6)]) for bs in BS]

def regime_W(out, seed):
    rnd = random.Random(seed)
    K, S, lat, bw, bpt, nl = 2, 8, 5.0, 10.0, 50000, 32
    slo1, slo2, distBase = 615.0, 110.0, 1.0
    tpBase, tpUB, wTp, wC = 0.004, 0.02, 0.0, 1.0
    def pre_proc(bs): return 60.0 + 0.30 * bs          # 98..1289 ms
    def pre_pre(bs):  return 3.0 + 0.003 * bs
    def pre_post(bs): return 3.0 + 0.003 * bs
    def dec_pre(m):   return 3.0 + 0.006 * m
    def dec_proc(m):  return 20.0 + 0.10 * m
    def dec_post(m):  return 3.0 + 0.006 * m
    rows = rows_from({0: pre_pre, 1: pre_proc, 2: pre_post, 3: dec_pre, 4: dec_proc, 5: dec_post})
    R = 400
    arrs = []
    t = 0.0
    for i in range(R):
        lin = rnd.choice([128,256,512,1024,2048,4096])
        lout = rnd.choice([1,2,4,8,16,32])
        arrs.append((i, lin, lout, t))
        t += rnd.uniform(240, 700)
    emit(out, K, S, lat, bw, bpt, nl, slo1, slo2, tpUB, tpBase, distBase, wTp, wC, rows, arrs)

def regime_T(out, seed):
    rnd = random.Random(seed)
    K, S, lat, bw, bpt, nl = 8, 8, 2.0, 25.0, 125000, 16
    slo1, slo2, distBase = 2000.0, 200.0, 50.0
    tpBase, tpUB, wTp, wC = 0.3, 2.2, 0.85, 0.15
    def pre_proc(bs): return 10.0 + 0.02 * bs
    def pre_pre(bs):  return 2.0 + 0.002 * bs
    def pre_post(bs): return 2.0 + 0.002 * bs
    def dec_pre(m):   return 2.0 + 0.016 * m
    def dec_proc(m):  return 2.0 + 0.06 * m
    def dec_post(m):  return 2.0 + 0.016 * m
    rows = rows_from({0: pre_pre, 1: pre_proc, 2: pre_post, 3: dec_pre, 4: dec_proc, 5: dec_post})
    R = 800
    arrs = []
    t = 0.0
    for i in range(R):
        lin = rnd.choice([64,128,256,512])
        lout = rnd.choice([8,16,32,64])
        arrs.append((i, lin, lout, t))
        t += rnd.uniform(2.0, 10.0)   # heavy load, arrivals packed
    emit(out, K, S, lat, bw, bpt, nl, slo1, slo2, tpUB, tpBase, distBase, wTp, wC, rows, arrs)

def regime_S(out, seed):
    rnd = random.Random(seed)
    K, S, lat, bw, bpt, nl = 2, 3, 8.0, 2.0, 80000, 8
    slo1, slo2, distBase = 554.0, 1e9, 33.85
    tpBase, tpUB, wTp, wC = 0.01, 0.2, 0.05, 0.95
    def pre_proc(bs): return 20.0 + 0.08 * bs
    def pre_pre(bs):  return 2.0 + 0.002 * bs
    def pre_post(bs): return 2.0 + 0.002 * bs
    def dec_pre(m):   return 1.0 + 0.004 * m
    def dec_proc(m):  return 2.0 + 0.03 * m
    def dec_post(m):  return 1.0 + 0.004 * m
    rows = rows_from({0: pre_pre, 1: pre_proc, 2: pre_post, 3: dec_pre, 4: dec_proc, 5: dec_post})
    R = 600
    arrs = []
    t = 0.0
    for i in range(R):
        lin = rnd.choice([64,128,256,512])
        lout = 1
        arrs.append((i, lin, lout, t))
        t += rnd.uniform(30, 80)   # overloaded: service takes ~100-200ms each
    emit(out, K, S, lat, bw, bpt, nl, slo1, slo2, tpUB, tpBase, distBase, wTp, wC, rows, arrs)

def regime_M(out, seed):
    rnd = random.Random(seed)
    K, S, lat, bw, bpt, nl = 4, 8, 3.0, 8.0, 100000, 24
    slo1, slo2, distBase = 1006.0, 40.0, 389.0
    tpBase, tpUB, wTp, wC = 0.005, 0.6, 0.15, 0.85
    def pre_proc(bs): return 20.0 + 0.06 * bs
    def pre_pre(bs):  return 2.0 + 0.002 * bs
    def pre_post(bs): return 2.0 + 0.002 * bs
    def dec_pre(m):   return 5.0 + 0.02 * m
    def dec_proc(m):  return 400.0 + 0.05 * m
    def dec_post(m):  return 5.0 + 0.02 * m
    rows = rows_from({0: pre_pre, 1: pre_proc, 2: pre_post, 3: dec_pre, 4: dec_proc, 5: dec_post})
    R = 300
    arrs = []
    t = 0.0
    for i in range(R):
        lin = rnd.choice([256,512,1024,2048])
        lout = rnd.choice([64,128,256,512])
        arrs.append((i, lin, lout, t))
        t += rnd.uniform(5000, 9000)
    emit(out, K, S, lat, bw, bpt, nl, slo1, slo2, tpUB, tpBase, distBase, wTp, wC, rows, arrs)


def regime_W2(out, seed):
    rnd = random.Random(seed)
    K, S, lat, bw, bpt, nl = 3, 8, 5.0, 10.0, 50000, 32
    slo1, slo2, distBase = 615.0, 110.0, 1.0
    tpBase, tpUB, wTp, wC = 0.004, 0.02, 0.0, 1.0
    def pre_proc(bs): return 80.0 + 0.40 * bs          # 285..1718 ms
    def pre_pre(bs):  return 3.0 + 0.003 * bs
    def pre_post(bs): return 3.0 + 0.003 * bs
    def dec_pre(m):   return 3.0 + 0.006 * m
    def dec_proc(m):  return 15.0 + 0.08 * m
    def dec_post(m):  return 3.0 + 0.006 * m
    rows = rows_from({0: pre_pre, 1: pre_proc, 2: pre_post, 3: dec_pre, 4: dec_proc, 5: dec_post})
    R = 300
    arrs = []
    t = 0.0
    for i in range(R):
        lin = rnd.choice([512,1024,2048,4096])
        lout = rnd.choice([32,64,128])
        arrs.append((i, lin, lout, t))
        t += rnd.uniform(500, 1200)
    emit(out, K, S, lat, bw, bpt, nl, slo1, slo2, tpUB, tpBase, distBase, wTp, wC, rows, arrs)


def regime_T2(out, seed):
    rnd = random.Random(seed)
    K, S, lat, bw, bpt, nl = 8, 8, 5.0, 25.0, 125000, 64
    slo1, slo2, distBase = 2000.0, 50.0, 1.0
    tpBase, tpUB, wTp, wC = 0.3, 2.2, 0.9, 0.1
    def pre_proc(bs): return 8.0 + 0.04 * bs
    def pre_pre(bs):  return 2.0 + 0.002 * bs
    def pre_post(bs): return 2.0 + 0.002 * bs
    def dec_pre(m):   return 1.0 + 0.004 * m
    def dec_proc(m):  return 2.0 + 0.05 * m + 0.0008 * m * m   # convex: m* ~ 50
    def dec_post(m):  return 1.0 + 0.004 * m
    rows = rows_from({0: pre_pre, 1: pre_proc, 2: pre_post, 3: dec_pre, 4: dec_proc, 5: dec_post})
    R = 800
    arrs = []
    t = 0.0
    for i in range(R):
        lin = rnd.choice([256,512,1024,2048])
        lout = rnd.choice([16,32,64])
        arrs.append((i, lin, lout, t))
        t += rnd.uniform(12, 30)
    emit(out, K, S, lat, bw, bpt, nl, slo1, slo2, tpUB, tpBase, distBase, wTp, wC, rows, arrs)


def regime_T3(out, seed):
    rnd = random.Random(seed)
    K, S, lat, bw, bpt, nl = 2, 8, 5.0, 25.0, 125000, 64
    slo1, slo2, distBase = 4000.0, 80.0, 1.0
    tpBase, tpUB, wTp, wC = 0.05, 0.25, 0.9, 0.1
    def pre_proc(bs): return 8.0 + 0.04 * bs
    def pre_pre(bs):  return 25.0 + 0.004 * bs     # long E prefill -> decode pools accumulate
    def pre_post(bs): return 25.0 + 0.004 * bs
    def dec_pre(m):   return 1.0 + 0.004 * m
    def dec_proc(m):  return 2.0 + 0.04 * m + 0.0015 * m * m   # convex: m* ~ 83
    def dec_post(m):  return 1.0 + 0.004 * m
    rows = rows_from({0: pre_pre, 1: pre_proc, 2: pre_post, 3: dec_pre, 4: dec_proc, 5: dec_post})
    R = 400
    arrs = []
    t = 0.0
    for i in range(R):
        lin = rnd.choice([512,1024,2048])
        lout = rnd.choice([32,64,128])
        arrs.append((i, lin, lout, t))
        t += rnd.uniform(40, 100)
    emit(out, K, S, lat, bw, bpt, nl, slo1, slo2, tpUB, tpBase, distBase, wTp, wC, rows, arrs)



def regime_T4(out, seed):
    rnd = random.Random(seed)
    K, S, lat, bw, bpt, nl = 4, 8, 5.0, 25.0, 125000, 64
    slo1, slo2, distBase = 5000.0, 40.0, 1.0
    tpBase, tpUB, wTp, wC = 0.05, 1.2, 0.9, 0.1
    def pre_proc(bs): return 30.0 + 0.25 * bs     # slow prefill -> P PROC starves if D PROC wins
    def pre_pre(bs):  return 2.0 + 0.002 * bs
    def pre_post(bs): return 2.0 + 0.002 * bs
    def dec_pre(m):   return 1.0 + 0.004 * m
    def dec_proc(m):  return 3.0 + 0.25 * m      # remotes decode-saturated
    def dec_post(m):  return 1.0 + 0.004 * m
    rows = rows_from({0: pre_pre, 1: pre_proc, 2: pre_post, 3: dec_pre, 4: dec_proc, 5: dec_post})
    R = 400
    arrs = []
    t = 0.0
    for i in range(R):
        lin = rnd.choice([256,512,1024,2048])
        lout = rnd.choice([16,32,64,128])
        arrs.append((i, lin, lout, t))
        t += rnd.uniform(8, 25)
    emit(out, K, S, lat, bw, bpt, nl, slo1, slo2, tpUB, tpBase, distBase, wTp, wC, rows, arrs)

if __name__ == '__main__':
    regime, out = sys.argv[1], sys.argv[2]
    seed = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    globals()['regime_' + regime](out, seed)
    print("wrote", out, "regime", regime)

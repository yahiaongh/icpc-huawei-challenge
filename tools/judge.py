#!/usr/bin/env python3
"""Local judge/simulator for Codeforces 2251A (Edge-Cloud Collaborative Scheduling).

Implements the interactor model EXACTLY as specified in the statement:
  - schedule cost S per task, dur from piecewise-linear task-time table
  - transfers: UP/DOWN independent FIFO, time = latency + 8*bytes/(bw*1e6)
  - frames coalesced by internal event time; TDN/XDN/FIN event formats
  - scoring: tp, tdr, tpot, dist, clamp components, 1000*(w_tp*norm_tp + w_c*norm_c)

Usage: judge.py <spec_file> <solver_binary>
Spec format:
  line1: K S latency bandwidth bytes_per_token num_layers
  line2: SLO1 SLO2 tp_UB tp_base dist_base w_tp w_c
  line3: N
  N rows: batch_size pre_pre pre_proc pre_post dec_pre dec_proc dec_post  (-1 = missing)
  line: R
  R rows: rid lin lout arrival_ms
"""
import sys, subprocess, heapq

class Spec: pass

def read_spec(path):
    s = Spec()
    with open(path) as f:
        raw = f.read().split()
    p = 0
    def nxt():
        nonlocal p
        v = raw[p]; p += 1
        return v
    s.K = int(nxt()); s.S = float(nxt()); s.lat = float(nxt()); s.bw = float(nxt())
    s.bpt = int(nxt()); s.nl = int(nxt())
    s.slo1 = float(nxt()); s.slo2 = float(nxt()); s.tpUB = float(nxt())
    s.tpBase = float(nxt()); s.distBase = float(nxt()); s.wTp = float(nxt()); s.wC = float(nxt())
    s.N = int(nxt())
    s.rows = []
    for _ in range(s.N):
        bs = int(nxt())
        vals = [float(nxt()) for _ in range(6)]
        s.rows.append((bs, vals))
    s.R = int(nxt())
    s.arr = []
    for _ in range(s.R):
        rid = int(nxt()); lin = int(nxt()); lout = int(nxt()); at = float(nxt())
        s.arr.append((rid, lin, lout, at))
    return s

def make_tables(spec):
    tabs = [[] for _ in range(6)]
    for bs, vals in spec.rows:
        for c in range(6):
            if vals[c] >= 0:
                tabs[c].append((bs, vals[c]))
    for c in range(6):
        tabs[c].sort()
    return tabs

def interp(tab, x):
    if not tab:
        return 0.0
    if x <= tab[0][0]:
        return tab[0][1]
    if x >= tab[-1][0]:
        return tab[-1][1]
    lo, hi = 0, len(tab) - 1
    while lo + 1 < hi:
        mid = (lo + hi) // 2
        if tab[mid][0] <= x:
            lo = mid
        else:
            hi = mid
    frac = (x - tab[lo][0]) / float(tab[hi][0] - tab[lo][0])
    return tab[lo][1] + frac * (tab[hi][1] - tab[lo][1])

# request phases
PH_ARR, PH_PRE_F, PH_PRE_READY, PH_PROC_F, PH_POST_READY, PH_READY, \
PH_DPRE_F, PH_DPROC_READY, PH_DPROC_F, PH_DPOST_READY = range(10)

class Req:
    __slots__ = ('rid','lin','lout','arr','remote','phase','nextLs','iter',
                 'ppost','toktimes','finished','inFlight')
    def __init__(self, rid, lin, lout, arr):
        self.rid = rid; self.lin = lin; self.lout = lout; self.arr = arr
        self.remote = -1; self.phase = PH_ARR; self.nextLs = 0; self.iter = 0
        self.ppost = None; self.toktimes = []; self.finished = False
        self.inFlight = False

def main():
    spec_path, solver_bin = sys.argv[1], sys.argv[2]
    spec = read_spec(spec_path)
    tabs = make_tables(spec)
    E = 0
    C = [0] * spec.K
    up_busy = 0.0
    down_busy = 0.0
    reqs = {}
    for rid, lin, lout, at in spec.arr:
        reqs[rid] = Req(rid, lin, lout, at)

    seq = 0
    events = []
    for rid, lin, lout, at in spec.arr:
        events.append((at, seq, 'ARR', (rid, lin))); seq += 1
    stats = {'E': {'tasks': 0, 'busy': 0.0}, 'C': {'tasks': 0, 'busy': 0.0},
             'UP': {'busy': 0.0}, 'DOWN': {'busy': 0.0}}
    st_batch = {}
    st_pieces = {'full': 0, 'split': 0}
    t_last = 0.0

    proc = subprocess.Popen([solver_bin], stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=0)
    def send(line):
        proc.stdin.write((line + '\n').encode())
    def recv():
        line = proc.stdout.readline()
        return line.decode().strip() if line else None

    send("%d %.9f %.9f %.9f %d %d" % (spec.K, spec.S, spec.lat, spec.bw, spec.bpt, spec.nl))
    send("%.9f %.9f %.9f %.9f %.9f %.9f %.9f" % (spec.slo1, spec.slo2, spec.tpUB,
                                                  spec.tpBase, spec.distBase, spec.wTp, spec.wC))
    send("%d" % spec.N)
    for bs, vals in spec.rows:
        send("%d %.9f %.9f %.9f %.9f %.9f %.9f" % (bs, vals[0], vals[1], vals[2], vals[3], vals[4], vals[5]))

    violations = []
    def viol(msg):
        if not violations:
            violations.append(msg)

    def queue_xfer(t, direction, remote, size, kindx, ids, order):
        nonlocal up_busy, down_busy, seq
        xf = spec.lat + 8.0 * size / (spec.bw * 1e6)
        if direction == 'UP':
            done = max(up_busy, t) + xf
            up_busy = done
        else:
            done = max(down_busy, t) + xf
            down_busy = done
        heapq.heappush(events, (done, order, 'XDN', (direction, remote, size, kindx, ids)))

    def apply_tdn(t, server, kind, toks, fin_out):
        nonlocal E, C
        if server == 'E':
            E = 0
        else:
            C[int(server[1:])] = 0
        if kind == 'P_PRE':
            c, rid = toks
            r = reqs[rid]
            r.inFlight = False
            r.phase = PH_PRE_F
            queue_xfer(t, 'UP', c, r.lin * spec.bpt, 'PRE', [rid], 10**15)
        elif kind == 'P_PROC':
            ls, le, c2, rid = toks
            r = reqs[rid]
            r.inFlight = False
            r.nextLs = le
            if le >= spec.nl:
                r.phase = PH_PRE_F  # waiting DOWN
                queue_xfer(t, 'DOWN', c2, r.lin * spec.bpt, 'PRE', [rid], 10**15)
            else:
                r.phase = PH_PROC_F
        elif kind == 'P_POST':
            c, rid = toks
            r = reqs[rid]
            r.inFlight = False
            r.ppost = t
            r.phase = PH_READY
        elif kind == 'D_PRE':
            m, ids = toks
            for c in range(spec.K):
                mems = [i for i in ids if reqs[i].remote == c]
                if mems:
                    queue_xfer(t, 'UP', c, len(mems) * spec.bpt, 'DEC', mems, 10**15)
            for i in ids:
                reqs[i].inFlight = False
                reqs[i].phase = PH_DPRE_F
        elif kind == 'D_PROC':
            c2, m, ids = toks
            queue_xfer(t, 'DOWN', c2, m * spec.bpt, 'DEC', ids, 10**15)
            for i in ids:
                reqs[i].inFlight = False
                reqs[i].phase = PH_DPROC_F
        elif kind == 'D_POST':
            m, ids = toks
            for i in ids:
                r = reqs[i]
                r.inFlight = False
                r.toktimes.append(t)
                r.iter += 1
                if r.iter >= r.lout:
                    r.phase = PH_DPROC_F
                    r.finished = True
                    fin_out.append(i)
                else:
                    r.phase = PH_READY

    def apply_xdn(d, ids, kindx):
        if kindx == 'PRE':
            rid = ids[0]
            if d == 'UP':
                reqs[rid].phase = PH_PRE_READY
            else:
                reqs[rid].phase = PH_POST_READY
        else:
            if d == 'UP':
                for i in ids:
                    reqs[i].phase = PH_DPROC_READY
            else:
                for i in ids:
                    reqs[i].phase = PH_DPOST_READY

    def dur_of(kind, toks):
        if kind == 'P_PRE':
            return interp(tabs[0], reqs[toks[1]].lin)
        if kind == 'P_PROC':
            ls, le, c2, rid = toks
            return (le - ls) / float(spec.nl) * interp(tabs[1], reqs[rid].lin)
        if kind == 'P_POST':
            return interp(tabs[2], reqs[toks[1]].lin)
        if kind == 'D_PRE':
            return interp(tabs[3], toks[0])
        if kind == 'D_PROC':
            return interp(tabs[4], toks[1])
        return interp(tabs[5], toks[0])

    while True:
        if not events:
            viol("no events left before all finished")
            break
        t, s0, kind, pl = heapq.heappop(events)
        frame = [(t, s0, kind, pl)]
        while events and abs(events[0][0] - t) < 1e-9:
            frame.append(heapq.heappop(events))
        frame.sort(key=lambda e: e[1])

        evlines = []
        fin_out = []
        for _, s1, k1, pl1 in frame:
            if k1 == 'ARR':
                rid, lin = pl1
                evlines.append("ARR %d %d" % (rid, lin))
            elif k1 == 'TDN':
                server, kind, toks, dur = pl1
                if server == 'E':
                    stats['E']['tasks'] += 1; stats['E']['busy'] += spec.S + dur
                else:
                    stats['C']['tasks'] += 1; stats['C']['busy'] += spec.S + dur
                if kind == 'D_PRE' or kind == 'D_PROC' or kind == 'D_POST':
                    st_batch[toks[0]] = st_batch.get(toks[0], 0) + 1
                if kind == 'P_PROC':
                    if toks[1] >= spec.nl: st_pieces['full'] += 1
                    else: st_pieces['split'] += 1
                apply_tdn(t, server, kind, toks, fin_out)
                if kind == 'P_PRE':
                    spec_t = "P PRE %d %d" % (toks[0], toks[1])
                elif kind == 'P_PROC':
                    spec_t = "P PROC %d %d %d %d" % (toks[0], toks[1], toks[2], toks[3])
                elif kind == 'P_POST':
                    spec_t = "P POST %d %d" % (toks[0], toks[1])
                elif kind == 'D_PRE':
                    spec_t = "D PRE -1 %d %s" % (toks[0], ' '.join(map(str, toks[1])))
                elif kind == 'D_PROC':
                    spec_t = "D PROC %d %d %s" % (toks[0], toks[1], ' '.join(map(str, toks[2])))
                else:
                    spec_t = "D POST -1 %d %s" % (toks[0], ' '.join(map(str, toks[1])))
                evlines.append("TDN %s %s %.9f" % (server, spec_t, dur))
                for fid in fin_out:
                    evlines.append("FIN %d" % fid)
                fin_out.clear()
            elif k1 == 'XDN':
                d, rem, size, kindx, ids = pl1
                apply_xdn(d, ids, kindx)
                evlines.append("XDN %s %d %d %s %d %s" % (d, rem, size, kindx, len(ids), ' '.join(map(str, ids))))
        fin_out.clear()

        t_last = t
        lines = ["%.9f" % t, "%d" % len(evlines)] + evlines
        send('\n'.join(lines))
        if __import__('os').environ.get('TRACE'):
            print('[J] ' + ' | '.join(lines), file=__import__('sys').stderr)
        cnt_line = recv()
        if cnt_line is None:
            viol("solver exited early")
            break
        cnt = int(cnt_line)
        assigns = []
        for _ in range(cnt):
            a = recv()
            if a is None:
                viol("solver exited mid-response")
                break
            assigns.append(a)
        if __import__('os').environ.get('TRACE'):
            print('[P] %s' % assigns, file=__import__('sys').stderr)
        if violations:
            break

        def parse(kind, toks2):
            nonlocal seq
            dur = dur_of(kind, toks2)
            completion = t + spec.S + dur
            heapq.heappush(events, (completion, seq, 'TDN', (server, kind, toks2, dur)))
            seq += 1

        for a in assigns:
            toks = a.split()
            if len(toks) < 2:
                viol("bad assignment %r" % a); break
            server = toks[0]
            if server == 'E':
                if E:
                    viol("E busy: %s" % a); break
            elif server.startswith('C') and server[1:].isdigit():
                c = int(server[1:])
                if c >= spec.K or C[c]:
                    viol("C%d busy/bad: %s" % (c, a)); break
            else:
                viol("bad server %r" % a); break

            word = toks[1]
            if word == 'P':
                if toks[2] == 'PRE' and server == 'E' and len(toks) == 5:
                    c2, rid = int(toks[3]), int(toks[4])
                    r = reqs[rid]
                    if c2 < 0 or c2 >= spec.K: viol("P PRE remote out of range"); break
                    if r.phase != PH_ARR or r.finished or r.inFlight:
                        viol("P PRE bad state rid=%d phase=%d" % (rid, r.phase)); break
                    r.remote = c2; r.inFlight = True; E = 1
                    parse('P_PRE', (c2, rid))
                elif toks[2] == 'POST' and server == 'E' and len(toks) == 5:
                    c2, rid = int(toks[3]), int(toks[4])
                    r = reqs[rid]
                    if r.phase != PH_POST_READY or r.finished or r.inFlight or r.remote != c2:
                        viol("P POST bad state rid=%d phase=%d" % (rid, r.phase)); break
                    r.inFlight = True; E = 1
                    parse('P_POST', (c2, rid))
                elif toks[2] == 'PROC' and len(toks) == 7:
                    ls, le, c2, rid = int(toks[3]), int(toks[4]), int(toks[5]), int(toks[6])
                    r = reqs[rid]
                    if server == 'E': viol("P PROC on E"); break
                    rc = int(server[1:])
                    if rc != c2: viol("P PROC remote mismatch"); break
                    if c2 != r.remote: viol("P PROC wrong assigned remote"); break
                    if not (0 <= ls < le <= spec.nl): viol("P PROC bad range"); break
                    if r.inFlight or r.finished: viol("P PROC in flight/finished"); break
                    if r.nextLs == 0:
                        if r.phase != PH_PRE_READY: viol("P PROC first piece bad state rid=%d" % rid); break
                    else:
                        if r.phase != PH_PROC_F or ls != r.nextLs: viol("P PROC later piece bad state rid=%d" % rid); break
                    r.inFlight = True; C[rc] = 1
                    parse('P_PROC', (ls, le, c2, rid))
                else:
                    viol("unparsed P %r" % a); break
            elif word == 'D':
                if toks[2] == 'PRE' and server == 'E' and toks[3] == '-1':
                    m = int(toks[4])
                    ids = [int(x) for x in toks[5:]]
                    if len(ids) != m or len(set(ids)) != m: viol("bad D PRE group"); break
                    bad = [i for i in ids if reqs[i].phase != PH_READY or reqs[i].finished or reqs[i].inFlight]
                    if bad:
                        viol("D PRE bad state rid=%d" % bad[0]); break
                    for i in ids: reqs[i].inFlight = True
                    E = 1
                    parse('D_PRE', (m, ids))
                elif toks[2] == 'POST' and server == 'E' and toks[3] == '-1':
                    m = int(toks[4])
                    ids = [int(x) for x in toks[5:]]
                    if len(ids) != m or len(set(ids)) != m: viol("bad D POST group"); break
                    bad = [i for i in ids if reqs[i].phase != PH_DPOST_READY or reqs[i].finished or reqs[i].inFlight]
                    if bad:
                        viol("D POST bad state rid=%d" % bad[0]); break
                    for i in ids: reqs[i].inFlight = True
                    E = 1
                    parse('D_POST', (m, ids))
                elif toks[2] == 'PROC' and server.startswith('C'):
                    c2 = int(toks[3])
                    m = int(toks[4])
                    ids = [int(x) for x in toks[5:]]
                    if len(ids) != m or len(set(ids)) != m: viol("bad D PROC group"); break
                    bad = [i for i in ids if reqs[i].remote != c2 or reqs[i].phase != PH_DPROC_READY
                           or reqs[i].finished or reqs[i].inFlight]
                    if bad:
                        viol("D PROC bad state rid=%d" % bad[0]); break
                    for i in ids: reqs[i].inFlight = True
                    C[int(server[1:])] = 1
                    parse('D_PROC', (c2, m, ids))
                else:
                    viol("unparsed D %r" % a); break
            else:
                viol("unparsed %r" % a); break

        if violations:
            break

        if all(r.finished for r in reqs.values()):
            send("END")
            proc.stdin.flush()
            break
        if not events:
            viol("stuck state")
            break

    proc.stdin.flush()
    try:
        proc.wait(timeout=10)
    except Exception:
        proc.kill()

    if violations:
        print("VIOLATION:", violations[0])
        print("points=0.0")
        return

    if __import__('os').environ.get('STATS'):
        last = max(r.toktimes[-1] for r in reqs.values())
        print("STATS last=%.1f E.tasks=%d E.util=%.3f C.util=%.3f"
              % (last, stats['E']['tasks'], stats['E']['busy']/last,
                 stats['C']['busy']/(last*spec.K)))
        print("STATS UP.total=%.1f DOWN.total=%.1f" % (stats['UP']['busy'], stats['DOWN']['busy']))
        print("STATS batches:", dict(sorted(st_batch.items())))
        print("STATS pieces full=%d split=%d" % (st_pieces['full'], st_pieces['split']))

    total_tokens = sum(r.lout for r in reqs.values())
    first_arr = min(r.arr for r in reqs.values())
    last_tok = max(r.toktimes[-1] for r in reqs.values())
    tp = total_tokens / (last_tok - first_arr)
    tdr = sum(r.ppost - r.arr for r in reqs.values()) / len(reqs)
    gaps = []
    for r in reqs.values():
        gaps.extend(r.toktimes[i+1] - r.toktimes[i] for i in range(len(r.toktimes)-1))
    tpot = sum(gaps) / len(gaps) if gaps else 0.0
    ex_tdr = max(0.0, (tdr - spec.slo1) / spec.slo1) if spec.slo1 > 0 else 0.0
    ex_tpot = max(0.0, (tpot - spec.slo2) / spec.slo2) if spec.slo2 > 0 else 0.0
    dist = (ex_tdr**2 + ex_tpot**2) ** 0.5
    def clamp(x, base, target):
        if target <= base:
            return 1.0 if x >= target else 0.0
        return max(0.0, min(1.0, (x - base) / (target - base)))
    norm_tp = clamp(tp, spec.tpBase, spec.tpUB)
    if spec.distBase > 0:
        norm_c = max(0.0, 1.0 - dist / spec.distBase)
    else:
        norm_c = 1.0 if dist == 0 else 0.0
    norm = spec.wTp * norm_tp + spec.wC * norm_c
    print("tp=%.6f mean_tdr=%.6f mean_tpot=%.6f dist=%.6f norm_tp=%.6f norm_c=%.6f normalized=%.6f points=%.6f"
          % (tp, tdr, tpot, dist, norm_tp, norm_c, norm, 1000.0 * norm))

if __name__ == '__main__':
    main()
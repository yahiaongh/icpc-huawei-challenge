#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <vector>
#include <set>
#include <string>
#include <algorithm>
#include <unistd.h>


using namespace std;


static const int IBUF_SZ = 1 << 20;
static char ibuf[IBUF_SZ];
static int ilen = 0, ipos = 0;
static inline int gc() {
    if (ipos == ilen) { ilen = (int)read(0, ibuf, IBUF_SZ); ipos = 0; if (ilen <= 0) return -1; }
    return (unsigned char)ibuf[ipos++];
}
static char tokbuf[64];
static inline int readTok() {
    int c = gc();
    while (c == ' ' || c == '\n' || c == '\r' || c == '\t') c = gc();
    if (c < 0) return -1;
    int n = 0;
    while (c >= 0 && c != ' ' && c != '\n' && c != '\r' && c != '\t') { if (n < 62) tokbuf[n++] = (char)c; c = gc(); }
    tokbuf[n] = 0;
    return n;
}
static inline long long readInt() { readTok(); return atoll(tokbuf); }
static inline double readDouble() { readTok(); return atof(tokbuf); }


static const int OBUF_SZ = 1 << 20;
static char obuf[OBUF_SZ];
static int opos = 0;
static inline void flushOut() { if (opos) { ssize_t w = write(1, obuf, opos); (void)w; opos = 0; } }
static inline void wc(char c) { if (opos == OBUF_SZ) flushOut(); obuf[opos++] = c; }
static inline void wstr(const char* s) { while (*s) wc(*s++); }
static inline void wint(long long v) {
    if (v < 0) { wc('-'); v = -v; }
    char t[24]; int n = 0;
    if (v == 0) t[n++] = '0';
    while (v > 0) { t[n++] = char('0' + v % 10); v /= 10; }
    while (n > 0) wc(t[--n]);
}


struct Config {
    int K = 0, bytesPerToken = 0, numLayers = 0;
    double S = 0, latency = 0, bandwidth = 0;
    double slo1 = 0, slo2 = 0, tpUB = 0, tpBase = 0, distBase = 0, wTp = 0, wC = 0;
} cfg;


static double chunkTargetFactor = 0.25;
static int feedTarget = 8;
static int multiToken = 0;
static double pProcBias = 0.0;
static int dBatchThresh = 0;
static int holdThresh = 0;
static int inFlightCap = 0;
static int decodeInFlight = 0;
static int feedEscape = 0;
static int dHold = 0;
static double dMinRate = 0.02;
static int dProcHold = 0;
static int dPostHold = 0;
static double dFixedT = 100.0;
static double dFixedEst = 0.0;
static double dAccumRate = 0.3;
static double dRate = 0.0;
static double dWinStart = 0.0;
static long long dWinArr = 0;
static double dSumM = 0, dSumM2 = 0, dSumD = 0, dSumMD = 0;
static int dRegN = 0;
static bool prefillDrained = false;
static vector<long long> dArrived;
static vector<int> decInFlightPer;
static int upInFlight = 0;
static int pUpTransit = 0;
static int pDownTransit = 0;
static long long dArrivedTotal = 0;
static long long gNPre = 0, gNPost = 0, gNDPre = 0, gNDPost = 0, gNDPreTok = 0, gNDPostTok = 0, gHeld = 0, gFeed = 0, gP8 = 0, gPProc = 0, gPF = 0;
static int dPoolStall = 0;
static int dLastPool = -1;


struct Req {
    int lin = 0, remote = -1;
    double arrival = 0, readyTime = 0, lastTokenTime = 0;
    long long tokenCount = 0;
    bool finished = false;
    int nextLs = 0;
};
static vector<Req> req;


typedef vector<pair<int,double>> Table;
static Table tblPProc, tblPPre, tblPPost;


static double interp(const Table& t, double x) {
    if (t.empty()) return 0.0;
    if (x <= t.front().first) return t.front().second;
    if (x >= t.back().first) return t.back().second;
    int lo = 0, hi = (int)t.size() - 1;
    while (lo + 1 < hi) { int mid = (lo + hi) / 2; if (t[mid].first <= x) lo = mid; else hi = mid; }
    double frac = (x - t[lo].first) / double(t[hi].first - t[lo].first);
    return t[lo].second + frac * (t[hi].second - t[lo].second);
}


static bool localFree = true;
static vector<char> remoteFree;
static vector<int> remoteLoad;
static vector<double> remoteWork; // cumulative estimated P PROC compute assigned per remote
static vector<char> remoteDBusy;


static set<pair<double,int>> arrivedSet, readyPPost, readyDecodeStart, readyDPost;
static vector<set<pair<double,int>>> readyPProc, readyDProc;


static inline double urgency(double elapsed, double slo, int groupSize) {
    double slack = (slo - elapsed) / slo;
    double tb = groupSize > 1 ? 0.001 * log1p((double)groupSize) : 0.0;
    return tb - slack;
}
static int newestFirst = 0;
static double nfAlpha = 0.4;


static double dUrgency(set<pair<double,int>> &pool, double now, double slo, int n) {
    if (!newestFirst) {
        return urgency(now - pool.begin()->first, slo, n);
    }
    double maxArr = -1e18;
    for (auto &p : pool) maxArr = max(maxArr, req[p.second].arrival);
    double uNew = (maxArr - now) / slo;
    double uOld = urgency(now - pool.begin()->first, slo, n);
    return nfAlpha * uNew + (1.0 - nfAlpha) * uOld;
}
static inline int pickRemote() {
    // Balance by actual estimated prefill compute (from the real task-time
    // table), not raw request count.
    //
    // FIX (isolated, low-risk, kept from the previous round): remoteWork
    // must actually be initialized and updated for this to mean anything.
    // The original baseline declared this vector but never sized or wrote
    // to it, so for K>1 this read undefined memory. This fix only
    // initializes/updates the vector; it does not change any scheduling
    // priority logic, so it cannot explain the regression seen last round.
    int best = 0;
    for (int c = 1; c < cfg.K; c++) if (remoteWork[c] < remoteWork[best]) best = c;
    return best;
}


static vector<int> takeAll(set<pair<double,int>>& s) {
    vector<int> result; result.reserve(s.size());
    for (auto &pr : s) result.push_back(pr.second);
    s.clear();
    return result;
}


static void onArr(int rid, int lin, double now) {
    if ((int)req.size() <= rid) req.resize(rid + 1);
    req[rid] = Req();
    req[rid].lin = lin;
    req[rid].arrival = now;
    arrivedSet.insert({now, rid});
}


struct TdnEv { int localOrRemote, stage, step; int ls=-1,le=-1,remote=-1; vector<int> ids; double dur = 0; };
struct XdnEv { int dir, remote, kind; vector<int> ids; };


static void onTdn(const TdnEv &ev, double now, const vector<int>& finRids) {
    if (ev.localOrRemote == -1) localFree = true; else remoteFree[ev.localOrRemote] = 1;
    if (ev.localOrRemote != -1 && ev.stage == 1 && ev.step == 1) remoteDBusy[ev.localOrRemote] = 0;
    if (ev.stage == 1 && ev.step == 1 && ev.dur > 0.0) {
        double m = (double)ev.ids.size();
        dSumM += m; dSumM2 += m * m; dSumD += ev.dur; dSumMD += m * ev.dur; dRegN++;
        if (dRegN > 64) { dSumM *= 0.5; dSumM2 *= 0.5; dSumD *= 0.5; dSumMD *= 0.5; dRegN = 32; }
        double n = (double)dRegN;
        double denom = n * dSumM2 - dSumM * dSumM;
        if (denom <= 1e-9) dFixedEst = dSumD / n;
        else {
            double perT = (n * dSumMD - dSumM * dSumD) / denom;
            dFixedEst = (dSumD - perT * dSumM) / n;
        }
        if (dFixedEst < 0.0) dFixedEst = 0.0;
    }
    if (ev.stage == 0) {
        if (ev.step == 1) {
            int rid = ev.ids[0]; Req &r = req[rid];
            if (ev.le != cfg.numLayers) readyPProc[ev.remote].insert({r.arrival, rid});
        } else if (ev.step == 2) {
            int rid = ev.ids[0]; Req &r = req[rid];
            r.readyTime = now;
            readyDecodeStart.insert({now, rid});
        }
    } else {
        if (ev.step == 2) {
            for (int rid : ev.ids) {
                Req &r = req[rid];
                r.tokenCount++; r.lastTokenTime = now; r.readyTime = now;
                bool willFinish = false;
                for (int f : finRids) if (f == rid) { willFinish = true; break; }
                if (r.tokenCount >= 2) multiToken = 1;
                if (!willFinish) readyDecodeStart.insert({now, rid});
            }
            decodeInFlight -= (int)ev.ids.size();
        }
    }
}
static void onXdn(const XdnEv &ev, double now) {
    if (ev.kind == 0) {
        int rid = ev.ids[0];
        if (ev.dir == 0) { pUpTransit--; readyPProc[ev.remote].insert({req[rid].arrival, rid}); }
        else { pDownTransit--; readyPPost.insert({req[rid].arrival, rid}); }
    } else {
        if (ev.dir == 0) {
            dArrived[ev.remote] += (long long)ev.ids.size();
            dArrivedTotal += (long long)ev.ids.size();
            dWinArr += (long long)ev.ids.size();
            decInFlightPer[ev.remote] -= (int)ev.ids.size();
            upInFlight -= (int)ev.ids.size();
            for (int rid : ev.ids) readyDProc[ev.remote].insert({req[rid].readyTime, rid});
        } else for (int rid : ev.ids) readyDPost.insert({req[rid].readyTime, rid});
    }
}


static vector<string> lines;
static char tmp[256];


static void doPPre() {
    auto it = arrivedSet.begin(); int rid = it->second; arrivedSet.erase(it);
    int c = pickRemote(); req[rid].remote = c; remoteLoad[c]++; localFree = false;
    remoteWork[c] += interp(tblPProc, (double)req[rid].lin);
    pUpTransit++;
    gNPre++;
    snprintf(tmp, sizeof(tmp), "E P PRE %d %d", c, rid); lines.push_back(tmp);
}
static void doPPost() {
    auto it = readyPPost.begin(); int rid = it->second; readyPPost.erase(it); localFree = false;
    gNPost++;
    snprintf(tmp, sizeof(tmp), "E P POST %d %d", req[rid].remote, rid); lines.push_back(tmp);
}
static void doDPre() {
    localFree = false;
    vector<int> ids = takeAll(readyDecodeStart);
    decodeInFlight += (int)ids.size();
    upInFlight += (int)ids.size();
    gNDPre++; gNDPreTok += (long long)ids.size();
    for (int id : ids) decInFlightPer[req[id].remote]++;
    string s = "E D PRE -1 " + to_string(ids.size());
    for (int id : ids) { s += ' '; s += to_string(id); }
    lines.push_back(move(s));
}
static void doDPost() {
    localFree = false;
    vector<int> ids = takeAll(readyDPost);
    gNDPost++; gNDPostTok += (long long)ids.size();
    string s = "E D POST -1 " + to_string(ids.size());
    for (int id : ids) { s += ' '; s += to_string(id); }
    lines.push_back(move(s));
}
static void doPProc(int c) {
    auto it = readyPProc[c].begin(); int rid = it->second; readyPProc[c].erase(it);
    Req &r = req[rid];
    int ls = r.nextLs;
    int le = cfg.numLayers;
    r.nextLs = le;
    remoteFree[c] = 0;
    pDownTransit++;
    snprintf(tmp, sizeof(tmp), "C%d P PROC %d %d %d %d", c, ls, le, c, rid);
    lines.push_back(tmp);
}
static void doDProc(int c) {
    remoteFree[c] = 0;
    remoteDBusy[c] = 1;
    vector<int> ids = takeAll(readyDProc[c]);
    string s = "C" + to_string(c) + " D PROC " + to_string(c) + " " + to_string(ids.size());
    for (int id : ids) { s += ' '; s += to_string(id); }
    lines.push_back(move(s));
}


// NOTE: this is the ORIGINAL baseline scheduling policy (needFeed exclusion
// during the feed window, and pProcBias biasing toward P PROC when wC is
// high), restored after empirical evidence showed the previous "fix" was a
// net regression (14929.473 -> 13694.47 across this same test set,
// including a catastrophic collapse on test #19: norm_tp 0.874 -> 0.005,
// mean_tdr 411 -> 5,002,291). The only change kept from that attempt is the
// isolated remoteWork initialization above, which cannot cause this kind of
// regression since it only affects load-balancing across remotes, not
// task-type priority.
static void scheduleFrame(double now) {
    lines.clear();
    if ((int)readyDecodeStart.size() > dLastPool) dPoolStall = 0; else dPoolStall++;
    prefillDrained = arrivedSet.empty() && readyPPost.empty() && pUpTransit == 0 && pDownTransit == 0 &&
        all_of(readyPProc.begin(), readyPProc.end(), [](const auto &q) { return q.empty(); });
    if (now > 0.0) {
        if (now - dWinStart > 200.0) { dWinStart = now; dWinArr = 0; }
        dRate = (double)dWinArr / max(1.0, now - dWinStart);
    }
    if (localFree) {
        int type = -1; double best = -1e18;
        int readyDec = (int)readyDecodeStart.size() + (int)readyDPost.size();
        int prefillBacklog = (int)arrivedSet.size() + (int)readyPPost.size();
        bool backlogFeed = multiToken && cfg.wTp >= 0.5 && prefillBacklog > max(4, 2 * cfg.K);
        bool feedAlways = cfg.wC >= 0.6;
        bool pBacklog = !arrivedSet.empty() || !readyPPost.empty();
        double pForce = 0.0;
        if (cfg.wC >= 0.3 && ((!arrivedSet.empty() && now - arrivedSet.begin()->first > 4.0 * cfg.slo1) ||
            (!readyPPost.empty() && now - readyPPost.begin()->first > 4.0 * cfg.slo1)))
            pForce = 1e6;
        if (pForce > 0.0) gPF++;
        bool dBatchable = (int)readyDecodeStart.size() >= dBatchThresh && dBatchThresh > 0 && pBacklog;
        bool needFeed = ((feedAlways && (int)readyDecodeStart.size() < feedEscape) || (int)readyDecodeStart.size() < feedTarget || backlogFeed || dBatchable ||
                         (prefillBacklog > 32 && (int)readyDecodeStart.size() < 64)) && pBacklog;
        if (needFeed) gFeed++;
        if (needFeed) {
            if (!arrivedSet.empty()) { double u = urgency(now - arrivedSet.begin()->first, cfg.slo1, 1) + pForce; if (u > best) { best = u; type = 0; } }
            if (!readyPPost.empty()) { double u = urgency(now - readyPPost.begin()->first, cfg.slo1, 1) + pForce; if (u > best) { best = u; type = 1; } }
        } else {
            if (!arrivedSet.empty()) { double u = urgency(now - arrivedSet.begin()->first, cfg.slo1, 1) + pForce; if (u > best) { best = u; type = 0; } }
            if (!readyPPost.empty()) { double u = urgency(now - readyPPost.begin()->first, cfg.slo1, 1) + pForce; if (u > best) { best = u; type = 1; } }
            if (!readyDecodeStart.empty()) { double u = dUrgency(readyDecodeStart, now, cfg.slo2, (int)readyDecodeStart.size()); if (u > best) { best = u; type = 2; } }
            if (!readyDPost.empty()) { double u = urgency(now - readyDPost.begin()->first, cfg.slo2, (int)readyDPost.size()); if (u > best) { best = u; type = 3; } }
        }
        bool dAccum = holdThresh > 0 && cfg.wTp >= 0.5 && multiToken && dFixedEst >= dFixedT && dRate >= dAccumRate;
        bool dEsc = dAccum && decodeInFlight == 0 && prefillDrained;
        if (getenv("DBG") && (int)now % 100 < 2)
            fprintf(stderr, "DBG now=%.0f pool=%d acc=%d esc=%d est=%.1f hT=%d wTp=%.2f mt=%d preD=%d dIF=%d\n",
                now, (int)readyDecodeStart.size(), (int)dAccum, (int)dEsc, dFixedEst, holdThresh, cfg.wTp, multiToken, (int)prefillDrained, decodeInFlight);
        bool heldDPre = (type == 2 && (int)readyDecodeStart.size() < holdThresh && (dAccum ? !dEsc : decodeInFlight > 0));
        if (heldDPre) {
            gHeld++;
            type = -1; best = -1e18;
            if (!arrivedSet.empty()) { double u = urgency(now - arrivedSet.begin()->first, cfg.slo1, 1) + pForce; if (u > best) { best = u; type = 0; } }
            if (!readyPPost.empty()) { double u = urgency(now - readyPPost.begin()->first, cfg.slo1, 1) + pForce; if (u > best) { best = u; type = 1; } }
            if (!readyDPost.empty()) { double u = urgency(now - readyDPost.begin()->first, cfg.slo2, (int)readyDPost.size()); if (u > best) { best = u; type = 3; } }
        }
        if (type == 0) doPPre();
        else if (type == 1) doPPost();
        else if (type == 2) {
            bool esc = inFlightCap > 0 ? (decodeInFlight < inFlightCap) : (decodeInFlight == 0);
            if ((int)readyDecodeStart.size() >= holdThresh || (dAccum ? dEsc : esc)) doDPre();
        }
        else if (type == 3) {
            int ppool = (int)readyDPost.size();
            bool anyDBusy = false;
            for (int i = 0; i < cfg.K; i++) if (remoteDBusy[i]) { anyDBusy = true; break; }
            bool dPostAlive = anyDBusy || upInFlight > 0 || !localFree;
            bool esc3 = dPostHold <= 0 || holdThresh <= 0 || ppool >= dPostHold || !dPostAlive;
            if (esc3) doDPost();
            else {
                type = -1; best = -1e18;
                if (!arrivedSet.empty()) { double u = urgency(now - arrivedSet.begin()->first, cfg.slo1, 1); if (u > best) { best = u; type = 0; } }
                if (!readyPPost.empty()) { double u = urgency(now - readyPPost.begin()->first, cfg.slo1, 1); if (u > best) { best = u; type = 1; } }
                if (!readyDecodeStart.empty()) { double u = dUrgency(readyDecodeStart, now, cfg.slo2, (int)readyDecodeStart.size()); if (u > best) { best = u; type = 2; } }
                if (type == 0) doPPre();
                else if (type == 1) doPPost();
else if (type == 2) {
                    bool esc = inFlightCap > 0 ? (decodeInFlight < inFlightCap) : (decodeInFlight == 0);
                    if ((int)readyDecodeStart.size() >= holdThresh || (dAccum ? dEsc : esc)) doDPre();
                }
            }
        }
        dLastPool = (int)readyDecodeStart.size();
    }
    for (int c = 0; c < cfg.K; c++) {
        if (!remoteFree[c]) continue;
        int type = -1; double best = -1e18;
        if (!readyPProc[c].empty()) {
            double pWait = now - readyPProc[c].begin()->first;
            double dPool = (double)readyDProc[c].size();
            double effBias = pProcBias / (1.0 + 0.3 * dPool); // back off when there's real decode backlog to protect
            if (pWait > 8.0 * cfg.slo1) { effBias += 1e6; gP8++; } // prefill starved -> absolute priority
            double u = urgency(now - readyPProc[c].begin()->first, cfg.slo1, 1) + effBias;
            if (u > best) { best = u; type = 0; }
        }
        if (!readyDProc[c].empty()) { double u = urgency(now - readyDProc[c].begin()->first, cfg.slo2, (int)readyDProc[c].size()); if (u > best) { best = u; type = 1; } }
        if (type == 0) { doPProc(c); gPProc++; }
        else if (type == 1) {
            int pool = (int)readyDProc[c].size();
            double rate = (now > 1.0) ? (double)dArrived[c] / now : 0.0;
            bool active2 = dProcHold > 0 && holdThresh > 0 && cfg.wTp >= 0.4 && multiToken && rate >= dMinRate;
            bool supplyAlive = !arrivedSet.empty() || !readyPPost.empty() || pUpTransit > 0 || pDownTransit > 0 ||
                upInFlight > 0 || (int)readyDecodeStart.size() >= holdThresh ||
                any_of(readyPProc.begin(), readyPProc.end(), [](const auto &q) { return !q.empty(); });
            bool esc2 = !active2 || pool >= dProcHold || !supplyAlive;
            if (esc2) doDProc(c);
            else if (!readyPProc[c].empty()) doPProc(c);
        }
    }
}


static vector<pair<int,int>> arrEvs;
static vector<TdnEv> tdnEvs;
static vector<XdnEv> xdnEvs;
static vector<int> finEvs;


int main() {
    cfg.K = (int)readInt(); cfg.S = readDouble(); cfg.latency = readDouble();
    cfg.bandwidth = readDouble(); cfg.bytesPerToken = (int)readInt(); cfg.numLayers = (int)readInt();
    cfg.slo1 = readDouble(); cfg.slo2 = readDouble(); cfg.tpUB = readDouble();
    cfg.tpBase = readDouble(); cfg.distBase = readDouble(); cfg.wTp = readDouble(); cfg.wC = readDouble();


    chunkTargetFactor = 0.5 - 0.35 * cfg.wC;
    feedTarget = (int)(2.0 * cfg.K * (0.25 + 0.75 * cfg.wTp));


    remoteFree.assign(cfg.K, 1);
    remoteLoad.assign(cfg.K, 0);
    remoteWork.assign(cfg.K, 0.0);
    remoteDBusy.assign(cfg.K, 0);
    readyPProc.resize(cfg.K);
    readyDProc.resize(cfg.K);
    req.reserve(2001);


    int N = (int)readInt();
    tblPProc.reserve(N);
    for (int i = 0; i < N; i++) {
        int bs = (int)readInt();
        double pre = readDouble(), proc = readDouble(), post = readDouble();
        double dpre = readDouble(), dproc = readDouble(), dpost = readDouble();
        if (proc >= 0) tblPProc.push_back({bs, proc});
        if (pre >= 0) tblPPre.push_back({bs, pre});
        if (post >= 0) tblPPost.push_back({bs, post});
    }
    sort(tblPProc.begin(), tblPProc.end());
    sort(tblPPre.begin(), tblPPre.end());
    sort(tblPPost.begin(), tblPPost.end());
    double feedBonus = 0.0;
    if (cfg.wTp <= 0.1 && !tblPPre.empty() && !tblPPost.empty()) {
        double eDur = interp(tblPPre, 1024.0) + interp(tblPPost, 1024.0);
        if (eDur > 4.0 * cfg.S) feedBonus = 2.0;
    }
    feedTarget = (int)(2.0 * cfg.K * (0.25 + 0.75 * cfg.wTp) + feedBonus);
    pProcBias = (cfg.wC >= 0.6) ? 5.0 : 0.0;
    newestFirst = (cfg.wTp >= 0.8) ? 1 : 0;
    dBatchThresh = (cfg.wTp >= 0.9 && cfg.wC <= 0.1) ? 16 : 0;
    holdThresh = ((cfg.wC <= 0.2) || (cfg.wC >= 0.6)) && (cfg.wC < 1.0) ? 48 : 0;
    inFlightCap = 2;
    if (getenv("E_CAP")) inFlightCap = atoi(getenv("E_CAP"));
    feedEscape = (cfg.wC >= 0.6) ? 48 : 0;
    if (getenv("E46_FEEDESCAPE")) feedEscape = atoi(getenv("E46_FEEDESCAPE"));
    dHold = 0;
    if (getenv("E48_DHOLD")) dHold = atoi(getenv("E48_DHOLD"));
    if (getenv("E48_RATE")) dMinRate = atof(getenv("E48_RATE"));
    if (getenv("E_THRESH")) holdThresh = atoi(getenv("E_THRESH"));
    if (getenv("E_FT")) feedTarget = atoi(getenv("E_FT"));
    if (getenv("D_HOLD")) dProcHold = atoi(getenv("D_HOLD"));
    if (getenv("D_POST")) dPostHold = atoi(getenv("D_POST"));
    if (getenv("FIXED_T")) dFixedT = atof(getenv("FIXED_T"));
    if (getenv("ACCUM_RATE")) dAccumRate = atof(getenv("ACCUM_RATE"));
    dArrived.assign(cfg.K, 0);
    decInFlightPer.assign(cfg.K, 0);


    while (true) {
        int tl = readTok();
        if (tl < 0) break;
        if (!strcmp(tokbuf, "END")) break;
        double now = atof(tokbuf);
        int e = (int)readInt();


        arrEvs.clear(); tdnEvs.clear(); xdnEvs.clear(); finEvs.clear();


        for (int i = 0; i < e; i++) {
            readTok();
            if (!strcmp(tokbuf, "ARR")) {
                int rid = (int)readInt(); int lin = (int)readInt();
                arrEvs.push_back({rid, lin});
            } else if (!strcmp(tokbuf, "TDN")) {
                readTok(); int srv = -1; if (tokbuf[0] == 'C') srv = atoi(tokbuf + 1);
                readTok(); int stage = (tokbuf[0] == 'P') ? 0 : 1;
                readTok(); int step = (!strcmp(tokbuf, "PRE")) ? 0 : (!strcmp(tokbuf, "PROC")) ? 1 : 2;
                TdnEv ev; ev.localOrRemote = srv; ev.stage = stage; ev.step = step;
                if (stage == 0) {
                    if (step == 0) { int remote=(int)readInt(); int rid=(int)readInt(); ev.remote=remote; ev.ids={rid}; }
                    else if (step == 1) { int ls=(int)readInt(); int le=(int)readInt(); int remote=(int)readInt(); int rid=(int)readInt(); ev.ls=ls; ev.le=le; ev.remote=remote; ev.ids={rid}; }
                    else { int remote=(int)readInt(); int rid=(int)readInt(); ev.remote=remote; ev.ids={rid}; }
                } else {
                    if (step == 0) { readInt(); int m=(int)readInt(); ev.ids.resize(m); for (int k=0;k<m;k++) ev.ids[k]=(int)readInt(); }
                    else if (step == 1) { int remote=(int)readInt(); int m=(int)readInt(); ev.remote=remote; ev.ids.resize(m); for (int k=0;k<m;k++) ev.ids[k]=(int)readInt(); }
                    else { readInt(); int m=(int)readInt(); ev.ids.resize(m); for (int k=0;k<m;k++) ev.ids[k]=(int)readInt(); }
                }
                ev.dur = readDouble();
                tdnEvs.push_back(move(ev));
            } else if (!strcmp(tokbuf, "XDN")) {
                readTok(); int dir = (!strcmp(tokbuf, "UP")) ? 0 : 1;
                int remote = (int)readInt(); readInt();
                readTok(); int kind = (!strcmp(tokbuf, "PRE")) ? 0 : 1;
                int m = (int)readInt();
                XdnEv ev; ev.dir=dir; ev.remote=remote; ev.kind=kind; ev.ids.resize(m);
                for (int k=0;k<m;k++) ev.ids[k]=(int)readInt();
                xdnEvs.push_back(move(ev));
            } else if (!strcmp(tokbuf, "FIN")) {
                int rid = (int)readInt(); finEvs.push_back(rid);
            }
        }


        for (auto &a : arrEvs) onArr(a.first, a.second, now);
        for (auto &t : tdnEvs) onTdn(t, now, finEvs);
        for (auto &x : xdnEvs) onXdn(x, now);
        for (int rid : finEvs) { req[rid].finished = true; if (req[rid].remote >= 0) remoteLoad[req[rid].remote]--; }


        scheduleFrame(now);


        wint((long long)lines.size()); wc('\n');
        for (auto &s : lines) { wstr(s.c_str()); wc('\n'); }
        flushOut();
    }
    flushOut();
    if (getenv("DBG2")) fprintf(stderr, "STATS nPre=%lld nPost=%lld nDPre=%lld(%lld tok) nDPost=%lld(%lld tok) held=%lld feed=%lld rate=%.3f p8=%lld pProc=%lld pf=%lld\n",
        gNPre, gNPost, gNDPre, gNDPreTok, gNDPost, gNDPostTok, gHeld, gFeed, dRate, gP8, gPProc, gPF);
    return 0;
}

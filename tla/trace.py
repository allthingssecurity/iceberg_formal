#!/usr/bin/env python3
"""usage: trace.py <area> <cfg> [var ...]  prints the counterexample as one line per state: action + changed vars (or selected vars)"""
import sys, re
area, cfg = sys.argv[1], sys.argv[2]; sel = sys.argv[3:]
txt = open(f"{area}/out/{cfg}.log").read()
states = re.split(r'\n(?=State \d+: )', txt)
prev = {}
for s in states:
    m = re.match(r'State (\d+): (<[^>]*>|.*)', s)
    if not m: continue
    act = re.sub(r' line.*', '', m.group(2)).strip('<> ')
    vs = {}
    for vm in re.finditer(r'^/\\ (\w+) = (.*?)(?=\n/\\ |\Z)', s, re.S | re.M):
        vs[vm.group(1)] = ' '.join(vm.group(2).split())
    show = {k: v for k, v in vs.items() if (k in sel if sel else prev.get(k) != v)}
    print(f"{m.group(1):>3} {act:<22} " + '; '.join(f"{k}={v}" for k, v in show.items()))
    prev = vs

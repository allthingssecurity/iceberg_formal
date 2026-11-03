#!/usr/bin/env python3
"""usage: gencfg.py <area>   reads <area>/configs.py (dict C: name -> (consts, invariants, properties)) and writes <name>.cfg + cfglist.txt"""
import sys, os
area = sys.argv[1]
ns = {}
exec(open(os.path.join(area, 'configs.py')).read(), ns)
C, BASE = ns['C'], ns['BASE']
for name, (over, invs, props) in C.items():
    c = dict(BASE); c.update(over)
    with open(os.path.join(area, name + '.cfg'), 'w') as f:
        f.write('SPECIFICATION Spec\nCONSTANTS\n')
        for k, v in c.items():
            f.write(f'    {k} = {v}\n')
        f.write('CONSTRAINT CtlBound\n')
        for i in invs:
            f.write(f'INVARIANT {i}\n')
        for p in props:
            f.write(f'PROPERTY {p}\n')
with open(os.path.join(area, 'cfglist.txt'), 'w') as f:
    f.write('\n'.join(C.keys()) + '\n')

"""Clean, reproducible Lean builds and design controls. Run from repository root."""
from pathlib import Path
import json
import os
import re
import subprocess
import tempfile

BASE=Path('formal/concord_bounded_maintenance')
MODULES={
    'Bounds':BASE/'Bounds.lean',
    'Work':BASE/'program/work/Work.lean',
    'Updates':BASE/'program/updates/Updates.lean',
    'Uncertainty':BASE/'program/uncertainty/Uncertainty.lean',
    'Progress':BASE/'program/progress/Progress.lean',
    'Recovery':BASE/'program/recovery/Recovery.lean',
    'Cleanup':BASE/'program/cleanup/Cleanup.lean',
    'Pairs':BASE/'program/pairs/Pairs.lean',
    'PairLevel':BASE/'program/pairlevel/PairLevel.lean',
    'Extract':BASE/'program/extract/Extract.lean',
    'Repair':BASE/'program/repair/Repair.lean',
}

def command(args,env):
    result=subprocess.run(args,text=True,capture_output=True,env=env)
    if result.returncode:
        raise RuntimeError(f'{args}: exit {result.returncode}\n{result.stdout}\n{result.stderr}')
    return result.stdout

def run():
    version=command(['lean','--version'],os.environ)
    if 'version 4.34.1,' not in version:
        raise RuntimeError('Expected pinned Lean4.34.1; '+version)
    with tempfile.TemporaryDirectory(prefix='concord-design-proofs-') as directory:
        env=dict(os.environ,LEAN_PATH=directory)
        builds={}
        for name,path in MODULES.items():
            text=command(['lean','-o',str(Path(directory)/(name+'.olean')),str(path)],env)
            builds[name]=dict(status='pass',stdout=text)
        audits=[]
        count=0
        for name,path in MODULES.items():
            source=path.read_text()
            if re.search(r'^\s*(axiom|opaque)\s',source,re.M) or re.search(r'\bsorry\b',source):
                raise RuntimeError(f'Placeholder or custom axiom in {path}')
            namespace=re.search(r'^namespace (\w+)',source,re.M).group(1)
            for theorem in re.findall(r'^theorem (\w+)',source,re.M):
                audits.append(f'#print axioms {namespace}.{theorem}')
                count+=1
        audit=Path(directory)/'Audit.lean'
        audit.write_text('\n'.join('import '+name for name in MODULES)+'\n'+'\n'.join(audits)+'\n')
        axiom_text=command(['lean',str(audit)],env)
        for axiom_list in re.findall(r'depends on axioms: \[(.*?)\]',axiom_text):
            if set(axiom_list.split(', '))-{'propext','Quot.sound'}:
                raise RuntimeError('Unexpected axioms: '+axiom_list)
        controls=json.loads(command(['python3',str(BASE/'program/check_program.py')],env))
        return dict(status='pass',lean_version=version.strip(),builds=builds,
                    audited_theorems=count,axiom_audit=axiom_text,controls=controls)

if __name__=='__main__':
    print(json.dumps(run(),indent=2))

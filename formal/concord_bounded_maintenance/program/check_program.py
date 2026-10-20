"""Executable design kernels and failing variants, not production tests."""
from dataclasses import dataclass, replace
from itertools import product
import json

class SparseGraph:
    def __init__(self, groups=None, parent=None):
        self.groups = groups or {}
        self.parent = parent
    def get(self, key):
        if key in self.groups:
            return self.groups[key]
        return self.parent.get(key) if self.parent else ()

def finding(values):
    return sum(values) if values else None

def move(graph, source, target, value):
    if source == target or value not in graph.get(source):
        return graph
    old = list(graph.get(source)); old.remove(value)
    return SparseGraph({source: tuple(old), target: (value,) + graph.get(target)}, graph)

def delete(graph, group, value):
    old = list(graph.get(group))
    if value in old:
        old.remove(value)
    return SparseGraph({group: tuple(old)}, graph)

def refresh(graph, touched):
    # Sparse output delta; no full-result copying, sorting or graph scan.
    return {key: finding(graph.get(key)) for key in touched}

def primitive_cost(graph, touched, trace, edits, roots, retired, retries):
    # All parameters are primitive-operation logs, not hidden high-level work.
    return sum(len(graph.get(key)) + 2 for key in touched) + len(trace) + len(edits) + len(roots) + len(retired) + len(retries) + 1

def work_checks():
    graph = SparseGraph({0:(2,3), 1:(7,)})
    padded = SparseGraph({key:(42,) for key in range(2,102)}, graph)
    logs = ([0], [0,1], [0], [9,10], [3], [4])
    assert refresh(graph,[0]) == refresh(padded,[0])
    base=primitive_cost(graph,*logs)
    assert base == primitive_cost(padded,*logs)
    # A global scan is a broken variant: it introduces unrelated work.
    def scan_mutant(graph, all_keys):
        charged = base
        for key in all_keys:
            graph.get(key)  # unnecessary whole-graph read
            charged += 1
        return charged
    bad_base = scan_mutant(graph, range(2))
    bad_padded = scan_mutant(padded, range(102))
    assert bad_base != bad_padded
    # Removing retry work gives a false no-change zero-work claim.
    empty=SparseGraph()
    assert primitive_cost(empty,[],[],[],[],[],[1]) > primitive_cost(empty,[],[],[],[],[],[])
    return dict(comparisons=2, work=base, padding=100, negatives=['global_scan','uncharged_retries'],
                scope='primitive indexed-operation cost; logs must enumerate complete operations')

def update_checks():
    choices=[()] + [(v,) for v in range(2)] + list(product(range(2), repeat=2))
    cases=0
    for groups in product(choices,repeat=3):
        graph=SparseGraph(dict(enumerate(groups)))
        old={key:finding(graph.get(key)) for key in range(3)}
        for source,target,value in product(range(3),range(3),range(2)):
            updated=move(graph,source,target,value)
            delta=refresh(updated,[source,target])
            result=old | delta
            assert result == {key:finding(updated.get(key)) for key in range(3)}
            assert sum(map(sum,groups)) == sum(sum(updated.get(key)) for key in range(3))
            assert sum(map(len,groups)) == sum(len(updated.get(key)) for key in range(3))
            cases+=1
        for group,value in product(range(3),range(2)):
            updated=delete(graph,group,value)
            assert old | refresh(updated,[group]) == {key:finding(updated.get(key)) for key in range(3)}
            cases+=1
    graph=SparseGraph({0:(5,),1:()}); updated=move(graph,0,1,5)
    broken={0:5,1:None} | refresh(updated,[1])
    assert broken != {0:None,1:5}
    assert refresh(delete(graph,0,5),[0]) == {0:None}
    return dict(comparisons=cases,negative='destination_only_refresh_leaves_stale_source')

def deps(expr):
    kind,*args=expr
    if kind=='answer':return [args[0]]
    if kind=='literal':return []
    return deps(args[0])+deps(args[1])
def evaluate(expr,answers):
    kind,*args=expr
    if kind=='answer':return answers.get(args[0],0)
    if kind=='literal':return args[0]
    return evaluate(args[0],answers)+evaluate(args[1],answers)
def marked(expr,answers):return any(key not in answers for key in deps(expr))
def expressions():
    leaves=[('literal',0),('literal',1)]+[('answer',k) for k in range(3)]
    one=leaves+[('add',a,b) for a,b in product(leaves,repeat=2)]
    return one+[('add',e,('add',('answer',0),('literal',2))) for e in one]

def uncertainty_checks():
    cases=0
    for expr in expressions():
        for states in product([None,0,1],repeat=3):
            answers={key:value for key,value in enumerate(states) if value is not None}
            unknown=[key for key in range(3) if key not in answers]
            if not marked(expr,answers):
                for supplied in product(range(2),repeat=len(unknown)):
                    complete=answers | dict(zip(unknown,supplied))
                    assert evaluate(expr,answers)==evaluate(expr,complete)
                    cases+=1
            # unrelated missing key never marks an independent literal
            assert not marked(('literal',2),answers)
    expr=('add',('literal',1),('answer',2))
    # Dropping right-hand uncertainty presents a wrong result as exact.
    broken_mark=marked(expr[1],{})
    assert not broken_mark and evaluate(expr,{})!=evaluate(expr,{2:3})
    return dict(comparisons=cases,negative='drop_reader_mark',scope='addition expression trees; no key-deciding calls')

def progress_checks():
    checks=0
    for prefix in range(1,6):
        for capacity in range(1,4):
            # Concrete queue prefix ends with target commit after input reads.
            queue=[('work',i) for i in range(prefix-1)]+[('commit',0,0,5)]
            emitted=[]
            rounds=(prefix+capacity-1)//capacity
            for tick in range(rounds):
                queue += [('commit',tick+1,1,tick)] # changes to another group
                emitted += queue[:capacity]; queue=queue[capacity:]
            assert ('commit',0,0,5) in emitted
            checks+=1
    # Resetting target on each unrelated publication prevents committing it.
    completed=False
    for tick in range(6):
        queue=[('work',tick),('commit',tick,0,5)]
        completed=completed or any(e[0]=='commit' for e in queue[:1])
    assert not completed
    return dict(comparisons=checks,negative='reset_pinned_target')

@dataclass(frozen=True)
class Journal:
    cp: tuple=(0,0,0)
    cpvalue:int=0
    target:tuple=(1,2,3)
    total:int=3
    completed:int=0
    effects:tuple=()
    running:bool=True

def effect(identity,ordinal):return sum(identity)+ordinal

def valid(state):return state.completed<=state.total and state.effects==tuple((i,effect(state.target,i)) for i in range(state.completed))
def transition(state,action):
    if action=='crash':return replace(state,running=False)
    if action=='resume':return replace(state,running=True)
    if action=='step' and state.running and state.completed<state.total:
        return replace(state,completed=state.completed+1,effects=state.effects+((state.completed,effect(state.target,state.completed)),))
    if action=='finish' and state.completed==state.total:
        return replace(state,cp=state.target,cpvalue=sum(value for _,value in state.effects))
    return state

def recovery_checks():
    seen={Journal()};todo=list(seen);checks=0
    while todo:
        state=todo.pop()
        for action in ['step','crash','resume','finish']:
            new=transition(state,action)
            assert valid(new)
            assert len({key for key,_ in new.effects})==len(new.effects)
            assert new.completed>=state.completed
            if action!='finish':assert (new.cp,new.cpvalue)==(state.cp,state.cpvalue)
            if new.cp==new.target:assert new.cpvalue==sum(effect(new.target,i) for i in range(new.total))
            checks+=1
            if new not in seen:seen.add(new);todo.append(new)
    one=transition(Journal(),'step')
    # Crash losing progress but retaining effect duplicates unit zero on retry.
    broken=replace(one,completed=0)
    duplicate=transition(broken,'step')
    assert not valid(duplicate) and len(duplicate.effects)!=len({key for key,_ in duplicate.effects})
    # A new answer identity mid-run changes future effects and invalidates ledger.
    assert not valid(replace(one,target=(1,2,99)))
    return dict(states=len(seen),transitions=checks,negatives=['lost_progress_duplicates_effect','changed_answer_snapshot'])

def cleanup(checkpoint,unfinished,answers):
    keys=set(key for expr in checkpoint+unfinished for key in deps(expr))
    return {key:answers[key] for key in keys if key in answers}

def cleanup_checks():
    checks=0
    expressions_=expressions()
    for checkpoint in expressions_:
        for unfinished in expressions_:
            for state in [{0:4,1:6,2:8,99:100},{0:4},{}, {2:8}]:
                after=cleanup([checkpoint],[unfinished],state)
                refs=set(deps(checkpoint)+deps(unfinished))
                assert len(after)<=len(refs)<=len(deps(checkpoint))+len(deps(unfinished))
                for expr in [checkpoint,unfinished]:
                    assert evaluate(expr,state)==evaluate(expr,after)
                    assert marked(expr,state)==marked(expr,after)
                checks+=1
    live=('literal',0);pending=('answer',2);answers={2:7}
    broken=cleanup([live],[],answers)
    assert evaluate(pending,broken)!=evaluate(pending,answers)
    return dict(comparisons=checks,negative='cleanup_ignores_unfinished_input')

# Pair-rule UPDATE (Algorithm 2, revised); mirrors program/pairs/Pairs.lean.
# A claim is (id, bucket, value); bucket None means Unknown. Answers map a
# request to a value; a missing key is Missing.
PAIR_BUCKETS=(0,1)

def pair_request(left,right):return (min(left[0],right[0]),max(left[0],right[0]))

def judged_test(left,right,answers):
    answer=answers.get(pair_request(left,right))
    if answer is None:return 'pending'
    if answer==1 and left[2]!=right[2]:return ('violation',1)
    return 'no_violation'

def peeking_test(left,right,answers):
    # Broken: also reads the answer to (0, 1), which it does not declare.
    if answers.get((0,1))==1:return 'failed'
    return judged_test(left,right,answers)

def bucket_out(claims,answers,bucket,test=judged_test):
    members=sorted((c for c in claims if c[1]==bucket),key=lambda c:c[0])
    rows=[];requests=[]
    for i,left in enumerate(members):
        for right in members[i+1:]:
            if left[0]<right[0]:
                rows.append((left[0],right[0],test(left,right,answers)))
                requests.append(pair_request(left,right))
    return (tuple(rows),tuple(requests))

def full_check(claims,answers,test=judged_test):
    return {b:bucket_out(claims,answers,b,test) for b in PAIR_BUCKETS}

def request_index(old):
    # Built from the stored old requests, not from the current claims.
    index={}
    for bucket,(_,requests) in old.items():
        for request in requests:index.setdefault(request,set()).add(bucket)
    return index

def affected_buckets(removed,added,changed,index,use_removed=True,use_answers=True):
    chosen={c[1] for c in added if c[1] is not None}
    if use_removed:chosen|={c[1] for c in removed if c[1] is not None}
    if use_answers:
        for request in changed:chosen|=index.get(request,set())
    return chosen

def pair_update(old,claims,answers,chosen,test=judged_test):
    return {b:(bucket_out(claims,answers,b,test) if b in chosen else old[b]) for b in PAIR_BUCKETS}

def pair_changes(claims):
    present={c[0]:c for c in claims}
    for cid in range(4):
        if cid in present:
            old=present[cid]
            yield 'remove',{old},set()
            for bucket in (None,)+PAIR_BUCKETS:
                if bucket!=old[1]:yield 'move',{old},{(cid,bucket,old[2])}
        else:
            for bucket in (None,)+PAIR_BUCKETS:
                for value in range(2):yield 'add',set(),{(cid,bucket,value)}

def pairs_checks():
    requests=[(a,b) for a in range(4) for b in range(a+1,4)]
    mixed=dict(zip(requests,[1,0,None,1,None,0]))
    answer_sets=[{},{q:0 for q in requests},{q:1 for q in requests},
                 {q:v for q,v in mixed.items() if v is not None}]
    slots=[None]+[(bucket,value) for bucket in (None,)+PAIR_BUCKETS for value in range(2)]
    cases=dict(claim_changes=0,answer_changes=0)
    caught=dict(ignore_answers=0,new_bucket_only=0,incomplete_index=0,undeclared_read=0)
    for choice in product(slots,repeat=4):
        c0=frozenset((cid,)+slot for cid,slot in enumerate(choice) if slot is not None)
        for a0 in answer_sets:
            old=full_check(c0,a0);index=request_index(old)
            bad_old=full_check(c0,a0,peeking_test);bad_index=request_index(bad_old)
            for kind,removed,added in pair_changes(c0):
                c1=(c0-removed)|added
                want=full_check(c1,a0)
                chosen=affected_buckets(removed,added,[],index)
                assert pair_update(old,c1,a0,chosen)==want
                assert None not in chosen  # Unknown claims never add a bucket.
                if kind=='move':
                    narrow=affected_buckets(removed,added,[],index,use_removed=False)
                    if pair_update(old,c1,a0,narrow)!=want:caught['new_bucket_only']+=1
                cases['claim_changes']+=1
            for request in requests:
                if request in a0:continue
                for value in range(2):
                    a1=dict(a0);a1[request]=value
                    want=full_check(c0,a1)
                    assert pair_update(old,c0,a1,affected_buckets(set(),set(),[request],index))==want
                    blind=affected_buckets(set(),set(),[request],index,use_answers=False)
                    if pair_update(old,c0,a1,blind)!=want:caught['ignore_answers']+=1
                    partial={q:bs-{1} for q,bs in index.items()}  # index misses bucket 1
                    if pair_update(old,c0,a1,affected_buckets(set(),set(),[request],partial))!=want:
                        caught['incomplete_index']+=1
                    bad_chosen=affected_buckets(set(),set(),[request],bad_index)
                    if pair_update(bad_old,c0,a1,bad_chosen,peeking_test)!=full_check(c0,a1,peeking_test):
                        caught['undeclared_read']+=1
                    cases['answer_changes']+=1
    assert all(count>0 for count in caught.values()),caught
    return dict(comparisons=cases['claim_changes']+cases['answer_changes'],**cases,
                negatives_caught=caught,
                scope='claim ids 0-3, buckets Unknown/0/1, values 0-1, judged rule; one claim or one answer change',
                chains=pair_chain_checks())

# Stored-state UPDATE (Pairs.lean `step`): claims_by_bucket, unplaced, outputs
# and buckets_by_request are all carried from run to run.
def scratch_state(claims,answers):
    outputs=full_check(claims,answers)
    return dict(claims={b:frozenset(c for c in claims if c[1]==b) for b in PAIR_BUCKETS},
                unplaced=frozenset(c[0] for c in claims if c[1] is None),
                outputs=outputs,index={q:frozenset(bs) for q,bs in request_index(outputs).items()})

def stored_step(state,removed,added,arrived,answers,variant=None):
    # Lines 3-15.
    claims={b:set(ms) for b,ms in state['claims'].items()}
    unplaced=set(state['unplaced']);affected=set()
    for c in removed:
        if c[1] is None:unplaced.discard(c[0])
        else:claims[c[1]].discard(c);affected.add(c[1])
    for c in added:
        if c[1] is None:unplaced.add(c[0])
        else:
            claims[c[1]].add(c)
            if variant!='omit_added_buckets':affected.add(c[1])
    # Lines 16-17: arrived requests through the old index.
    for request in arrived:affected|=state['index'].get(request,frozenset())
    # Lines 20-42.
    outputs=dict(state['outputs']);index={q:set(bs) for q,bs in state['index'].items()}
    for bucket in affected:
        old=outputs.pop(bucket)
        if variant!='keep_old_index_entries':
            for request in old[1]:index.get(request,set()).discard(bucket)
        result=bucket_out(claims[bucket],answers,bucket)
        outputs[bucket]=result
        if variant!='skip_new_index_entries':
            for request in result[1]:index.setdefault(request,set()).add(bucket)
    return dict(claims={b:frozenset(ms) for b,ms in claims.items()},unplaced=frozenset(unplaced),
                outputs=outputs,index={q:frozenset(bs) for q,bs in index.items() if bs}), len(affected)

def chain_steps(claims,answers,requests,combined):
    # One claim change, one arrived answer, or (combined) both at once.
    claim_moves=[(removed,added) for _,removed,added in pair_changes(claims)
                 if all(c[0]<3 for c in added|removed) or combined]
    arrivals=[(request,value) for request in requests if request not in answers for value in range(2)]
    steps=[(removed,added,None) for removed,added in claim_moves]+[(set(),set(),a) for a in arrivals]
    if combined:steps+=[(removed,added,a) for removed,added in claim_moves for a in arrivals]
    return steps

def apply_step(claims,answers,step):
    removed,added,arrival=step
    new_answers=dict(answers)
    if arrival is not None:new_answers[arrival[0]]=arrival[1]
    return (claims-removed)|added,new_answers,[arrival[0]] if arrival else []

CHAIN_VARIANTS=('skip_new_index_entries','keep_old_index_entries','omit_added_buckets')

def run_chain(c0,a0,steps,tally,cache):
    states={None:scratch_state(c0,a0)} | {v:scratch_state(c0,a0) for v in CHAIN_VARIANTS}
    claims,answers=c0,a0
    for step in steps:
        removed,added,_=step
        claims,new_answers,arrived=apply_step(claims,answers,step)
        key=(claims,tuple(sorted(new_answers.items())))
        if key not in cache:cache[key]=scratch_state(claims,new_answers)
        want=cache[key]
        for variant,state in states.items():
            state,work=stored_step(state,removed,added,arrived,new_answers,variant)
            states[variant]=state
            outputs_ok=state['outputs']==want['outputs']
            index_ok=state['index']==want['index']
            if variant is None:
                assert state==want,(c0,a0,steps)
                tally['states']+=1
            else:
                tally[variant]['stale_outputs']+=not outputs_ok
                tally[variant]['index_differs']+=not index_ok
                # Stale index entries: listed buckets whose output no longer reads q.
                unsound=any(q not in state['outputs'][b][1] for q,bs in state['index'].items() for b in bs)
                tally[variant]['index_unsound']+=unsound
        answers=new_answers

def pair_chain_checks():
    import random
    requests=[(a,b) for a in range(4) for b in range(a+1,4)]
    small=[q for q in requests if q[1]<3]
    tally=dict(states=0,chains_two=0,chains_three=0)
    for v in CHAIN_VARIANTS:tally[v]=dict(stale_outputs=0,index_differs=0,index_unsound=0)
    cache={}
    slots=[None]+[(bucket,value) for bucket in (None,)+PAIR_BUCKETS for value in range(2)]
    starts=[{},{small[0]:1},{q:1 for q in small},{small[0]:0,small[2]:1}]
    # Exhaustive: claim ids 0-2, every two-step chain of single changes.
    for choice in product(slots,repeat=3):
        c0=frozenset((cid,)+slot for cid,slot in enumerate(choice) if slot is not None)
        for a0 in starts:
            for first in chain_steps(c0,a0,small,False):
                c1,a1,_=apply_step(c0,a0,first)
                for second in chain_steps(c1,a1,small,False):
                    run_chain(c0,a0,[first,second],tally,cache)
                    tally['chains_two']+=1
    # Sampled: claim ids 0-3, three-step chains whose steps may combine a claim
    # change with an arrived answer.
    rng=random.Random(0)
    for _ in range(20000):
        choice=[rng.choice(slots) for _ in range(4)]
        c0=frozenset((cid,)+slot for cid,slot in enumerate(choice) if slot is not None)
        a0={q:v for q,v in zip(requests,[rng.choice([None,0,1]) for _ in requests]) if v is not None}
        claims,answers,steps=c0,a0,[]
        for _ in range(3):
            options=chain_steps(claims,answers,requests,True)
            if not options:break
            step=rng.choice(options);steps.append(step)
            claims,answers,_=apply_step(claims,answers,step)
        run_chain(c0,a0,steps,tally,cache)
        tally['chains_three']+=1
    # Forgetting new index entries leaves a later arrival unseen; omitting the
    # added buckets misses new pairs. Keeping old entries only adds re-checks:
    # outputs stay exact but the index is no longer sound.
    assert tally['skip_new_index_entries']['stale_outputs']>0
    assert tally['omit_added_buckets']['stale_outputs']>0
    assert tally['keep_old_index_entries']['stale_outputs']==0
    assert tally['keep_old_index_entries']['index_unsound']>0
    scope=('two-step chains exhaustive over claim ids 0-2; 20000 sampled three-step chains over ids 0-3'+
           ' with combined claim+answer steps; every intermediate stored state compared with a'+
           ' from-scratch build (members, unplaced, outputs, index)')
    return dict(tally,scope=scope)

# Pair-level UPDATE (program/pairlevel/PairLevel.lean): one stored row per
# claim-ID pair of a known bucket, keyed (left id, right id), and an index from
# a request to the pairs whose test read it. UPDATE re-checks affected pairs.
def pl_key(y,z):return (min(y[0],z[0]),max(y[0],z[0]))

def pl_row(y,z,answers):
    left,right=(y,z) if y[0]<z[0] else (z,y)
    return (left,right,judged_test(left,right,answers),(pair_request(left,right),))

def pl_scratch(claims,answers):
    rows={}
    for bucket in PAIR_BUCKETS:
        members=sorted((c for c in claims if c[1]==bucket),key=lambda c:c[0])
        for i,left in enumerate(members):
            for right in members[i+1:]:
                rows[pl_key(left,right)]=pl_row(left,right,answers)
    index={}
    for key,row in rows.items():
        for request in row[3]:index.setdefault(request,set()).add(key)
    return dict(claims={b:frozenset(c for c in claims if c[1]==b) for b in PAIR_BUCKETS},
                unplaced=frozenset(c[0] for c in claims if c[1] is None),
                rows=rows,index={q:frozenset(ks) for q,ks in index.items()})

def pl_view(state,bucket):
    # Derived per-bucket view: stored rows of the pairs of the bucket's claims.
    members=sorted(state['claims'][bucket],key=lambda c:c[0])
    rows=[];requests=[]
    for i,left in enumerate(members):
        for right in members[i+1:]:
            row=state['rows'].get(pl_key(left,right))
            if row is not None:
                rows.append((left[0],right[0],row[2]));requests.extend(row[3])
    return (tuple(rows),tuple(requests))

PL_VARIANTS=('old_members_only','keep_removed_pairs','ignore_arrivals')

def pl_step(state,removed,added,arrived,answers,variant=None):
    claims={b:list(ms) for b,ms in state['claims'].items()}
    unplaced=set(state['unplaced']);rows=dict(state['rows'])
    index={q:set(ks) for q,ks in state['index'].items()};old_index=state['index']
    count=dict(tests=0,deletes=0,visited=0);tested=set()
    def drop(key):
        row=rows.pop(key,None)
        if row is not None:
            for request in row[3]:index[request].discard(key)
    def put(key,row):
        drop(key);rows[key]=row
        for request in row[3]:index.setdefault(request,set()).add(key)
    # 1. Removed claims: delete their pairs, then leave the bucket.
    for x in removed:
        if x[1] is None:unplaced.discard(x[0]);continue
        if variant!='keep_removed_pairs':
            for z in claims[x[1]]:
                if z[0]!=x[0]:drop(pl_key(x,z));count['deletes']+=1
        claims[x[1]]=[z for z in claims[x[1]] if z!=x]
    before={b:list(ms) for b,ms in claims.items()}
    # 2. Added claims in list order, against the current bucket claims.
    for y in added:
        if y[1] is None:unplaced.add(y[0]);continue
        current=before[y[1]] if variant=='old_members_only' else claims[y[1]]
        for z in current:
            if z[0]!=y[0]:
                put(pl_key(y,z),pl_row(y,z,answers));tested.add(pl_key(y,z));count['tests']+=1
        claims[y[1]].append(y)
    # 3. Arrived requests through the OLD index.
    if variant!='ignore_arrivals':
        for request in arrived:
            for key in sorted(old_index.get(request,())):
                count['visited']+=1
                if key in tested or key not in rows:continue
                left,right=rows[key][:2]
                put(key,pl_row(left,right,answers));count['tests']+=1
    new=dict(claims={b:frozenset(ms) for b,ms in claims.items()},unplaced=frozenset(unplaced),
             rows=rows,index={q:frozenset(ks) for q,ks in index.items() if ks})
    return new,count,claims

PL_SLOTS=[None]+[(bucket,value) for bucket in (None,)+PAIR_BUCKETS for value in range(2)]

def pl_batch(claims,targets):
    # targets: claim id -> new slot. Removes the old claim, adds the new one.
    present={c[0]:c for c in claims}
    removed=[present[cid] for cid in targets if cid in present]
    added=[(cid,)+slot for cid,slot in targets.items() if slot is not None]
    return removed,added

def pl_kinds(removed,added,arrived):
    kinds=set()
    known=[c[1] for c in added if c[1] is not None]
    if len(known)>len(set(known)):kinds.add('two_adds_same_bucket')
    old={c[0]:c[1] for c in removed}
    for c in added:
        if c[0] in old:
            if old[c[0]] is None or c[1] is None:
                if old[c[0]]!=c[1]:kinds.add('unknown_move')
            elif old[c[0]]!=c[1]:kinds.add('known_move')
    if arrived:kinds.add('arrival')
    if arrived and (removed or added):kinds.add('claims_and_arrival')
    return kinds

def pl_run(c0,a0,steps,tally,cache):
    states={v:pl_scratch(c0,a0) for v in (None,)+PL_VARIANTS}
    claims,answers=c0,a0
    for removed,added,arrivals in steps:
        new_answers=dict(answers);new_answers.update(arrivals)
        arrived=list(arrivals)
        new_claims=(claims-frozenset(removed))|frozenset(added)
        key=(new_claims,tuple(sorted(new_answers.items())))
        if key not in cache:
            cache[key]=(pl_scratch(new_claims,new_answers),
                        {b:bucket_out(new_claims,new_answers,b) for b in PAIR_BUCKETS})
        want,full=cache[key]
        for variant,state in list(states.items()):
            old=state
            state,count,lists=pl_step(old,removed,added,arrived,new_answers,variant)
            states[variant]=state
            view_ok=all(pl_view(state,b)==full[b] for b in PAIR_BUCKETS)
            if variant is None:
                assert state==want and view_ok,(c0,a0,steps)
                # Theorem 2 bound, checked on every step.
                test_bound=(sum(len(lists[c[1]]) for c in added if c[1] is not None)+
                            sum(len(old['index'].get(q,())) for q in arrived))
                delete_bound=sum(len(old['claims'][c[1]]) for c in removed if c[1] is not None)
                assert count['tests']<=test_bound and count['deletes']<=delete_bound
                tally['states']+=1
                tally['pair_tests']+=count['tests']
                if count['tests']>tally['max_tests']:
                    tally['max_tests'],tally['bound_at_max_tests']=count['tests'],test_bound
                tally['tight_steps']+=count['tests']==test_bound>0
                tally['max_deletes']=max(tally['max_deletes'],count['deletes'])
                # Bucket-level UPDATE re-checks every pair of every affected bucket.
                affected={c[1] for c in list(removed)+list(added) if c[1] is not None}
                for q in arrived:
                    affected|={old['rows'][k][0][1] for k in old['index'].get(q,())}
                tally['bucket_level_tests']+=sum(len(full[b][0]) for b in affected)
                for kind in pl_kinds(removed,added,arrived):tally['kinds'][kind]+=1
                if len(added)==1 and not removed and not arrived and added[0][1] is not None:
                    size=len(lists[added[0][1]])
                    assert count['tests']<=size
                    tally['single_add_max_tests_vs_size']=max(
                        tally['single_add_max_tests_vs_size'],(count['tests'],size))
            else:
                tally[variant]['stale_rows']+=state['rows']!=want['rows']
                tally[variant]['stale_view']+=not view_ok
        claims,answers=new_claims,new_answers

def pairlevel_checks():
    import random
    requests=[(a,b) for a in range(4) for b in range(a+1,4)]
    small=[q for q in requests if q[1]<3]
    tally=dict(states=0,single_steps=0,chains_two=0,chains_three=0,pair_tests=0,
               bucket_level_tests=0,max_tests=0,bound_at_max_tests=0,tight_steps=0,max_deletes=0,
               single_add_max_tests_vs_size=(0,0),
               kinds={k:0 for k in ('two_adds_same_bucket','known_move','unknown_move','arrival',
                                     'claims_and_arrival')})
    for v in PL_VARIANTS:tally[v]=dict(stale_rows=0,stale_view=0)
    cache={}
    starts=[{},{small[0]:1},{q:1 for q in small},{small[0]:0,small[2]:1}]
    # Exhaustive single steps over claim ids 0-2: every batch that changes at
    # most two claims (removal, addition, move, Unknown move, in both orders
    # of the added list), and every single arrival.
    for choice in product(PL_SLOTS,repeat=3):
        c0=frozenset((cid,)+slot for cid,slot in enumerate(choice) if slot is not None)
        for a0 in starts:
            steps=[]
            for cid in range(3):
                for slot in PL_SLOTS:
                    if slot!=choice[cid]:steps.append(pl_batch(c0,{cid:slot}))
            for i,j in [(0,1),(0,2),(1,2)]:
                for si in PL_SLOTS:
                    for sj in PL_SLOTS:
                        if si!=choice[i] and sj!=choice[j]:
                            removed,added=pl_batch(c0,{i:si,j:sj})
                            steps.append((removed,added))
                            steps.append((removed,added[::-1]))
            for removed,added in steps:
                pl_run(c0,a0,[(removed,added,{})],tally,cache);tally['single_steps']+=1
            for q in small:
                if q in a0:continue
                for value in range(2):
                    pl_run(c0,a0,[([],[],{q:value})],tally,cache);tally['single_steps']+=1
    # Sampled chains: two steps over ids 0-2 and three steps over ids 0-3; a
    # step changes each claim with probability 0.4 and adds 0-2 arrivals.
    rng=random.Random(0)
    def random_step(claims,answers,ids,reqs):
        present={c[0]:c[1:] for c in claims}
        targets={}
        for cid in range(ids):
            if rng.random()<0.4:
                slot=rng.choice([s for s in PL_SLOTS if s!=present.get(cid)])
                targets[cid]=slot
        removed,added=pl_batch(claims,targets);rng.shuffle(added)
        open_=[q for q in reqs if q not in answers];rng.shuffle(open_)
        arrivals={q:rng.choice([0,1]) for q in open_[:rng.choice([0,0,1,2])]}
        return removed,added,arrivals
    for ids,length,count_key in [(3,2,'chains_two'),(4,3,'chains_three')]:
        reqs=small if ids==3 else requests
        for _ in range(20000):
            choice=[rng.choice(PL_SLOTS) for _ in range(ids)]
            c0=frozenset((cid,)+slot for cid,slot in enumerate(choice) if slot is not None)
            a0={q:v for q,v in zip(reqs,[rng.choice([None,0,1]) for _ in reqs]) if v is not None}
            claims,answers,steps=c0,a0,[]
            for _ in range(length):
                step=random_step(claims,answers,ids,reqs);steps.append(step)
                claims=(claims-frozenset(step[0]))|frozenset(step[1])
                answers=dict(answers);answers.update(step[2])
            pl_run(c0,a0,steps,tally,cache);tally[count_key]+=1
    assert all(n>0 for n in tally['kinds'].values()),tally['kinds']
    # (a) misses the pair of two claims added together; (b) keeps a removed
    # claim's rows (the view of current claims hides them, the stored rows do
    # not); (c) leaves a stale Pending.
    assert tally['old_members_only']['stale_view']>0
    assert tally['keep_removed_pairs']['stale_rows']>0
    assert tally['ignore_arrivals']['stale_view']>0
    tally['scope']=('exhaustive single steps over claim ids 0-2 (batches changing up to two claims,'+
                    ' both added-list orders, single arrivals); 20000 sampled two-step chains over'+
                    ' ids 0-2 and 20000 three-step chains over ids 0-3 with mixed claim and answer'+
                    ' changes; every state compared with a from-scratch build and every bucket view'+
                    ' with the full check; theorem 2 bound asserted on every step')
    return tally

# Algorithm 1 (EXTRACT). Answers: None is Missing, 'fail' is a stored failure,
# a tuple of row ids is a stored success. A request is (source, item).
def decode(request,answer):
    return [(request,row) for row in answer] if isinstance(answer,tuple) else []

def rebuild(sources,answers):
    records={}
    for source,item in sources.items():
        request=(source,item); answer=answers.get(request)
        records[source]=(request,answer,tuple(decode(request,answer)))
    return records

def incomplete(record):
    return not isinstance(record[1],tuple)

def exact_index(records):
    index={}
    for source,record in records.items():
        if record[1] is None:
            index.setdefault(record[0],set()).add(source)
    return index

def extract(old,index,unfinished,sources,answers,changed,arrived,
            use_index=True,removed_from_changed_only=False,keep_removed=False):
    selected=set(changed)
    if use_index:
        for request in arrived:
            selected|=index.get(request,set())
    records=dict(old); new_index={q:set(v) for q,v in index.items()}
    new_unfinished=set(unfinished); removed=set(); added=set()
    for source in selected:
        if source in old:
            record=old[source]
            if not removed_from_changed_only or source in changed:
                removed|=set(record[2])
            if record[1] is None:
                new_index.get(record[0],set()).discard(source)
            new_unfinished.discard(source)
            if not keep_removed or source in sources:
                records.pop(source,None)
        if source not in sources:
            continue
        request=(source,sources[source]); answer=answers.get(request)
        claims=tuple(decode(request,answer))
        if answer is None:
            new_index.setdefault(request,set()).add(source)
        if incomplete((request,answer)):
            new_unfinished.add(source)
        records[source]=(request,answer,claims)
        added|=set(claims)
    new_index={q:v for q,v in new_index.items() if v}
    return records,new_index,new_unfinished,removed-added,added-removed

def all_claims(records):
    return {claim for record in records.values() for claim in record[2]}

def extract_outcome_ok(S0,A0,S1,A1,changed,arrived,stale_index=None,**variant):
    old=rebuild(S0,A0)
    # stale_index: a complete but not exact index (extra entries), which the
    # pending-completeness precondition allows.
    index=exact_index(old) if stale_index is None else stale_index
    records,new_index,unfinished,removed,added=extract(
        old,index,{s for s,r in old.items() if incomplete(r)},S1,A1,changed,arrived,**variant)
    new=rebuild(S1,A1)
    return (records==new and (stale_index is not None or new_index==exact_index(new))
            and unfinished=={s for s,r in new.items() if incomplete(r)}
            and added==all_claims(new)-all_claims(old) and removed==all_claims(old)-all_claims(new))

def extract_cases(source_ids,items,outcomes):
    source_maps=[{s:i for s,i in zip(source_ids,choice) if i is not None}
                 for choice in product([None]+items,repeat=len(source_ids))]
    requests=[(s,i) for s in source_ids for i in items]
    for S0 in source_maps:
        for S1 in source_maps:
            for before in product(outcomes,repeat=len(requests)):
                A0={q:a for q,a in zip(requests,before) if a is not None}
                # Missing may become any outcome; a stored answer never changes.
                choices=[outcomes if a is None else [a] for a in before]
                for after in product(*choices):
                    A1={q:a for q,a in zip(requests,after) if a is not None}
                    yield S0,A0,S1,A1,requests

def extract_checks():
    outcomes=[None,'fail',(),(0,),(0,1)]
    sources=(0,1)
    exact=0
    for S0,A0,S1,A1,_ in extract_cases(list(sources),[0,1],outcomes):
        changed=[s for s in sources if S0.get(s)!=S1.get(s)]
        arrived=[q for q in A1 if q not in A0]
        assert extract_outcome_ok(S0,A0,S1,A1,changed,arrived)
        exact+=1
    # Three sources, one item each; supersets of changed/arrived and a stale
    # (complete, not exact) index are allowed by the hypotheses and stay exact.
    widened=0
    for S0,A0,S1,A1,requests in extract_cases([0,1,2],[0],outcomes):
        changed=[s for s in (0,1,2) if S0.get(s)!=S1.get(s)]
        arrived=[q for q in A1 if q not in A0]
        stale={q:{0,1,2} for q in requests}
        for c,a in [(changed,arrived),([0,1,2],arrived),(changed,requests),([0,1,2],requests)]:
            assert extract_outcome_ok(S0,A0,S1,A1,c,a)
            assert extract_outcome_ok(S0,A0,S1,A1,c,a,stale_index=stale)
            widened+=2
    variants=dict(ignore_pending_index=dict(use_index=False),
                  removed_only_from_changed=dict(removed_from_changed_only=True),
                  keep_claims_of_removed_source=dict(keep_removed=True))
    caught={}
    caught_exact_index={}
    for name,variant in variants.items():
        caught[name]=caught_exact_index[name]=0
        for S0,A0,S1,A1,requests in extract_cases(list(sources),[0,1],outcomes):
            changed=[s for s in sources if S0.get(s)!=S1.get(s)]
            arrived=[q for q in A1 if q not in A0]
            stale={q:set(sources) for q in requests}
            if not extract_outcome_ok(S0,A0,S1,A1,changed,arrived,**variant):
                caught_exact_index[name]+=1
            if not extract_outcome_ok(S0,A0,S1,A1,changed,arrived,stale_index=stale,**variant):
                caught[name]+=1
        assert caught[name]+caught_exact_index[name]>0,name
    # With an exact index, a source selected only by an arrival was Pending and
    # has no old claims, so computing removed from changed alone is harmless;
    # it is wrong only when the index holds stale entries.
    assert caught_exact_index['removed_only_from_changed']==0
    return dict(exact_two_sources=exact,exact_three_sources_widened=widened,
                broken_variants_caught_stale_index=caught,
                broken_variants_caught_exact_index=caught_exact_index,
                scope='finite check: 2 sources x 2 items and 3 sources x 1 item; outcomes Missing/failure/success with 0-2 rows')

# Algorithm 3 (REPAIR) over bitmask claim sets.
def safe_mask(edges,mask):
    return all(mask>>a&1 or mask>>b&1 for a,b in edges)

def safe_subsets(edges,allowed,vertex_mask):
    return [m for m in range(vertex_mask+1) if m&~allowed==0 and safe_mask(edges,m)]

def minimal_sets(edges,allowed,vertex_mask):
    safe=safe_subsets(edges,allowed&vertex_mask,vertex_mask)
    return {m for m in safe if not any(o!=m and o&m==o for o in safe)}

def weight_of(mask,weights):
    return sum(w for i,w in enumerate(weights) if mask>>i&1)

def min_weight_sets(edges,allowed,vertex_mask,weights):
    safe=safe_subsets(edges,allowed&vertex_mask,vertex_mask)
    if not safe:return set()
    best=min(weight_of(m,weights) for m in safe)
    return {m for m in safe if weight_of(m,weights)==best}

def components(edges):
    parent={}
    def find(x):
        parent.setdefault(x,x)
        while parent[x]!=x:x=parent[x]
        return x
    for a,b in edges:parent[find(a)]=find(b)
    groups={}
    for a,b in edges:
        groups.setdefault(find(a),[]).append((a,b))
    return list(groups.values())

def component_mask(edges):
    mask=0
    for a,b in edges:mask|=1<<a|1<<b
    return mask

def repair_component(component,protected,unfinished_check,weights=None):
    # Lines 10-13: an unfinished hard check refuses before any enumeration.
    if unfinished_check:return 'Unavailable',set()
    vertex_mask=component_mask(component); allowed=vertex_mask&~protected
    if weights is None:
        chosen=minimal_sets(component,allowed,vertex_mask)
    else:
        chosen=min_weight_sets(component,allowed,vertex_mask,weights)
    return ('Available',chosen) if chosen else ('Impossible',set())

def combine(per_component):
    result={0}
    for choices in per_component:
        result={r|c for r in result for c in choices}
    return result

def repair_checks(vertices=5):
    pairs=[(a,b) for a in range(vertices) for b in range(a+1,vertices)]
    full=(1<<vertices)-1
    graphs=checked=one_component_missed=0
    for chosen_edges in range(1<<len(pairs)):
        edges=[pairs[i] for i in range(len(pairs)) if chosen_edges>>i&1]
        comps=components(edges)
        graphs+=1
        for protected in range(1<<vertices):
            truth=minimal_sets(edges,full&~protected,full)
            per=[]
            for comp in comps:
                status,chosen=repair_component(comp,protected,False)
                assert (status=='Impossible')==(not chosen)
                per.append(chosen)
            combined=combine(per) if all(per) else set()
            assert combined==truth,(edges,protected)
            # Safe, never protected, never a claim outside every edge.
            for m in truth:
                assert safe_mask(edges,m) and m&protected==0 and m&~component_mask(edges)==0
            if len(comps)>1 and all(per) and combine(per[:1])!=truth:
                one_component_missed+=1
            checked+=1
    assert one_component_missed>0
    # Weighted mode with positive weights: per-component minimum-weight choices
    # combine to the global minimum-weight sets, each inclusion-minimal.
    small=4; small_pairs=[(a,b) for a in range(small) for b in range(a+1,small)]
    small_full=(1<<small)-1
    weighted=zero_weight_nonminimal=0
    for chosen_edges in range(1<<len(small_pairs)):
        edges=[small_pairs[i] for i in range(len(small_pairs)) if chosen_edges>>i&1]
        comps=components(edges)
        for protected in range(1<<small):
            minimal=minimal_sets(edges,small_full&~protected,small_full)
            for weights in product([1,2,3],repeat=small):
                truth=min_weight_sets(edges,component_mask(edges)&~protected,small_full,weights)
                assert truth<=minimal
                per=[repair_component(c,protected,False,weights)[1] for c in comps]
                assert (combine(per) if all(per) else set())==truth
                weighted+=1
        for weights in product([0,1],repeat=small):
            truth=min_weight_sets(edges,small_full,small_full,weights)
            if not truth<=minimal_sets(edges,small_full,small_full):
                zero_weight_nonminimal+=1
    assert zero_weight_nonminimal>0
    # Pending or Failed outcome in a hard rule's bucket: refuse. The broken
    # variant treats the unknown pair as no conflict and proposes a set that is
    # unsafe once the pending pair resolves to a violation.
    known=[(0,1)];pending_pair=(1,2)
    assert repair_component(known,0,True)==('Unavailable',set())
    broken=repair_component(known,0,False)[1]
    pending_caught=sum(1 for m in broken if not safe_mask(known+[pending_pair],m))
    assert pending_caught>0
    return dict(graphs=graphs,graph_protected_cases=checked,weighted_cases=weighted,
                broken_variants_caught=dict(zero_weight_returns_nonminimal=zero_weight_nonminimal,
                    one_component_only_misses_choices=one_component_missed,
                    pending_treated_as_no_conflict=pending_caught),
                scope='finite check: all graphs on 5 claims x every protected set; weighted on 4 claims, weights 1-3')

if __name__=='__main__':
    print(json.dumps(dict(status='pass',work=work_checks(),updates=update_checks(),uncertainty=uncertainty_checks(),progress=progress_checks(),recovery=recovery_checks(),cleanup=cleanup_checks(),pairs=pairs_checks(),pairlevel=pairlevel_checks(),extract=extract_checks(),repair=repair_checks()),indent=2))

import logging
import kopf
import dataclasses


# @kopf.on.login()
# def delayed_k3s(**_):
#     conn = kopf.login_via_pykube(logger=logging.getLogger('xxx'))
#     if conn:
#         return dataclasses.replace(conn, server=conn.server.rsplit(':', 1)[0] + ':11223')


@kopf.on.create('kopfexamples')
@kopf.on.update('kopfexamples')
def create_fn(meta, spec, reason, logger, **kwargs):
    rv = meta.get('resourceVersion')
    logger.warning(f">>> {rv=} And here we are! {reason=}: {spec}")


# @kopf.on.create('kopfexamples')
# def create_fn2(spec, **kwargs):
#     print(f"And here we are! Creating2: {spec}")

"""
=======================================================================================================================
Trigger with (delete the object first!):
$ kubectl apply -f examples/obj.yaml && sleep 1 && kubectl patch -f examples/obj.yaml --type merge -p '{"spec": {"field": 2}}'
=======================================================================================================================

The timeline of a misbehaving operator (with an artificial latency of 3 seconds):

         /-- kubectl creates an object (state a=s0)
         | ... sleep 1s
         |    /-- kubectl patches the spec.field with the patch "p1", creates state "b"=s0+p1
         |    |      /-- Kopf patches with annotations (state c=s0+p1+p2)
         |    |      |    /-- Kopf patches with annotations (the same state d=s0+p1+p2+p3, d==c)
         ↓    ↓      |    |
----+-//-aaaaabbbbbbbcccccdddddddddddddddddd--> which state is stored in kubernetes
         ↓    ↓      ↑↓   ↑↓
         |    |      ||   |\----3s----\
         |    |      |\---+3s----\    |
         |    \----3s+---\|      |    |
         \----3s----\|   ||      |    |
                    ↓↑   ↓↑      ↓    ↓
----+-//------------aaaaabbbbbbbbcccccdddddd--> which state is seen by the operator
    ↓               ↓↑   ↓↑      ↓    ↓
    |               ||   ||      |    \-- Kopf gets the state "d"=s0+p1+p2+p3, sees the annotations, goes idle.
    |               ||   ||      \-- Kopf gets the state "c"=s0+p1+p2, sees the annotations, goes idle.
    |               ||   ||
    |               ||   |\-- Kopf reacts, executes handlers (2ND TIME), adds annotations with a patch (p3)
    |               ||   \-- Kopf gets the state "b"=s0+p1 with NO annotations of "p2" yet.
    |               ||       !BUG!: "c"=s0+p1+p2 is not seen yet, though "c"/"p2" exists by now!
    |               ||
    |               |\-- Kopf reacts, executes handlers (1ST TIME), adds annotations with a patch (p2)
    |               \-- Kopf gets a watch-event (state a)
    \-- Kopf starts watching the resource

A fix with consistency tracking (with an artificial latency of 3 seconds):

         /-- kubectl creates an object (state a=s0)
         | ... sleep 1s
         |    /-- kubectl patches the spec.field with the patch "p1", creates state "b"=s0+p1
         |    |      /-- Kopf patches with annotations (state c=s0+p1+p2)
         ↓    ↓      |
----+-//-aaaaabbbbbbbccccccccccccccccccc-> which state is stored in kubernetes
         ↓    ↓      ↑↓
         |    |      |\----3s----\
         |    \----3s+---\       |
         \----3s----\|   |       |
                    ↓↑   ↓       ↓ 
----+-//------------aaaaabbbbbbbbcccccc-> which state is seen by the operator
    ↓               ↓↑⇶⇶⇶⇶⇶⇶⇶⇶⇶⇶⇶⇶↓  Kopf's own patch "p2" enables the consistency expectation for 5s OR version "c"
    |               ||   |       |
    |               ||   |       \-- Kopf gets a consistent state "c"=s0+p1+p2 as expected, thus goes idle.
    |               ||   |
    |               ||   \-- Kopf executes ONLY the low-level handlers over the state "b"=s0+p1.
    |               ||   \~~~~~~⨳ inconsistency mode: wait until a new event (then discard it) OR timeout (then process it) 
    |               ||
    |               |\-- Kopf reacts, executes handlers, adds annotations with a patch (p2)
    |               \-- Kopf gets a watch-event (state a)
    \-- Kopf starts watching the resource

"""

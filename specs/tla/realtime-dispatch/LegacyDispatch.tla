------------------------- MODULE LegacyDispatch -------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Capacity, Input, BypassBarrier, RemoteResponds,
          WriterResponds, RouterResponds, AllowCancel, CorruptReplies

WorkKinds == {"query", "relay"}
StreamKinds == {"data", "ack"}
IOKinds == StreamKinds \cup {"ping"}
Kinds == WorkKinds \cup IOKinds \cup {"state"}
WorkerPhases == {"remote", "workerWait", "workerWrite"}
Phases == {"fresh", "held", "reader", "done", "aborted"} \cup WorkerPhases
ReaderPCs == {"read", "decoded", "admit", "drain", "route",
              "writeWait", "writing", "stopped"}
Ids == 1..Len(Input)
Kind(i) == IF i \in Ids THEN Input[i] ELSE "none"
Peer(i) == IF Kind(i) \in WorkKinds THEN "slow" ELSE "healthy"
Stream(i) == IF Kind(i) = "ack" THEN "B" ELSE "A"

\* 有限输入配置。CapacityInput 的额外请求对应真实实现的第 65 项。
IsolationInput == <<"query", "data", "ping", "ack">>
RelayInput == <<"relay", "data", "ping", "ack">>
CapacityInput == [i \in 1..(Capacity + 4) |->
    IF i <= Capacity + 1 THEN "query"
    ELSE IF i = Capacity + 2 THEN "data"
    ELSE IF i = Capacity + 3 THEN "ping" ELSE "ack"]
MixedInput == <<"query", "relay", "data", "ping", "state", "ack">>
StateInput == <<"query", "state", "data", "ping", "ack">>
MultiFrameInput == <<"query", "data", "data", "ping", "ack", "data", "ack">>
EmptyInput == <<>>

ASSUME /\ Capacity \in {1, 2}
       /\ Input \in Seq(Kinds)
       /\ (Len(Input) = 0 \/ Len(Input) > 1)
       /\ BypassBarrier \in BOOLEAN
       /\ RemoteResponds \in BOOLEAN
       /\ WriterResponds \in BOOLEAN
       /\ RouterResponds \in BOOLEAN
       /\ AllowCancel \in BOOLEAN
       /\ CorruptReplies \in BOOLEAN

VARIABLES connection, transportClosed, pc, next, held, phase, writer,
          replies, routed, observations, stop

vars == <<connection, transportClosed, pc, next, held, phase, writer,
          replies, routed, observations, stop>>
Pending == {i \in Ids : phase[i] \in WorkerPhases}
IOIds == {i \in Ids : Kind(i) \in IOKinds}
Replied == {r.request : r \in {replies[n] : n \in 1..Len(replies)}}

Init == /\ connection = "open"
        /\ transportClosed = FALSE
        /\ pc = "read"
        /\ next = 1
        /\ held = 0
        /\ phase = [i \in Ids |-> "fresh"]
        /\ writer = 0
        /\ replies = <<>>
        /\ routed = <<>>
        /\ observations = <<>>
        /\ stop = [cause |-> "none", reader |-> "none", writer |-> 0, pending |-> 0]

Read == /\ connection = "open" /\ pc = "read" /\ next \in Ids
        /\ held' = next /\ next' = next + 1
        /\ phase' = [phase EXCEPT ![next] = "held"] /\ pc' = "decoded"
        /\ UNCHANGED <<connection, transportClosed, writer, replies, routed,
                       observations, stop>>

Admit == /\ connection = "open" /\ pc \in {"decoded", "admit"}
         /\ Kind(held) \in WorkKinds /\ Cardinality(Pending) < Capacity
         /\ phase' = [phase EXCEPT ![held] = "remote"]
         /\ pc' = "read" /\ held' = 0
         /\ UNCHANGED <<connection, transportClosed, next, writer, replies,
                        routed, observations, stop>>

WaitAdmission == /\ connection = "open" /\ pc = "decoded"
                 /\ Kind(held) \in WorkKinds
                 /\ Cardinality(Pending) = Capacity /\ pc' = "admit"
                 /\ UNCHANGED <<connection, transportClosed, next, held, phase,
                                writer, replies, routed, observations, stop>>

CanStart == Pending = {} \/ (BypassBarrier /\ Kind(held) \in IOKinds)

BeginHandler == /\ connection = "open" /\ pc \in {"decoded", "drain"}
                /\ Kind(held) \notin WorkKinds /\ CanStart
                /\ pc' = "route" /\ phase' = [phase EXCEPT ![held] = "reader"]
                /\ UNCHANGED <<connection, transportClosed, next, held, writer,
                               replies, routed, observations, stop>>

WaitDrain == /\ connection = "open" /\ pc = "decoded"
             /\ Kind(held) \notin WorkKinds /\ ~CanStart /\ pc' = "drain"
             /\ UNCHANGED <<connection, transportClosed, next, held, phase,
                            writer, replies, routed, observations, stop>>

\* Stream 调用仍在单一 reader 内完成；本模型不展开 mesh/session 校验。
Route == /\ connection = "open" /\ pc = "route"
         /\ (Kind(held) \notin StreamKinds \/ RouterResponds)
         /\ IF Kind(held) = "state"
               THEN /\ observations' = Append(observations,
                           [request |-> held, pending |-> Cardinality(Pending)])
                    /\ phase' = [phase EXCEPT ![held] = "done"]
                    /\ pc' = "read" /\ held' = 0
               ELSE /\ pc' = "writeWait"
                    /\ UNCHANGED <<phase, held, observations>>
         /\ routed' = IF Kind(held) \in StreamKinds
                        THEN Append(routed, [request |-> held, stream |-> Stream(held)])
                        ELSE routed
         /\ UNCHANGED <<connection, transportClosed, next, writer, replies, stop>>

RemoteReady(i) == /\ connection = "open" /\ phase[i] = "remote"
                  /\ RemoteResponds
                  /\ phase' = [phase EXCEPT ![i] = "workerWait"]
                  /\ UNCHANGED <<connection, transportClosed, pc, next, held,
                                 writer, replies, routed, observations, stop>>

AcquireWorker(i) == /\ connection \in {"open", "closing"}
                    /\ phase[i] = "workerWait" /\ writer = 0
                    /\ phase' = [phase EXCEPT ![i] = "workerWrite"] /\ writer' = i
                    /\ UNCHANGED <<connection, transportClosed, pc, next, held,
                                   replies, routed, observations, stop>>

AcquireReader == /\ connection \in {"open", "closing"}
                /\ pc = "writeWait" /\ writer = 0
                /\ writer' = held /\ pc' = "writing"
                /\ UNCHANGED <<connection, transportClosed, next, held, phase,
                               replies, routed, observations, stop>>

Reply(i) == [request |-> i,
             response |-> IF CorruptReplies THEN (i % Len(Input)) + 1 ELSE i,
             peer |-> Peer(i)]

\* Close 中断原响应 transport 的写入。不是对其他 receiver 的写入保证。
FinishWrite == /\ writer # 0 /\ (WriterResponds \/ transportClosed)
               /\ phase' = [phase EXCEPT ![writer] =
                     IF ~transportClosed THEN "done" ELSE "aborted"]
               /\ replies' = IF ~transportClosed
                               THEN Append(replies, Reply(writer)) ELSE replies
               /\ IF writer = held
                     THEN /\ pc' = IF connection = "open" THEN "read" ELSE "stopped"
                          /\ held' = 0
                     ELSE UNCHANGED <<pc, held>>
               /\ writer' = 0
               /\ UNCHANGED <<connection, transportClosed, next, routed, observations, stop>>

Cancel == /\ connection = "open" /\ AllowCancel
          /\ connection' = "closing"
          /\ stop' = [cause |-> "cancel", reader |-> pc,
                       writer |-> writer, pending |-> Cardinality(Pending)]
          /\ UNCHANGED <<transportClosed, pc, next, held, phase, writer,
                         replies, routed, observations>>

FailWrite == /\ connection = "open" /\ AllowCancel /\ writer # 0
             /\ connection' = "closing"
             /\ stop' = [cause |-> "writeFailure", reader |-> pc,
                          writer |-> writer, pending |-> Cardinality(Pending)]
             /\ phase' = [phase EXCEPT ![writer] = "aborted"] /\ writer' = 0
             /\ IF writer = held
                   THEN /\ pc' = "stopped" /\ held' = 0
                   ELSE UNCHANGED <<pc, held>>
             /\ UNCHANGED <<transportClosed, next, replies, routed, observations>>

CloseTransport == /\ connection = "closing" /\ ~transportClosed
                  /\ transportClosed' = TRUE
                  /\ UNCHANGED <<connection, pc, next, held, phase, writer,
                                 replies, routed, observations, stop>>

\* backend 遵守取消仍不足以直接退出：错误响应也必须经过 writeMu。
RemoteCancelled(i) == /\ connection = "closing" /\ phase[i] = "remote"
                      /\ phase' = [phase EXCEPT ![i] = "workerWait"]
                      /\ UNCHANGED <<connection, transportClosed, pc, next, held,
                                     writer, replies, routed, observations, stop>>

\* realtime IO 在 decode 后绕过 Wait，不做其后的 ctx.Err 检查。
BeginCancelledIO == /\ connection = "closing" /\ pc = "decoded"
                    /\ BypassBarrier /\ Kind(held) \in IOKinds
                    /\ pc' = "route" /\ phase' = [phase EXCEPT ![held] = "reader"]
                    /\ UNCHANGED <<connection, transportClosed, next, held, writer,
                                   replies, routed, observations, stop>>

\* 路由因 ctx 取消返回后，handleStreamFrame 仍会写错误响应。
RouteCancelled == /\ connection = "closing" /\ pc = "route"
                  /\ pc' = "writeWait"
                  /\ UNCHANGED <<connection, transportClosed, next, held, phase,
                                 writer, replies, routed, observations, stop>>

StopReader == /\ connection = "closing"
              /\ pc \in {"read", "decoded", "admit", "drain"}
              /\ (pc # "drain" \/ Pending = {})
              /\ (pc # "decoded" \/ Kind(held) \in WorkKinds \/
                     (~(BypassBarrier /\ Kind(held) \in IOKinds) /\ Pending = {}))
              /\ phase' = IF held # 0
                            THEN [phase EXCEPT ![held] = "aborted"] ELSE phase
              /\ held' = 0 /\ pc' = "stopped"
              /\ UNCHANGED <<connection, transportClosed, next, writer, replies,
                             routed, observations, stop>>

FinishClose == /\ connection = "closing" /\ transportClosed
               /\ Pending = {} /\ writer = 0 /\ pc = "stopped"
               /\ connection' = "closed"
               /\ UNCHANGED <<transportClosed, pc, next, held, phase, writer,
                              replies, routed, observations, stop>>

Next == Read \/ Admit \/ WaitAdmission \/ BeginHandler \/ WaitDrain \/ Route
        \/ AcquireReader \/ FinishWrite \/ Cancel \/ FailWrite \/ CloseTransport
        \/ BeginCancelledIO \/ RouteCancelled \/ StopReader \/ FinishClose
        \/ (\E i \in Ids : RemoteReady(i) \/ AcquireWorker(i) \/ RemoteCancelled(i))

Fairness == /\ WF_vars(Read) /\ WF_vars(Admit) /\ WF_vars(WaitAdmission)
            /\ WF_vars(BeginHandler) /\ WF_vars(WaitDrain) /\ WF_vars(Route)
            /\ WF_vars(AcquireReader) /\ WF_vars(FinishWrite)
            /\ WF_vars(CloseTransport) /\ WF_vars(StopReader) /\ WF_vars(FinishClose)
            /\ WF_vars(BeginCancelledIO) /\ WF_vars(RouteCancelled)
            /\ \A i \in Ids : /\ WF_vars(RemoteReady(i))
                                /\ WF_vars(AcquireWorker(i))
                                /\ WF_vars(RemoteCancelled(i))

Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK == /\ connection \in {"open", "closing", "closed"}
          /\ transportClosed \in BOOLEAN /\ pc \in ReaderPCs
          /\ next \in 1..(Len(Input) + 1) /\ held \in Ids \cup {0}
          /\ phase \in [Ids -> Phases] /\ writer \in Ids \cup {0}
          /\ replies \in Seq([request : Ids, response : Ids,
                              peer : {"slow", "healthy"}])
          /\ routed \in Seq([request : Ids, stream : {"A", "B"}])
          /\ observations \in Seq([request : Ids, pending : 0..Capacity])
          /\ stop \in [cause : {"none", "cancel", "writeFailure"},
                       reader : ReaderPCs \cup {"none"}, writer : Ids \cup {0},
                       pending : 0..Capacity]

BoundedWork == Cardinality(Pending) <= Capacity
WriterOwnership ==
    /\ writer # 0 => (phase[writer] = "workerWrite" \/
         (writer = held /\ pc = "writing" /\ phase[writer] = "reader"))
    /\ \A i \in Ids : phase[i] = "workerWrite" => writer = i
    /\ pc = "writing" => (writer = held /\ held # 0)
ReaderOwnership ==
    /\ (pc \in {"read", "stopped"}) = (held = 0)
    /\ \A i \in Ids : phase[i] \in {"held", "reader"} => i = held
ResponseCorrelation == \A n \in 1..Len(replies) :
    /\ replies[n].request = replies[n].response
    /\ replies[n].peer = Peer(replies[n].request)
AtMostOnceReply == \A a, b \in 1..Len(replies) :
    a # b => replies[a].request # replies[b].request
ReaderRouteOrder == \A a, b \in 1..Len(routed) :
    a < b => routed[a].request < routed[b].request
StateBarrier == \A n \in 1..Len(observations) : observations[n].pending = 0
ClosedHasNoWork == connection = "closed" =>
    (Pending = {} /\ pc = "stopped" /\ writer = 0 /\ transportClosed)

\* 结构性质比“查询迟早超时后会完成”更强，直接检查排空依赖。
NoQueryDrainForIO == ~(pc = "drain" /\ Kind(held) \in IOKinds)
IOEventuallyReplies == <> (IOIds \subseteq Replied)
AllRequestsComplete == <> (\A i \in Ids : phase[i] = "done")
ClosingDrains == (connection = "closing") ~> (connection = "closed")

\* 负向覆盖见证：这些不变量应失败，确认成功/取消状态确实可达。
NoIsolationWitness == ~(Pending # {} /\ IOIds \subseteq Replied)
NoCancelWitness == ~(connection = "closed" /\ stop.cause = "cancel" /\ stop.pending > 0)
NoCancelWriterWitness == ~(connection = "closed" /\ stop.cause = "cancel" /\ stop.writer # 0)
NoCancelAdmissionWitness == ~(connection = "closed" /\ stop.cause = "cancel" /\ stop.reader = "admit")
NoCancelRouteWitness == ~(connection = "closed" /\ stop.cause = "cancel" /\ stop.reader = "route")
NoWriteFailureWitness == ~(connection = "closed" /\ stop.cause = "writeFailure")
NoIdleCancelWitness == ~(connection = "closed" /\ stop.cause = "cancel" /\ Len(Input) = 0)
=============================================================================

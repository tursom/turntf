------------------------- MODULE RealtimeDispatch -------------------------
EXTENDS Naturals, Sequences, FiniteSets

CONSTANTS Capacity, StreamLimit, StreamBytes, ResponseLimit, ResponseBytes,
          Input, RemoteResponds, WriterResponds, SlowRoute, AllowCancel,
          Deadlines, SerialClient, RejectFull, CorruptReplies, SplitEpochKey,
          BreakFIFO, SkipBarrier

WorkKinds == {"query", "relay"}
Kinds == WorkKinds \cup {"stream", "ping", "state"}
F(k, u, s, t, e, w) ==
    [kind |-> k, user |-> u, session |-> s, stream |-> t, epoch |-> e, weight |-> w]
Q == F("query", "u", "s", "A", 1, 1)
R == F("relay", "u", "s", "A", 1, 1)
A == F("stream", "u", "s", "A", 1, 1)
A2 == F("stream", "u", "s", "A", 2, 1)
B == F("stream", "u", "s", "B", 1, 1)
P == F("ping", "u", "s", "A", 1, 1)
S == F("state", "u", "s", "A", 1, 1)
IsolationInput == <<Q, B, P>>
RelayInput == <<R, B, P>>
CapacityInput == [i \in 1..(Capacity + 3) |->
    IF i <= Capacity + 1 THEN Q ELSE IF i = Capacity + 2 THEN B ELSE P]
NormalInput == <<Q, R, B, P, S>>
StateInput == <<Q, S, B, P>>
StreamBarrierInput == <<A, S, P>>
ResponseBarrierInput == <<P, S, B>>
MultiFrameInput == <<A, A2, B, B, P>>
\* 相同 stream_id，分别只改变 user/session，必须有独立路由能力。
KeyInput == <<A, A2, F("stream", "v", "s", "A", 1, 1),
             F("stream", "u", "t", "A", 1, 1), P>>
StreamOverflowInput == <<A, A2, A>>
StreamByteInput == <<F("stream", "u", "s", "A", 1, 2), A2>>
ResponseOverflowInput == <<P, P, P>>
ResponseByteInput == <<F("ping", "u", "s", "A", 1, 2), P>>
EmptyInput == <<>>

Ids == 1..Len(Input)
Kind(i) == IF i \in Ids THEN Input[i].kind ELSE "none"
Weight(i) == Input[i].weight
Key(i) == <<Input[i].user, Input[i].session, Input[i].stream>>
ScheduleKey(i) == IF SplitEpochKey THEN Append(Key(i), Input[i].epoch) ELSE Key(i)
Slow(i) == SlowRoute /\ Key(i) = <<"u", "s", "A">>
IOIds == {i \in Ids : Kind(i) \in {"stream", "ping"}}
HealthyIds == {i \in IOIds : Kind(i) = "ping" \/ ~Slow(i)}
StreamPhases == {"queued", "route", "wait", "write"}
WorkPhases == {"remote", "wait", "write"}
Terminal == {"done", "rejected", "aborted"}
Phases == {"fresh", "held", "responseQueued"} \cup Terminal \cup StreamPhases \cup WorkPhases

ASSUME /\ Capacity \in {1, 2} /\ StreamLimit \in {1, 2, 4}
       /\ StreamBytes \in {1, 2, 4, 8} /\ ResponseLimit \in {1, 2, 4}
       /\ ResponseBytes \in {1, 2, 4, 8}
       /\ Input \in Seq([kind : Kinds, user : {"u", "v"}, session : {"s", "t"},
                         stream : {"A", "B"}, epoch : {1, 2}, weight : {1, 2}])
       /\ \A b \in {RemoteResponds, WriterResponds, SlowRoute, AllowCancel,
                    Deadlines, SerialClient, RejectFull, CorruptReplies,
                    SplitEpochKey, BreakFIFO, SkipBarrier} : b \in BOOLEAN

VARIABLES connection, transportClosed, pc, next, held, phase, writer,
          responseQueue, responseActive, armed, started, rejected,
          replies, routed, observations, stop
vars == <<connection, transportClosed, pc, next, held, phase, writer,
          responseQueue, responseActive, armed, started, rejected,
          replies, routed, observations, stop>>
Work == {i \in started : phase[i] \in WorkPhases}
Streams == {i \in Ids : Kind(i) = "stream" /\ phase[i] \in StreamPhases}
Responses == {i \in Ids : phase[i] = "responseQueued" \/ i = responseActive}
Outstanding == Work \cup Streams \cup Responses
\* 权重只有 1/2，避免无界整数/递归求和；含活动项，非仅队列长度。
Bytes(xs) == Cardinality(xs) + Cardinality({i \in xs : Weight(i) = 2})
ResponseFits == Cardinality(Responses) < ResponseLimit /\
                Bytes(Responses) + Weight(held) <= ResponseBytes
StreamFits == Cardinality(Streams) < StreamLimit /\
              Bytes(Streams) + Weight(held) <= StreamBytes
SameKeyBefore(i) == {j \in Streams : j < i /\ ScheduleKey(j) = ScheduleKey(i)}
CanStart(i) == BreakFIFO \/ SameKeyBefore(i) = {}
Success == {replies[n].request : n \in {m \in 1..Len(replies) : replies[m].status = "ok"}}

Init == /\ connection = "open" /\ transportClosed = FALSE
        /\ pc = "read" /\ next = 1 /\ held = 0
        /\ phase = [i \in Ids |-> "fresh"] /\ writer = 0
        /\ responseQueue = <<>> /\ responseActive = 0 /\ armed = {}
        /\ started = {} /\ rejected = {} /\ replies = <<>> /\ routed = <<>>
        /\ observations = <<>>
        /\ stop = [cause |-> "none", reader |-> "none", writer |-> 0,
                    work |-> 0, streams |-> 0, responses |-> 0, expired |-> 0]

Read == /\ connection = "open" /\ pc = "read" /\ next \in Ids
        /\ held' = next /\ next' = next + 1 /\ pc' = "decoded"
        /\ phase' = [phase EXCEPT ![next] = "held"]
        /\ UNCHANGED <<connection, transportClosed, writer, responseQueue,
                       responseActive, armed, started, rejected, replies, routed, observations, stop>>

Admit == /\ connection = "open" /\ pc \in {"decoded", "admit"}
         /\ Kind(held) \in WorkKinds /\ Cardinality(Work) < Capacity
         /\ phase' = [phase EXCEPT ![held] = "remote"] /\ started' = started \cup {held}
         /\ pc' = IF SerialClient THEN "serial" ELSE "read"
         /\ held' = IF SerialClient THEN held ELSE 0
         /\ UNCHANGED <<connection, transportClosed, next, writer, responseQueue,
                        responseActive, armed, rejected, replies, routed, observations, stop>>
WaitAdmission == /\ connection = "open" /\ pc = "decoded"
                 /\ Kind(held) \in WorkKinds /\ Cardinality(Work) = Capacity /\ ~RejectFull
                 /\ pc' = "admit"
                 /\ UNCHANGED <<connection, transportClosed, next, held, phase, writer,
                                responseQueue, responseActive, armed, started, rejected,
                                replies, routed, observations, stop>>
NeedsResponse == Kind(held) = "ping" \/
    (Kind(held) \in WorkKinds /\ Cardinality(Work) = Capacity /\ RejectFull)
EnqueueResponse == /\ connection = "open" /\ pc = "decoded"
                   /\ NeedsResponse /\ ResponseFits
                   /\ phase' = [phase EXCEPT ![held] = "responseQueued"]
                   /\ responseQueue' = Append(responseQueue, held)
                   /\ rejected' = IF Kind(held) \in WorkKinds THEN rejected \cup {held} ELSE rejected
                   /\ pc' = IF SerialClient THEN "serial" ELSE "read"
                   /\ held' = IF SerialClient THEN held ELSE 0
                   /\ UNCHANGED <<connection, transportClosed, next, writer, responseActive,
                                  armed, started, replies, routed, observations, stop>>
EnqueueStream == /\ connection = "open" /\ pc = "decoded"
                 /\ Kind(held) = "stream" /\ StreamFits
                 /\ phase' = [phase EXCEPT ![held] = "queued"]
                 /\ pc' = IF SerialClient THEN "serial" ELSE "read"
                 /\ held' = IF SerialClient THEN held ELSE 0
                 /\ UNCHANGED <<connection, transportClosed, next, writer, responseQueue,
                                responseActive, armed, started, rejected, replies, routed, observations, stop>>
StartStream(i) == /\ connection = "open" /\ phase[i] = "queued" /\ CanStart(i)
                  /\ phase' = [phase EXCEPT ![i] = "route"]
                  /\ UNCHANGED <<connection, transportClosed, pc, next, held, writer,
                                 responseQueue, responseActive, armed, started, rejected,
                                 replies, routed, observations, stop>>
Route(i) == /\ connection = "open" /\ phase[i] = "route" /\ ~Slow(i)
            /\ phase' = [phase EXCEPT ![i] = "wait"] /\ armed' = armed \cup {i}
            /\ routed' = Append(routed, i)
            /\ UNCHANGED <<connection, transportClosed, pc, next, held, writer,
                           responseQueue, responseActive, started, rejected, replies, observations, stop>>
RemoteReady(i) == /\ connection = "open" /\ phase[i] = "remote" /\ RemoteResponds
                  /\ phase' = [phase EXCEPT ![i] = "wait"] /\ armed' = armed \cup {i}
                  /\ UNCHANGED <<connection, transportClosed, pc, next, held, writer,
                                 responseQueue, responseActive, started, rejected,
                                 replies, routed, observations, stop>>
StartResponse == /\ connection \in {"open", "closing"}
                 /\ responseActive = 0 /\ Len(responseQueue) > 0
                 /\ responseActive' = Head(responseQueue) /\ responseQueue' = Tail(responseQueue)
                 /\ phase' = [phase EXCEPT ![Head(responseQueue)] = "wait"]
                 /\ armed' = armed \cup {Head(responseQueue)}
                 /\ UNCHANGED <<connection, transportClosed, pc, next, held, writer,
                                started, rejected, replies, routed, observations, stop>>
Acquire(i) == /\ connection \in {"open", "closing"} /\ phase[i] = "wait" /\ writer = 0
              /\ phase' = [phase EXCEPT ![i] = "write"] /\ writer' = i
              \* armed 在等锁和 Send 之间不重置，表示同一总预算。
              /\ UNCHANGED <<connection, transportClosed, pc, next, held,
                             responseQueue, responseActive, armed, started, rejected,
                             replies, routed, observations, stop>>
Reply(i) == [request |-> i, response |-> IF CorruptReplies THEN (i % Len(Input)) + 1 ELSE i,
             status |-> IF connection # "open" THEN "cancelled"
                        ELSE IF i \in rejected THEN "service_unavailable" ELSE "ok"]
FinishWrite == /\ writer # 0 /\ (WriterResponds \/ transportClosed)
               /\ phase' = [phase EXCEPT ![writer] = IF transportClosed \/ connection # "open"
                    THEN "aborted" ELSE IF writer \in rejected THEN "rejected" ELSE "done"]
               /\ replies' = IF transportClosed THEN replies ELSE Append(replies, Reply(writer))
               /\ armed' = armed \ {writer}
               /\ responseActive' = IF responseActive = writer THEN 0 ELSE responseActive
               /\ writer' = 0
               /\ UNCHANGED <<connection, transportClosed, pc, next, held, responseQueue,
                              started, rejected, routed, observations, stop>>
Barrier == /\ connection = "open" /\ pc \in {"decoded", "drain"} /\ Kind(held) = "state"
           /\ (Outstanding = {} \/ SkipBarrier)
           /\ observations' = Append(observations,
                [request |-> held, prior |-> Cardinality(Outstanding)])
           /\ phase' = [phase EXCEPT ![held] = "done"] /\ pc' = "read" /\ held' = 0
           /\ UNCHANGED <<connection, transportClosed, next, writer, responseQueue,
                          responseActive, armed, started, rejected, replies, routed, stop>>
WaitBarrier == /\ connection = "open" /\ pc = "decoded" /\ Kind(held) = "state"
               /\ Outstanding # {} /\ ~SkipBarrier /\ pc' = "drain"
               /\ UNCHANGED <<connection, transportClosed, next, held, phase, writer,
                              responseQueue, responseActive, armed, started, rejected,
                              replies, routed, observations, stop>>
SerialAdvance == /\ connection = "open" /\ pc = "serial" /\ phase[held] \in Terminal
                 /\ pc' = "read" /\ held' = 0
                 /\ UNCHANGED <<connection, transportClosed, next, phase, writer,
                                responseQueue, responseActive, armed, started, rejected,
                                replies, routed, observations, stop>>

Snapshot(cause, expired) == [cause |-> cause, reader |-> pc, writer |-> writer,
    work |-> Cardinality(Work), streams |-> Cardinality(Streams),
    responses |-> Cardinality(Responses), expired |-> expired]
Cancel == /\ connection = "open" /\ AllowCancel
          /\ connection' = "closing" /\ stop' = Snapshot("cancel", 0)
          /\ UNCHANGED <<transportClosed, pc, next, held, phase, writer, responseQueue,
                         responseActive, armed, started, rejected, replies, routed, observations>>
FailWrite == /\ connection = "open" /\ writer # 0 /\ AllowCancel /\ WriterResponds
             /\ connection' = "closing" /\ stop' = Snapshot("writeFailure", writer)
             /\ UNCHANGED <<transportClosed, pc, next, held, phase, writer, responseQueue,
                            responseActive, armed, started, rejected, replies, routed, observations>>
\* 无时钟抽象：永久不可写时，总预算最终到期；正常写环境假定预算内返回。
\* 不公平化 Cancel，也不让慢路由触发写 deadline。
Expire(i) == /\ connection = "open" /\ Deadlines /\ ~WriterResponds /\ i \in armed
             /\ connection' = "closing" /\ stop' = Snapshot("deadline", i)
             /\ UNCHANGED <<transportClosed, pc, next, held, phase, writer, responseQueue,
                            responseActive, armed, started, rejected, replies, routed, observations>>
Overflow == /\ connection = "open" /\ pc = "decoded"
            /\ ((Kind(held) = "stream" /\ ~StreamFits) \/ (NeedsResponse /\ ~ResponseFits))
            /\ connection' = "closing"
            /\ stop' = Snapshot(IF Kind(held) = "stream"
                 THEN IF Cardinality(Streams) = StreamLimit THEN "streamItems" ELSE "streamBytes"
                 ELSE IF Cardinality(Responses) = ResponseLimit THEN "responseItems" ELSE "responseBytes", 0)
            /\ UNCHANGED <<transportClosed, pc, next, held, phase, writer, responseQueue,
                           responseActive, armed, started, rejected, replies, routed, observations>>
CloseTransport == /\ connection = "closing" /\ ~transportClosed
                  /\ transportClosed' = TRUE
                  /\ UNCHANGED <<connection, pc, next, held, phase, writer, responseQueue,
                                 responseActive, armed, started, rejected, replies, routed, observations, stop>>
CancelHandler(i) == /\ connection = "closing"
                    /\ (phase[i] \in {"remote", "route"} \/
                         (phase[i] = "queued" /\ CanStart(i)))
                    /\ phase' = [phase EXCEPT ![i] = "wait"] /\ armed' = armed \cup {i}
                    /\ UNCHANGED <<connection, transportClosed, pc, next, held, writer,
                                   responseQueue, responseActive, started, rejected,
                                   replies, routed, observations, stop>>
StopReader == /\ connection = "closing" /\ pc # "stopped"
              /\ (pc # "read" \/ transportClosed)
              /\ (pc # "drain" \/ Outstanding = {})
              /\ (IF pc = "serial" THEN phase[held] \in Terminal ELSE TRUE)
              /\ phase' = IF held = 0 THEN phase ELSE
                    IF phase[held] = "held" THEN [phase EXCEPT ![held] = "aborted"] ELSE phase
              /\ pc' = "stopped" /\ held' = 0
              /\ UNCHANGED <<connection, transportClosed, next, writer, responseQueue,
                             responseActive, armed, started, rejected, replies, routed, observations, stop>>
FinishClose == /\ connection = "closing" /\ transportClosed
               /\ Outstanding = {} /\ writer = 0 /\ pc = "stopped"
               /\ connection' = "closed"
               /\ UNCHANGED <<transportClosed, pc, next, held, phase, writer, responseQueue,
                              responseActive, armed, started, rejected, replies, routed, observations, stop>>
Next == Read \/ Admit \/ WaitAdmission \/ EnqueueResponse \/ EnqueueStream \/ StartResponse
        \/ FinishWrite \/ Barrier \/ WaitBarrier \/ SerialAdvance \/ Cancel \/ FailWrite
        \/ Overflow \/ CloseTransport \/ StopReader \/ FinishClose
        \/ (\E i \in Ids : StartStream(i) \/ Route(i) \/ RemoteReady(i) \/ Acquire(i)
                           \/ Expire(i) \/ CancelHandler(i))
Fairness == /\ WF_vars(Read) /\ WF_vars(Admit) /\ WF_vars(WaitAdmission)
            /\ WF_vars(EnqueueResponse) /\ WF_vars(EnqueueStream) /\ WF_vars(StartResponse)
            /\ WF_vars(FinishWrite) /\ WF_vars(Barrier) /\ WF_vars(WaitBarrier)
            /\ WF_vars(SerialAdvance) /\ WF_vars(Overflow) /\ WF_vars(CloseTransport)
            /\ WF_vars(StopReader) /\ WF_vars(FinishClose)
            /\ \A i \in Ids : /\ WF_vars(StartStream(i)) /\ WF_vars(Route(i))
                /\ WF_vars(RemoteReady(i)) /\ WF_vars(Acquire(i))
                /\ WF_vars(Expire(i)) /\ WF_vars(CancelHandler(i))
Spec == Init /\ [][Next]_vars /\ Fairness

TypeOK == /\ connection \in {"open", "closing", "closed"} /\ transportClosed \in BOOLEAN
          /\ pc \in {"read", "decoded", "admit", "drain", "serial", "stopped"}
          /\ next \in 1..(Len(Input) + 1) /\ held \in Ids \cup {0}
          /\ phase \in [Ids -> Phases] /\ writer \in Ids \cup {0}
          /\ responseQueue \in Seq(Ids) /\ responseActive \in Ids \cup {0}
          /\ armed \subseteq Ids /\ started \subseteq Ids /\ rejected \subseteq Ids
          /\ replies \in Seq([request : Ids, response : Ids,
                               status : {"ok", "service_unavailable", "cancelled"}])
          /\ routed \in Seq(Ids)
          /\ observations \in Seq([request : Ids, prior : 0..(Capacity + StreamLimit + ResponseLimit)])
          /\ stop \in [cause : {"none", "cancel", "writeFailure", "deadline", "streamItems",
                                "streamBytes", "responseItems", "responseBytes"},
                       reader : {"none", "read", "decoded", "admit", "drain", "serial", "stopped"},
                       writer : Ids \cup {0}, work : 0..Capacity, streams : 0..StreamLimit,
                       responses : 0..ResponseLimit, expired : Ids \cup {0}]
BoundedWork == Cardinality(Work) <= Capacity
BoundedStreams == Cardinality(Streams) <= StreamLimit /\ Bytes(Streams) <= StreamBytes
BoundedResponses == Cardinality(Responses) <= ResponseLimit /\ Bytes(Responses) <= ResponseBytes
WriterOwnership == /\ writer # 0 => (phase[writer] = "write" /\ writer \in armed)
                   /\ \A i \in Ids : (phase[i] = "write" => writer = i)
                   /\ armed = {i \in Ids : phase[i] \in {"wait", "write"}}
ResponseOwnership == /\ Cardinality(Responses) = Len(responseQueue) + (IF responseActive = 0 THEN 0 ELSE 1)
                     /\ \A n \in 1..Len(responseQueue) : phase[responseQueue[n]] = "responseQueued"
                     /\ responseActive # 0 => phase[responseActive] \in {"wait", "write"}
ReaderOwnership == /\ (pc \in {"read", "stopped"}) = (held = 0)
                   /\ \A i \in Ids : phase[i] = "held" => held = i
                   /\ pc = "serial" => SerialClient
ResponseCorrelation == \A n \in 1..Len(replies) : replies[n].request = replies[n].response
AtMostOnceReply == \A a, b \in 1..Len(replies) : a # b => replies[a].request # replies[b].request
RejectionSafety == /\ started \cap rejected = {} /\ Success \cap rejected = {}
                   /\ \A n \in 1..Len(replies) :
                        (replies[n].status = "service_unavailable" => replies[n].request \in rejected)
StreamFIFO == \A a, b \in 1..Len(routed) :
    (a < b /\ Key(routed[a]) = Key(routed[b])) => routed[a] < routed[b]
\* 除完成日志外检查尚未完成的前帧，防止慢首帧使顺序断言真空成立。
StreamPrefix == \A n \in 1..Len(routed) : \A j \in Ids :
    (j < routed[n] /\ Kind(j) = "stream" /\ Key(j) = Key(routed[n])) =>
         j \in {routed[m] : m \in 1..(n - 1)}
StateBarrier == \A n \in 1..Len(observations) : observations[n].prior = 0
ClosedHasNoWork == connection = "closed" =>
    (Outstanding = {} /\ writer = 0 /\ armed = {} /\ pc = "stopped" /\ transportClosed)
NoBlockingAdmission == pc # "admit"
SessionOpen == connection = "open"
IOEventuallyReplies == <> (IOIds \subseteq Success)
HealthyIO == <> (HealthyIds \subseteq Success)
IOCompletedOrFailed == <> (IOIds \subseteq Success \/ connection = "closed")
AllRequestsResolved == <> (\A i \in Ids : phase[i] \in {"done", "rejected"})
AllRequestsSuccessful == <> (\A i \in Ids : phase[i] = "done")
ClosingDrains == (connection = "closing") ~> (connection = "closed")
EventuallyClosed == <> (connection = "closed")

\* 以下都是预期失败的不变量；覆盖必须包含响应和具体未完成工作/原因。
NoIsolationWitness == ~(connection = "open" /\ Work # {} /\ IOIds \subseteq Success)
NoRejectedWitness == ~(connection = "open" /\ rejected # {} /\ IOIds \subseteq Success /\
    \E n \in 1..Len(replies) : replies[n].status = "service_unavailable")
NoHealthyWitness == ~(connection = "open" /\ HealthyIds \subseteq Success /\
    \E i, j \in Streams : i < j /\ Slow(i) /\ Key(i) = Key(j) /\
         Input[i].epoch # Input[j].epoch /\ phase[i] = "route" /\ phase[j] = "queued")
NoCrossKeyWitness == ~(connection = "open" /\
    \E i \in Streams : Slow(i) /\ \E n \in 1..Len(routed) : routed[n] > i /\ Key(routed[n]) # Key(i))
NoMultiFrameWitness == ~(connection = "open" /\ IOIds \subseteq Success /\
    \E a, b \in 1..Len(routed) : a < b /\ Key(routed[a]) = Key(routed[b]) /\
        Input[routed[a]].epoch # Input[routed[b]].epoch)
NoCancelWitness == ~(connection = "closed" /\ stop.cause = "cancel" /\ stop.work > 0)
NoCancelRouteWitness == ~(connection = "closed" /\ stop.cause = "cancel" /\ stop.streams > 0)
NoCancelWriterWitness == ~(connection = "closed" /\ stop.cause = "cancel" /\ stop.writer # 0)
NoWriteFailureWitness == ~(connection = "closed" /\ stop.cause = "writeFailure")
NoIdleCancelWitness == ~(connection = "closed" /\ stop.cause = "cancel" /\ Len(Input) = 0)
NoDeadlineWitness == ~(connection = "closed" /\ stop.cause = "deadline")
NoLockDeadlineWitness == ~(connection = "closed" /\ stop.cause = "deadline" /\
    stop.writer # 0 /\ stop.expired # stop.writer)
NoStreamItemsWitness == ~(connection = "closed" /\ stop.cause = "streamItems")
NoStreamBytesWitness == ~(connection = "closed" /\ stop.cause = "streamBytes")
NoResponseItemsWitness == ~(connection = "closed" /\ stop.cause = "responseItems")
NoResponseBytesWitness == ~(connection = "closed" /\ stop.cause = "responseBytes")
NoActiveResponseWitness == ~(connection = "closed" /\ stop.cause = "responseItems" /\
    stop.responses = ResponseLimit /\ stop.writer # 0)
NoBarrierWitness == ~(connection = "open" /\ Len(observations) > 0 /\
    \A i \in 1..(observations[1].request - 1) : phase[i] \in {"done", "rejected"})
=============================================================================

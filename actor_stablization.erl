-module(actor_stablization).


-export([start/1, is_in_range/4, find_successor/4, is_alive/1, get_successor_from_state/1, is_in_range/4, closest_to/2]).
-import(math, [pow/2]).
-record(state, {id,
            predecessor,
            finger_table,
            next,
            m}).

start(NodeState) -> 
    StablizeOutput = stablize(NodeState),
    FixFingersOutput = fix_fingers(StablizeOutput, NodeState#state.id),
    NodeState = check_predecessor(FixFingersOutput),
    start_on_other_nodes(NodeState#state.id, NodeState#state.m),
    NodeState.



stablize(NodeState) ->
    io:format("Stablize ~p\n", [NodeState#state.id]),
    io:format("In stablize method ~p\n", [NodeState#state.finger_table]),
    Successor = get_successor(NodeState),
    io:format("Nodeid: ~p, It's Successor: ~p\n", [NodeState#state.id, Successor]),

    if [NodeState#state.id] =:= [Successor] ->  Predecessor = NodeState#state.predecessor;
       true -> Predecessor = get_predecessor(Successor)
    end,

    io:format("Predecessor of Node ~p is ~p\n", [NodeState#state.id, Predecessor]),

    IsInRange = is_in_range(NodeState#state.id, Predecessor, Successor, NodeState#state.m), 
    if ((Predecessor =/= -1) and (IsInRange)) -> Successor = Predecessor;
        true -> ok
    end,

    notify_successor(Successor, NodeState#state.id),
    update_successor(Successor, NodeState, 1).


get_successor(NodeState) ->
    FingerTable = [NodeState#state.finger_table],
    FirstNode = lists:nth(1, FingerTable),

    AliveFirstNode = try whereis(list_to_atom([FirstNode])) of
                        _ -> FirstNode
                    catch 
                        _ErrType:_Err -> errormessage,
                        NodeState#state.id
                    end,

    AliveFirstNode.

get_predecessor(Successor) ->
    io:format("Lookup in empty table: ~p\n\n", [ets:lookup(predecessor_table,"abc")]),
    Predecessor = ets:lookup(predecessor_table,Successor),
    io:format("Predecessor from lookup: ~p\n", [Predecessor]),
    case Predecessor of
        [{_, Predecessor}] -> Predecessor;
        _ -> -1
    end.

is_in_range(From, Key, To, M) ->
    TwoPowerM = math:pow(2, M) + 1,
    To = if (To > TwoPowerM)-> 1;
            true -> To
        end,

    if 
        From < To -> (From < Key) and (Key < To);
        From =:= To -> Key =/= From;
        From > To -> ((Key > 0 ) and (Key < To)) or ((Key > From) and (Key =< math:pow(2, M)))
    end.


notify_successor(Successor, NewPredecessor) ->
    SuccessorPid = whereis(list_to_atom([Successor])),
    NewPredecessorId = list_to_atom([NewPredecessor]),
    NewPredecessorId ! {notify, SuccessorPid}.

update_successor(Successor, NodeState, Index) ->
    io:format("Entering update_successor. NodeState: ~p \n", [NodeState]),
    FingerTable = NodeState#state.finger_table,
    if (Index =:= 1) -> NodeState#state{finger_table = [Successor] ++ lists:nthtail(Index+1, FingerTable)};
        (Index > 1) -> NodeState#state{finger_table = lists:sublist(FingerTable, Index - 1) ++ [Successor] ++ lists:nthtail(Index+1, FingerTable)};
    true -> ok
    end,
    io:format("update_successor done. NodeState: ~p \n", [NodeState]),
    NodeState.


fix_fingers(NodeState, Origin) ->
    NextId = get_valid_next_id(NodeState#state.next, NodeState),
    [Successor, _] = find_successor(Origin, NextId, 0, NodeState),
    UpdatedNodeState = update_successor(Successor, NodeState, NodeState#state.next),
    UpdatedNodeState#state{next = (UpdatedNodeState#state.next + 1) rem (UpdatedNodeState#state.m)},
    io:format("Done with fix_finger. NodeState: ~p, Pred: ~p \n", [UpdatedNodeState, UpdatedNodeState#state.predecessor]),
    UpdatedNodeState.


get_valid_next_id(Next, NodeState) ->
    NumNodes = math:pow(2, NodeState#state.m),
    NextId = NodeState#state.id + math:pow(2, Next),

    if NextId > NumNodes -> NextId - NumNodes;
        true -> NextId
    end.

find_successor(Origin, NodeId, HopsCount, NodeState) ->
    Successor = get_successor_from_state(NodeState),
    IsInRange = is_in_range(NodeState#state.id, NodeId, Successor + 1, NodeState#state.m),
    if IsInRange -> [Successor, HopsCount];
        true -> ClosestOutput = closest_to(NodeId, NodeState),
                get_successor_of_closest(ClosestOutput, NodeState#state.id, NodeId, HopsCount+1, Origin)
    end.
    
get_successor_from_state(NodeState) ->
    Successor = lists:nth(1, NodeState#state.finger_table),
    IsAlive = is_alive(Successor),
    if (IsAlive =:= 1) -> Successor;
        true -> NodeState#state.id
    end.

is_alive(Pid) ->
    if Pid < 0 -> 1;
    true ->
            io:format("Pid is ~p\n", [Pid]),
            NodePid = whereis(list_to_atom([Pid])),
            io:format("NodePid is ~p\n", [NodePid]),
            try NodePid ! {dosp}  of
                _ ->  1
            catch 
                _ErrType:_Err -> errormessage,
                0   
            end
    end.

closest_to(NodeId, NodeState) ->
    Range = lists:seq(NodeState#state.m - 1, 1, -1),
    FingerTableBottomUp = lists:map(fun(RangeElm) ->  lists:nth(RangeElm, NodeState#state.finger_table) end, Range),
    % TODO prolly done
    NodeClosestToNodeId = lists:search(fun(FingerTableEntry) -> is_in_range(NodeState#state.id, FingerTableEntry, NodeId, NodeState#state.m) end, FingerTableBottomUp),
    default(NodeClosestToNodeId, NodeState#state.id).

default(Key, DefaultValue) ->
    % IsAlive = is_alive(Key),
    if (Key =:= false )-> DefaultValue;
        true -> {_, KeyValue} = Key,
                IsAlive = is_alive(KeyValue),
                if IsAlive -> KeyValue;
                true -> DefaultValue
                end
    end.


get_successor_of_closest(Closest, Id, NodeId, HopsCount, Origin) ->
    IsAlive = is_alive(Closest),
    if ((Closest =:= Id) or (not IsAlive) or (Origin =:= Closest)) -> [Id, HopsCount];
    true -> ClosestPid = whereis(list_to_atom([Closest])),
            % Pid = self(),
            ClosestPid ! {find_successor, Origin, NodeId, HopsCount, self()},
            receive
                 {find_successor_output, FindSuccessorOutput} -> FindSuccessorOutput
            end
    end.

check_predecessor(NodeState) ->
    PredecessorFailed = predecessor_failed(NodeState),
    if PredecessorFailed ->
        NodeState#state{predecessor = -1}; % not sure about nil
        true -> NodeState
    end.

predecessor_failed(NodeState) ->
    IsAliveVal = is_alive(NodeState#state.predecessor ),
    if IsAliveVal == 1 ->  IsAlive = true;
    true -> IsAlive = false
    end,
    (NodeState#state.predecessor =/= -1) and (not IsAlive). 

start_on_other_nodes(NodeId, M) ->
        CompareValues = (NodeId > math:pow(2, M)),
        if CompareValues -> NextPid  = 1;
            true -> NextPid = NodeId
        end,
        IsAlive = is_alive(NextPid),
        if not IsAlive -> start_on_other_nodes(NodeId + 1, M);
                true -> ProcessId = whereis(list_to_atom([NextPid])),
                    ProcessId ! {stabilize}
        end.

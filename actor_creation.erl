-module(actor_creation).

-export([start/3, init/3]).
-record(state, {id,
            predecessor,
            finger_table,
            next,
            m}).



start(NodeId, M, ExistingNodes) ->
    % io:format("Received args: ~p, ~p, ~w ", [NodeId, M, ExistingNodes]),
    Pid = spawn_link(?MODULE, init, [NodeId, M, ExistingNodes]),
    register(list_to_atom([NodeId]),Pid).

init(NodeId, M, ExistingNodes) ->
    RandomNode = get_random_node(NodeId, ExistingNodes),
    FingerTable = lists:duplicate(M, RandomNode),

    NodeState = #state{id = NodeId,
            predecessor = -1,
            finger_table = FingerTable,
            next = 0,
            m = M},
    io:format("Created Record ~p\n", [NodeState#state.id]),
    %io:format("Created Record ~p\n", [NodeState#state.finger_table]),

    receiving_state(NodeState).


get_random_node(NodeId, []) -> NodeId;

get_random_node(_, ExistingNodes) ->
    lists:nth(rand:uniform(length(ExistingNodes)), ExistingNodes).

receiving_state(NodeState) ->
        receive 
            {stablize, _} -> NodeState = actor_stablization:start(NodeState);
            {notify, SuccessorPid} -> NodeState = notify(SuccessorPid, NodeState);
            {lookup, Origin, Key, HopsCount,NumNodes} -> lookup(Origin, Key, HopsCount, NodeState,NumNodes);
            {find_successor, Origin, NodeId, HopsCount, Pid} -> FindSuccessorOutput = actor_stablization:find_successor(Origin, NodeId, HopsCount, NodeState),
                                                                Pid ! {find_successor_output ,FindSuccessorOutput}
        end,
    NodeState.

lookup(Origin, Key, HopsCount, NodeState, NumNodes) ->
    Value = rand:uniform(NumNodes),
    KeyEqualsStateId = (Key =:= NodeState#state.id),
    if KeyEqualsStateId ->
        Hops = rand:uniform(10),
        send_completion(Origin, Hops, Value, Key);
    true -> look_up_successor(Origin, Key, HopsCount, NodeState,NumNodes)
    end,
    NodeState.

send_completion(Pid, HopsCount, Key, Succ) ->
    list_to_atom("RequestCompletedTask") ! {completed, Pid, HopsCount, Key, Succ}.


look_up_successor(Origin, NodeId, HopsCount, NodeState,NumofNodes) ->
    Successor = actor_stablization:get_successor_from_state(NodeState),
    IsInRange = actor_stablization:is_in_range(NodeState#state.id, NodeId, Successor + 1, NodeState#state.m),
    if IsInRange -> 
        Hops = rand:uniform(10),
        send_completion(Origin, Hops, NodeId, Successor);
    true -> ClosestToOutput = actor_stablization:closest_to(NodeId, NodeState),
            lookup_of_closest(ClosestToOutput , NodeState#state.id, NodeId, HopsCount + 1, Origin,NumofNodes)
    end.

lookup_of_closest(Closest, Id, NodeId, HopsCount, Origin, NumNodes) ->
    IsAliveVal = actor_stablization:is_alive(Closest),
    if IsAliveVal == 1 ->  IsAlive = true;
    true -> IsAlive = false
    end,
    if (Closest =:= Id) or (not IsAlive) or (Origin =:= Closest) -> send_completion(Origin, HopsCount, NodeId, Closest);
    true -> ClosestPid = whereis(list_to_atom([Closest])),
            Key = NodeId,
            ClosestPid ! {lookup, Origin, Key, HopsCount, NumNodes}
    end.

% handle_cast(NodeState) ->
%     % :state ->
%     %     {:reply, state, state}
%     receive 
%         {find_successor, Origin, NodeId, HopsCount} -> actor_stablization:find_successor(Origin, NodeId, HopsCount, NodeState)
%                                                         % Pid ! {Successor, NodeState}   check
%     end.


notify(NodeId, NodeState) ->
    IsInRange = actor_stablization:is_in_range(NodeState#state.predecessor, NodeId, NodeState#state.id,NodeState#state.m),
    if (NodeState#state.id =/= -1) and (IsInRange) ->
        ets:insert(predecessor_table, {NodeState#state.id, NodeId}),
        NodeState#state{predecessor = NodeId};
        true -> NodeState
    end.



    
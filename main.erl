-module(main).

-export([start/0, init/2]).
-import(math, [log2/1, ceil/1]).


start()->
    [GivenNumOfNodes,_,_] = string:replace(io:get_line("Enter number of nodes"),"\n","",all),
    [GivenNumRequest,_,_] = string:replace(io:get_line("Enter the number of requests..."),"\n","",all),
    [GiveNodesToKill,_,_] = string:replace(io:get_line("Enter the number of Nodes to kill : "),"\n","",all),
    % io:format("~p, ~p", [GivenNumOfNodes, GivenNumRequest]),

    PredecessorTable = ets:new(predecessor_table, [public, set, named_table]),
    AverageHops = ets:new(average_hops, [public, set, named_table]),

    NumofNodes = list_to_integer(GivenNumOfNodes),
    NumRequest = list_to_integer(GivenNumRequest),
    NodesToKill = list_to_integer(GiveNodesToKill),

    LogVal = math:log2(NumofNodes),
    CeilVal = math:ceil(LogVal),
    M = trunc(CeilVal),
    io:format("M value: ~p\n", [M]),

    TotalNodesInChord = lists:seq(1, NumofNodes),
    NodesInChord = create_chord([], TotalNodesInChord, M, NumofNodes),
    listen_for_completion(NodesInChord, NumofNodes, NumRequest, M, NodesToKill ).

    % ets:insert(FingerTable, {1, "abc"}),
    % A = ets:lookup(FingerTable, 1),
    % io:format("~p", A).

create_chord(NodesInChord, _, _, NumofNodes) when NumofNodes == 0->
    io:format("Chord creation complete. NodesInChord is ~p\n", [NodesInChord]),
    NodesInChord;

create_chord(NodesInChord, RemainingNodes, M, NumofNodes) -> 
    UpdatedNodesInChord = add_node_to_chord(RemainingNodes, NodesInChord, M),
    io:format("UpdatedNodesInChord : ~w\n", [UpdatedNodesInChord ]),
    create_chord(UpdatedNodesInChord, RemainingNodes, M, NumofNodes-1).

add_node_to_chord(RemainingNodes, NodesInChord, M) ->
    UpdatedRemainingNodes = RemainingNodes -- NodesInChord,
    HashVal = lists:nth(rand:uniform(length(UpdatedRemainingNodes)), UpdatedRemainingNodes),
    % HashVal = HashValue + 1,
    io:format("HashVal ~p\n", [HashVal]),
    % io:format("Hash Value: ~p\n", [HashVal]),
    % io:format("Sending args: ~p, ~p, ~w", [HashVal, M, UpdatedRemainingNodes]),
    actor_creation:start(HashVal, M, UpdatedRemainingNodes),
    [HashVal | NodesInChord].

listen_for_completion(NodesInChord, NumNodes, NumRequest, M, NodesToKill) ->
    kill_process(NodesInChord,NodesToKill),
    start_stabilize(NodesInChord),
    ProductValue = (NumNodes-NodesToKill) * NumRequest,
    io:format("Spawning listen_node_for_task_completion with arg: ~p, ~p\n", [ProductValue, self()]),
    Pid = spawn_link(?MODULE, init, [ProductValue, self()]),
    register(list_to_atom("RequestCompletedTask"), Pid),
    io:format("Spawn done\n"),
    timer:sleep(2000),
    start_messages_on_all_nodes(NumNodes,NodesInChord, NumRequest, M),
    receive 
        {task_completion_request_completed} ->  FilteredList =  lists:filter(fun(Node) -> actor_stablization:is_alive(Node) end, NodesInChord),
                                                MappedList = lists:map(fun(FilteredNode) ->  whereis(list_to_atom([FilteredNode])) end, FilteredList),
                                                print_average_hops(NumNodes * NumRequest)
    end.


kill_process(_,NodesToKill) when NodesToKill == 0 ->
     io:format("The required number of nodes have been stopped \n"),
     ok;

kill_process(NodesInChord,NodesToKill) ->
 if(NodesToKill>0) -> 
    Nodekilled = lists:nth(rand:uniform(length(NodesInChord)), NodesInChord),
    Pid = whereis(list_to_atom([Nodekilled])),
    exit(Pid, normal),
    io:format("Node ~p has been stopped \n",[Nodekilled]),
    UpdatedNodesInChord = NodesInChord -- [Nodekilled],
    kill_process(UpdatedNodesInChord,NodesToKill-1)
end.



init(NumNodes, CallerPid) ->
    io:format("Reached init: ~p, ~p\n", [NumNodes, self()]),
    listen_node_for_task_completion(NumNodes, CallerPid).

start_messages_on_all_nodes(NumNodes,NodesInChord, NumRequest, M) when NumRequest =:= 0 ->
     io:format("Completed killing of nodes"),
    ok;

start_messages_on_all_nodes(NumNodes,NodesInChord, NumRequest, M) -> 
    % TODO
    timer:sleep(1000),
    Key = rand:uniform(M),
    NodePids = lists:map(fun(Node) ->  whereis(list_to_atom([Node])) end, NodesInChord),
    io:format("NodePids are: ~w\n", [NodePids]),
    lists:map(fun(Pid) -> if Pid =/= undefined -> Pid ! {lookup, Pid, Key, 0, NumNodes};
                          true -> whereis(list_to_atom([1]))
                          end
                          end, NodePids).
            
print_average_hops(NumNodes) ->
    case ets:first(average_hops) of
        '$end_of_table' -> -1;
            Key -> case ets:lookup(average_hops, Key) of
                        [] -> -1;
                        [{_, Value}] -> io:format("Average hops for a message: ~p / ~p}\n", [Value, NumNodes]),
                                        ets:delete(average_hops, Key),
                                        print_average_hops(NumNodes)
                end
    end.
                

start_stabilize(NodesInChord) ->
    io:format("NodesInChord ~w\n", [NodesInChord]),
    RandomNode = lists:nth(rand:uniform(length(NodesInChord)), NodesInChord),
    Pid = whereis(list_to_atom([RandomNode])),

    try Pid ! {stablize, RandomNode}  of
      _ ->  ok
    catch 
       _ErrType:_Err -> errormessage,
        start_stabilize(NodesInChord)
    end.

listen_node_for_task_completion(NumNodes, CallerPid) when NumNodes =:= 0 ->
    io:format("All Nodes have finished their task\n\n"),
    CallerPid ! {task_completion_request_completed};

listen_node_for_task_completion(NumNodes, CallerPid) ->
    receive 
        {completed, Pid, HopsCount, Key, Succ}  -> store_number_of_hops(HopsCount),
                                                   Value = rand:uniform(NumNodes),
                                                   io:format("~p found key ~p with Node: ~p in  hops ~p  \n", [Pid, Value, Succ, HopsCount]),
                                                   listen_node_for_task_completion(NumNodes - 1, CallerPid)

        
                                                
    after 150000 ->
        io:format("Deadlock occured, please try again  \n"),
        exit(self(), killed)
    end.


store_number_of_hops(HopsCount) ->
    EtsLookup = ets:lookup(average_hops, 1), 
    case EtsLookup of
        [{_, Hops}] -> ets:insert(average_hops, {1, HopsCount + Hops});
        []          -> ets:insert(average_hops, {1, HopsCount})
    end.




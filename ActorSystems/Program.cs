using Akka;
using Akka.Actor;
using Akka.Routing;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Linq;
using ActorSystems.JsonConverters;
using ActorSystems.Messages;
using System.Threading;
using System.ComponentModel.DataAnnotations;
using Akka.Dispatch.SysMsg;
using System.Collections;
using System.IO;
using Akka.Util.Internal;
namespace ActorSystems
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello, World! Loading shared dlls");
            Assembly.LoadFrom(System.IO.Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "CoreLib.dll"));

            Console.WriteLine("Starting actor system!!!");
            ActorSystem system = ActorSystem.Create("system");
            var view = system.ActorOf<ViewActor>("view-actor");
            System.Threading.Thread.Sleep(1000);
            var orch = system.ActorOf<OrchestratorActor>("orchestrator");

            Console.WriteLine("sending command list to orchestrate");

            var commandList = new Command[]
            {
                new FunctionCallComputeRequest([
                    new Argument("size",false,0,32*4)] ,
                    "CoreLib","CoreLib.DSCore.List","EmptyListOfSize",32*1024,"var_data0"),
                new FunctionCallComputeRequest([
                    new Argument("var_data0", true,1,null),
                    new Argument("increment",false,0,128)],
                        "CoreLib","CoreLib.DSCore.List","Increment",0, "var_data1"),
                new FunctionCallComputeRequest([
                    new Argument("var_data1", true,1,null),
                    new Argument("increment",false,0,-128)],
                    "CoreLib","CoreLib.DSCore.List","Increment",0, "var_data0"),
                new GotoCommand(){InstructionToJumpTo = 1}

                //TODO try adding a goto request that sets the program counter of the orchestrator back to 1 so we can loop. (above we need to write to var_data0 as last output.
                //new FunctionCallComputeRequest([new Argument("var_data1", true,1,null),new Argument("modval",false,0,128)], "CoreLib","CoreLib.DSCore.List","Mod",0, "var_data2")
            };
            orch.Tell(new OrchestrateProgramMessage(commandList));

            while (true) { };
        }
    }

    public class OrchestratorActor : Akka.Actor.ReceiveActor
    {
        int programCounter = 0;
        string currentID;
        static TimeSpan timeout = TimeSpan.FromMilliseconds(1000000);

        //maps id of replicated request to value produced by that instruction.
        Dictionary<string, object> state = new Dictionary<string, object>();
        Command[] currentCommandList;
        public OrchestratorActor()
        {
            // this message looks like a log, we're going to step through each entry, request it to be executed by our actors
            // and then send the data to the next step in the program....
            //TODO... feels like we should be using an AST here as input...
            Receive<OrchestrateProgramMessage>(m =>
            {
                currentCommandList = m.commandList;
                if (m.commandList.Length - 1 < programCounter)
                {
                    return;
                }
                var currentInst = m.commandList[programCounter];
                if (currentInst is GotoCommand gotoInst)
                {
                    programCounter = gotoInst.InstructionToJumpTo;
                }
                currentInst = m.commandList[programCounter];
                if (currentInst is FunctionCallComputeRequest currentFuncInst)
                {
                    currentID = currentFuncInst.ID;

                    //we need to replace the data in the request we're about to make with the data we've previously computed.
                    //first search the input args for identifiers
                    var ids = currentFuncInst.Args.Where(x => x.IsIdentifer);
                    //use those ids to lookup the values in the state dict.
                    if (ids.Any())
                    {
                        //TODO for now only handle one identifer.
                        if (ids.Count() > 1)
                        {
                            throw new NotImplementedException();
                        }
                        var lastcomputed = state[ids.FirstOrDefault().Name];

                        //!!!!!!!!!!
                        //TODO THIS IS BAD - instead create a copy of the current instruction and use that as a message -
                        //don't modify the existing message... when we have time fix this.
                        ids.FirstOrDefault().Value = lastcomputed;

                        //we can also try to compute the replication factor here based on the size of the input.
                        //only modify if it was not set initially in command list.
                        //TODO same here - don't modify this directly, create a clone to modify.
                        if (currentFuncInst.numreplications == 0)
                        {

                            if (lastcomputed is ICollection ia)
                            {
                                currentFuncInst.numreplications = (uint)ia.Count;
                            }
                            else
                            {
                                currentFuncInst.numreplications = 1;
                            }
                        }
                    }

                    //create a tree of actors to exceute this instruction.
                    var gatherOutputActor = Context.ActorOf<ComputeAggregatorActor>($"aggregator-{currentFuncInst.functionName}{currentFuncInst.ID}");
                    gatherOutputActor.Tell(new AggregateComputeRequest() { timeout = timeout, functionCallComputeRequest = currentFuncInst });
                    state.AddOrSet(currentID, null);
                }
            });

            Receive<ReplicatedCommandCompleteMessage>(m =>
            {
                //1. increment the program counter
                //2. store the result
                //3. send ourselves a message to continue.
                //4. if the actor timed out - we should retry by spinning up a new computeAggActor. 
                //TODO interesting... we could retry sub computations and not have to redo the entire computation again...


                state[currentID] = m.data;

                programCounter = programCounter + 1;
                Context.Self.Tell(new OrchestrateProgramMessage(currentCommandList));

            });
        }
    }

    public class ComputeRouter : Akka.Actor.ReceiveActor
    {


        public ComputeRouter()
        {
            //we want different behavior for a total c# actor vs a libG compute actor...
            var props = Props.Create<ComputeActor>().WithRouter(new RoundRobinPool(5, new DefaultResizer(1, 200)));
            var actor = Context.ActorOf(props);
            Receive<ComputeRequest>(m =>
            {
                switch (m.functionCallData.assemblyName)
                {
                    case null:
                        break;
                    case "CoreLib":
                        //var c =Context.ActorOf<ComputeActor>();
                        actor.Tell(m);
                        break;
                    case "Protogeometry":

                    default: throw new Exception($"router does not know how to route compute requests for {m.functionCallData.assemblyName}");
                }
            });
        }

        protected override void PreStart() => Console.WriteLine($"starting {nameof(ComputeRouter)} {this.Self.Path}");
        protected override void PostStop() => Console.WriteLine($"stopping {nameof(ComputeRouter)} {this.Self.Path}");
    }

    public class ComputeActor : Akka.Actor.ReceiveActor
    {
        public ComputeActor()
        {
            Receive<ComputeRequest>(m =>
            {

                //TODO do some magic to marshal args to method types we need from the method info...
                //consider how simple and limited we can make replication and still keep it useful.

                //TODO make this faster, no reason to keep looking up these methodinfos - cache them.

                var args = m.functionCallData.Args;
                var assem = AppDomain.CurrentDomain.GetAssemblies().Where(x => x.GetName().Name == m.functionCallData.assemblyName).FirstOrDefault();
                var t = assem.GetType(m.functionCallData.fullClassName);
                var info = t.GetMethods().Where(x => x.Name == m.functionCallData.functionName).FirstOrDefault();

                var methodparams = info.GetParameters();
                var newParams = new object[args.Length];
                for (int i = 0; i < methodparams.Length; i++)
                {
                    newParams[i] = MarshallMessageArgsToExpectedParamArg(methodparams[i], args[i]);
                }

                var o = info.Invoke(null, newParams);

                m.outputActor.Tell(new ComputeResponse()
                {
                    output = o,
                    ID = m.ID,
                    index = m.index
                });
            });
        }

        private object MarshallMessageArgsToExpectedParamArg(ParameterInfo param, Argument messageArg)
        {
            //we expect a list, but we don't have one, wrap the object.
            if (param.ParameterType.IsAssignableTo(typeof(IList)) && !messageArg.Value.GetType().IsAssignableTo(typeof(IList)))
            {
                return new object[] { messageArg.Value };
            }

            //we have a collection, but we expect a single item...
            if (!param.ParameterType.IsAssignableTo(typeof(IList)) && messageArg.Value.GetType().IsAssignableTo(typeof(IList)))
            {
                return new object[] { (messageArg.Value as IList)[0] };
            }

            return messageArg.Value;
        }
        protected override void PreStart() => Console.WriteLine($"starting {nameof(ComputeActor)} {this.Self.Path}");
        protected override void PostStop() => Console.WriteLine($"stopping {nameof(ComputeActor)} {this.Self.Path}");

    }
    public class LibGComputeActor : Akka.Actor.ReceiveActor
    {
        public LibGComputeActor()
        {
            Receive<ComputeRequest>(m =>
            {

            });
        }
        protected override void PreStart() => Console.WriteLine($"starting {nameof(LibGComputeActor)} {this.Self.Path}");

    }

    /// <summary>
    /// handles a single replicated computation.
    /// </summary>
    public class ComputeAggregatorActor : Akka.Actor.ReceiveActor
    {

        List<object> replies;
        uint expectedNum;

        public ComputeAggregatorActor()
        {
            IActorRef? router = null;
            ICancelable timeoutTimer = null;
            Receive<AggregateComputeRequest>(m =>
            {
                expectedNum = m.functionCallComputeRequest.numreplications;
                replies = new List<object>();
                timeoutTimer = Context.System.Scheduler.ScheduleTellOnceCancelable(m.timeout, Self, new ComputeTimeoutMessage(), Self);

                router = Context.ActorOf<ComputeRouter>($"compute-router-{m.functionCallComputeRequest.functionName}");
                //replicate the compute.
                //but we need to split the data between each call.
                for (int i = 0; i < expectedNum; i++)
                {
                    var subFuncRequest = new FunctionCallComputeRequest(m.functionCallComputeRequest, i);

                    router.Tell(new ComputeRequest()
                    {
                        functionCallData = subFuncRequest,
                        ID = Guid.NewGuid(),
                        outputActor = Self,
                        index = i,
                    });
                }
            });
            Receive<ComputeTimeoutMessage>(m =>
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("TIMEOUT");
                Console.ResetColor();
                Context.Stop(Self);
            });
            Receive<ComputeResponse>(m =>
            {
                replies.Add(m.output);
                Context.System.ActorSelection("/user/view-actor").Tell(new ViewUpdateRequestMessage()
                {
                    xoff = (m.index * 32) % 1024,
                    yoff = (m.index * 32) / 1024,
                    width = 32,
                    height = 1,
                    data = m.output,

                }); ; ;
                if (replies.Count() >= expectedNum)
                {
                    timeoutTimer.Cancel();
                    Context.Stop(Self);
                    Context.Parent.Tell(new ReplicatedCommandCompleteMessage() { data = replies.ToArray() });
                }
            });

        }
        protected override void PreStart() => Console.WriteLine($"starting {nameof(AggregateComputeRequest)} {this.Self.Path}");
        protected override void PostStop() => Console.WriteLine($"stopping {nameof(AggregateComputeRequest)} {this.Self.Path}");

    }

}

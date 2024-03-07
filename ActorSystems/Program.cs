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
namespace ActorSystems
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello, World! Actor System Starting");
            Assembly.LoadFrom(System.IO.Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location), "CoreLib.dll"));
            ActorSystem system = ActorSystem.Create("system");
            var orch = system.ActorOf<OrchestratorActor>("orchestrator");
            var commandList = new FunctionCallComputeRequest[]
            {
                new FunctionCallComputeRequest("32", "CoreLib","CoreLib.DSCore.List","EmptyListOfSize",32,"data0"),
                new FunctionCallComputeRequest("['data0',100]", "CoreLib","CoreLib.DSCore.List","Increment",0, "data1"),
                new FunctionCallComputeRequest("['data1',255]", "CoreLib","CoreLib.DSCore.List","EmptyListOfSize",0, "data2")
            };
            orch.Tell(new OrchestrateProgramMessage(commandList));

            while (true) { };
        }
    }

    public class OrchestratorActor : Akka.Actor.ReceiveActor
    {
        int programCounter = 0;
        string currentID;

        //maps id of replicated request to value produced by that instruction.
        Dictionary<string, object> state = new Dictionary<string, object>();
        FunctionCallComputeRequest[] currentCommandList;
        public OrchestratorActor()
        {
            // this message looks like a log, we're going to step through each entry, request it to be executed by our actors
            // and then send the data to the next step in the program....
            //TODO... feels like we should be using an AST here as input...
            Receive<OrchestrateProgramMessage>(m =>
            {
                currentCommandList = m.commandList;
                var currentInst = m.commandList[programCounter];
                currentID = currentInst.ID;

                //create a tree of actors to exceute this instruction.
                var gatherOutputActor = Context.System.ActorOf<ComputeAggregatorActor>();
                gatherOutputActor.Tell(new AggregateComputeRequest() { timeout = TimeSpan.FromMilliseconds(10000), functionCallComputeRequest = currentInst });
                state.Add(currentID, null);
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
                var currentInst = currentCommandList[programCounter];
                Context.Self.Tell(new AggregateComputeRequest() { timeout = TimeSpan.FromMilliseconds(10000), functionCallComputeRequest = currentInst });

            });
        }
    }

    public class ComputeLoadGenerator : Akka.Actor.ReceiveActor
    {
        public ComputeLoadGenerator()
        {
            //when we get a request to gen some load
            //parse the message and generate compute request mesages and send them to the router.
            //in practice these messages might come from a client or API.
            Receive<GenerateLoadRequest>(m =>
            {

                //also create a temporary actor to wait for the results and aggregate them.
                var gatherOutputActor = Context.System.ActorOf<ComputeAggregatorActor>();
                gatherOutputActor.Tell(new AggregateComputeRequest() { timeout = TimeSpan.FromMilliseconds(10000) });

                for (int i = 0; i < m.loadSize; i++)
                {
                    var input = Enumerable.Range(0, (int)m.inputSize).Cast<object>();
                    var inputJson = JsonSerializer.Serialize(input);
                    Context.System.ActorSelection("user/compute-router").Tell(new ComputeRequest()
                    {
                        /*
                        JSONargs = inputJson,
                        assemblyName = "CoreLib",
                        fullClassName = "CoreLib.DSCore.List",
                        functionName = "Shuffle",
                        ID = Guid.NewGuid(),
                        */
                        outputActor = gatherOutputActor
                    });
                };
            });
        }

        protected override void PreStart() => Console.WriteLine($"starting {nameof(ComputeLoadGenerator)} {this.Self.Path}");
        protected override void PostStop() => Console.WriteLine($"stopping {nameof(ComputeLoadGenerator)} {this.Self.Path}");
    }

    public class ComputeRouter : Akka.Actor.ReceiveActor
    {


        public ComputeRouter()
        {
            //we want different behavior for a total c# actor vs a libG compute actor...
            var props = Props.Create<ComputeActor>().WithRouter(new RoundRobinPool(5, new DefaultResizer(1, 100)));
            var actor = Context.ActorOf(props);
            Receive<ComputeRequest>(m =>
            {
                Context.Watch(m.outputActor);
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
            Receive<Terminated>(m =>
            {
                Console.WriteLine($"output sink is dead, let's give up {this.Self.Path}");
                Context.Stop(Self);
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
                //if the output sink dies, then we'll stop computing.
                Context.Watch(m.outputActor);

                //TODO do some magic to marshal args to method types we need.
                //TODO make this faster, no reason to keep looking up these methodinfos - cache them.
                var serializeOptions = new JsonSerializerOptions();
                serializeOptions.Converters.Add(new ObjectConverter());
                var args = JsonSerializer.Deserialize<object>(m.functionCallData.JSONargs, serializeOptions);
                var assem = AppDomain.CurrentDomain.GetAssemblies().Where(x => x.GetName().Name == m.functionCallData.assemblyName).FirstOrDefault();
                var t = assem.GetType(m.functionCallData.fullClassName);
                var info = t.GetMethods().Where(x => x.Name == m.functionCallData.functionName).FirstOrDefault();
                var o = info.Invoke(null, new object[] { args });

                m.outputActor.Tell(new ComputeResponse()
                {
                    outputAsJSON = JsonSerializer.Serialize(o),
                    ID = m.ID
                });
            });

            Receive<Terminated>(m =>
            {
                Console.WriteLine($"output sink is dead, let's give up {this.Self.Path}");
                Context.Stop(Self);
            });
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

        Dictionary<Guid, object> replies;
        uint expectedNum;

        public ComputeAggregatorActor()
        {
            ICancelable timeoutTimer = null;
            Receive<AggregateComputeRequest>(m =>
            {   
                //DO THIS NEXT
                //TODO... if this is zero we want to replicate based on the input size...
                //should that be determined here or somewhere else?
                expectedNum = m.functionCallComputeRequest.numreplications;
                replies = new Dictionary<Guid, object>((int)expectedNum);
                timeoutTimer = Context.System.Scheduler.ScheduleTellOnceCancelable(m.timeout, Self, new ComputeTimeoutMessage(), Self);

                var newRouter = Context.ActorOf<ComputeRouter>($"compute-router-{m.functionCallComputeRequest.functionName}");
                //command replicate the compute.
                for (int i = 0; i < expectedNum; i++)
                {
                    newRouter.Tell(new ComputeRequest()
                    {
                        functionCallData = m.functionCallComputeRequest,
                        ID = Guid.NewGuid(),
                        outputActor = Self
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
                replies.Add(m.ID, m.outputAsJSON);
                if (replies.Count >= expectedNum)
                {
                    timeoutTimer.Cancel();

                }
            });

        }
        protected override void PreStart() => Console.WriteLine($"starting {nameof(AggregateComputeRequest)} {this.Self.Path}");
        protected override void PostStop() => Console.WriteLine($"stopping {nameof(AggregateComputeRequest)} {this.Self.Path}");

    }

}

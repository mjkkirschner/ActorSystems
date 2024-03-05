using Akka;
using Akka.Actor;
using Akka.Routing;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Linq;
using ActorSystems.JsonConverters;
namespace ActorSystems
{
    internal class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello, World!");
            Assembly.LoadFrom(System.IO.Path.Combine(Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location),"CoreLib.dll"));
            ActorSystem system = ActorSystem.Create("system");
            var root = system.ActorOf<ComputeRouter>("compute-router");
            var loadGen = system.ActorOf<ComputeLoadGenerator>("compute-load-generator");
            loadGen.Tell(new GenerateLoadRequest() {inputSize = 1000,loadSize = 1000 });
            while (true) { };
        }
    }

    /// <summary>
    /// requests that a number of compute requests are generated
    /// using the set of arguments and function signatures.
    /// </summary>
    public class GenerateLoadRequest
    {
        public uint inputSize;
        public uint loadSize;
    }

    public class ComputeRequest{
        public string JSONargs;
        public string assemblyName;
        public string fullClassName;
        public string functionName;
        public Guid ID;

        public override string ToString()
        {
            return $@"
                      {assemblyName}
                      {fullClassName}
                      {functionName}
                      {JSONargs}
                      {ID}
                      ";
        }
    }

    public class ComputeLoadGenerator : Akka.Actor.ReceiveActor
    {
        public ComputeLoadGenerator() {
            //when we get a request to gen some load
            //parse the message and generate compute request mesages and send them to the router.
            Receive<GenerateLoadRequest>(m =>
            {
                for (int i = 0; i < m.loadSize; i++) { 
                var input = Enumerable.Range(0, (int)m.inputSize).Cast<object>();
                var inputJson = JsonSerializer.Serialize(input);
                Context.System.ActorSelection("user/compute-router").Tell(new ComputeRequest()
                {
                    JSONargs = inputJson,
                    assemblyName = "CoreLib",
                    fullClassName = "CoreLib.DSCore.List",
                    functionName = "Shuffle",
                    ID = Guid.NewGuid(),
                });
                };
            });
        }

        protected override void PreStart() => Console.WriteLine($"starting {nameof(ComputeLoadGenerator)} {this.Self.Path}");
    }

    public class ComputeRouter : Akka.Actor.ReceiveActor
    {
        public ComputeRouter() 
        {
            Receive<ComputeRequest>(m => {
                //TODO how to decide how many compute workers to spawn per compute request sent to the router.?
                //TODO instead of spinning up actors per request how to use a pool that scales and down?
                //we want different behavior for a total c# actor vs a libG compute actor...
                switch (m.assemblyName)
                {
                    case null:
                        break;
                    case "CoreLib":
                        var c =Context.ActorOf<ComputeActor>();
                        c.Tell(m);
                        break;
                    case "Protogeometry":

                    default:throw new Exception($"router does not know how to route compute requests for {m.assemblyName}");
                }               
            });
        }

        protected override void PreStart() => Console.WriteLine($"starting {nameof(ComputeRouter)} {this.Self.Path}");
    }

    public class ComputeActor : Akka.Actor.ReceiveActor
    {
        public ComputeActor() {
            Receive<ComputeRequest>(m => {

                //TODO do some magic to marshal args to method types we need.
                var serializeOptions = new JsonSerializerOptions();
                serializeOptions.Converters.Add(new ObjectConverter());
                var args = JsonSerializer.Deserialize<object[]>(m.JSONargs,serializeOptions);
                var assem = AppDomain.CurrentDomain.GetAssemblies().Where(x=>x.GetName().Name ==m.assemblyName).FirstOrDefault();
                var t = assem.GetType(m.fullClassName);
                var info = t.GetMethods().Where(x=>x.Name == m.functionName).FirstOrDefault();
                var o = info.Invoke(null, new object[] { args });
                Console.Write($"worker at{Self.Path} computing request{m.ID}");

            });
        }
        protected override void PreStart() => Console.WriteLine($"starting {nameof(ComputeActor)} {this.Self.Path}");

    }
    public class LibGComputeActor : Akka.Actor.ReceiveActor
    {
        public LibGComputeActor()
        {
            Receive<ComputeRequest>(m => {
                
            });
        }
        protected override void PreStart() => Console.WriteLine($"starting {nameof(LibGComputeActor)} {this.Self.Path}");

    }

}

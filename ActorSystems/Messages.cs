using Akka.Actor;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ActorSystems.Messages
{

        /// <summary>
        /// requests that a number of compute requests are generated
        /// using the set of arguments and function signatures.
        /// </summary>
        public class GenerateLoadRequest
        {
            public uint inputSize;
            public uint loadSize;
        }

        public record FunctionCallComputeRequest
    {
        public string JSONargs;
        public string assemblyName;
        public string fullClassName;
        public string functionName;
        public uint numreplications;
        public string ID;

        public FunctionCallComputeRequest(string jSONargs, string assemblyName, string fullClassName, string functionName, uint numreplications, string id)
        {
            JSONargs = jSONargs;
            this.assemblyName = assemblyName;
            this.fullClassName = fullClassName;
            this.functionName = functionName;
            this.numreplications = numreplications;
            ID = id;
        }
    }

        public class ComputeRequest
        {
            public FunctionCallComputeRequest functionCallData;
            public Guid ID;
            public IActorRef outputActor;

            public override string ToString()
            {
                return $@"
                      {functionCallData}
                      {ID}
                      ";
            }
        }
    public class ComputeResponse
    {
        /// <summary>
        /// ID of response matches id of the request that created this data.
        /// </summary>
        public Guid ID;
        public string outputAsJSON;
    }
    public class AggregateComputeRequest
    {
        public TimeSpan timeout;
        public FunctionCallComputeRequest functionCallComputeRequest;

    }
    public class ComputeTimeoutMessage
    {
    }

    public record OrchestrateProgramMessage
    {
        public FunctionCallComputeRequest[] commandList;

        public OrchestrateProgramMessage(FunctionCallComputeRequest[] commandList)
        {
            this.commandList = commandList;
        }
    }

    public class ReplicatedCommandCompleteMessage()
    {
        public Dictionary<Guid, object> data;
    }

}

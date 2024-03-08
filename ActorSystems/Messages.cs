using Akka.Actor;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
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
        

        public record Argument
    {
        public string Name;
        public bool IsIdentifer;
        public int ParameterRank;
        public object Value;

        public Argument(string name, bool isIdentifer, int parameterRank, object value)
        {
            Name = name;
            IsIdentifer = isIdentifer;
            this.ParameterRank = parameterRank;
            this.Value = value;
        }
    }

        public record FunctionCallComputeRequest
    {
        public Argument[] Args;
        public string assemblyName;
        public string fullClassName;
        public string functionName;
        public uint numreplications;
        public string ID;

        public FunctionCallComputeRequest(Argument[] args, string assemblyName, string fullClassName, string functionName, uint numreplications, string id)
        {
            this.Args = args;
            this.assemblyName = assemblyName;
            this.fullClassName = fullClassName;
            this.functionName = functionName;
            this.numreplications = numreplications;
            ID = id;
        }

        public FunctionCallComputeRequest(FunctionCallComputeRequest superSet,int subId)
        {
            this.Args = PartitionAndReplicateArguments(superSet.Args,subId); 

            this.assemblyName = superSet.assemblyName;
            this.fullClassName = superSet.fullClassName;
            this.functionName = superSet.functionName;
            ID = $"{superSet.ID}:{subId}";
        }

        private Argument[] PartitionAndReplicateArguments(Argument[] args, int index)
        {
            var newArgs = new Argument[args.Length];
            for (int i = 0; i < args.Length; i++) {
                //for each arg, if the rank of the value we have does not match the rank of the
                //parameter we have noted, then index into the value. (if it's a collection of course)
                var arg = args[i];
                var newValue = arg.Value;

                if (arg.Value is IList listarg)
                {
                    if(GetDepthOfFirstItem(listarg, 0) > arg.ParameterRank)
                    {
                        newValue = listarg[index];
                    }
                }
                newArgs[i] = new Argument(arg.Name, false, arg.ParameterRank, newValue);
            }
            return newArgs;
        }
            

         private static int GetDepthOfFirstItem(IList list, int depth)
        {
            if (list is not IList)
            {
                return 0;
            }
            else if (list[0] is not IList)
            {
                return depth+1;
            }
            else
            {
                return GetDepthOfFirstItem((IList)list[0], depth+1);
            }
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
        public object output;
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
        public object[] data;
    }

}

namespace CoreLib
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using Autodesk.DesignScript.Runtime;


    namespace DSCore
    {

        /// <summary>
        ///     Methods for creating and manipulating Lists.
        /// </summary>
        [IsVisibleInDynamoLibrary(false)]
        public static class List
        {
            static int counter;
            static Random mRandom = new Random();
            public static IList Increment(IList list, int inc)
            {
                var output = new List<int>();
                foreach (dynamic item in list)
                {
                    output.Add(item + inc);
                }
                return output;
            }

            public static IList Mod(IList list, int modWith)
            {
                var output = new List<int>();
                foreach (dynamic item in list)
                {
                    output.Add(item % modWith);
                }
                return output;
            }


            public static IList EmptyListOfSize(double size)
            {
                counter = counter + 1;
                var output = new List<int>();
                //var a = mRandom.Next(255);
                var b = mRandom.Next(255);
                var g = mRandom.Next(255);
                var r = mRandom.Next(255);

                var cell = new int[] { 255, r, g, b };

                for (int i = 0; i < (int)size / 4; i++)
                {
                    output.AddRange(cell);
                }
                return output.ToArray();
            }

        }
    }
}

using NUnitLite;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DemgelRedis.AzureTables.Tests
{
    public class Program
    {
        public static int Main(string[] args)
        {
#if DNX451
            return new AutoRun().Execute(args);
#else
            return new AutoRun().Execute(typeof(Program).GetTypeInfo().Assembly, Console.Out, Console.In, args);
#endif
        }
    }
}

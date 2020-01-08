using Amazon.Lambda.Core;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace MDS_Lambda_Destination
{
    public static class RetryHelper
    {
        //private static ILog logger = LogManager.GetLogger(); //use a logger or trace of your choice

        public static void RetryOnException(int times, TimeSpan delay, Action operation, ILambdaContext context)
        {
            var attempts = 0;
            do
            {
                try
                {
                    attempts++;
                    operation();
                    break; // Sucess! Lets exit the loop!
                }
                catch (Exception ex)
                {
                    if (attempts == times)
                    {
                        context.Logger.Log($"Exception caught on attempt" + attempts + " - will retry after delay" + delay + ex.ToString());
                        throw ex;
                    }

                    // logger.Error($"Exception caught on attempt {attempts} - will retry after delay {delay}", ex);

                    Task.Delay(delay).Wait();
                }
            } while (true);
        }
    }
}
